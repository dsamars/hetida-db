// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Instance metadata and the startup layout check (§11).
//!
//! The partition interval is fixed when an instance is initialised and recorded
//! on disk. Every later start validates the configured interval against that
//! record and **refuses to start on a mismatch**, because adopting the new value
//! would write a second layout beside the first and leave the archive with two
//! incompatible directory shapes and no way to tell which rows belong to which.
//!
//! Three states, and the third is the one worth spelling out:
//!
//! - The record is present and agrees — start.
//! - The record is present and disagrees — [`InstanceError::IntervalMismatch`].
//! - The record is absent. If no partition has been written the layout is not yet
//!   decided, so initialising is right. If data *is* present, the layout is
//!   whatever produced it and cannot be inferred — [`InstanceError::UnknownLayout`]
//!   rather than a guess, since a wrong guess is exactly the silent second layout
//!   §11 rules out. A store holding only logs has not decided anything yet.
//!
//! The format is one `key=value` per line. Unknown keys are ignored so a newer
//! build may add fields without an older one refusing to start; a missing or
//! unparsable `partition_interval` is [`InstanceError::Corrupt`], never a
//! fallback to the default.

use std::path::Path;
use std::sync::Arc;

use crate::partition::PartitionInterval;
use crate::storage::{Storage, StorageError};

/// Where the instance records its layout.
pub const METADATA_PATH: &str = "instance.meta";

/// Directory holding the partitioned Parquet files.
pub const DATA_DIR: &str = "data";

/// Key recording the partition interval (§11).
const PARTITION_INTERVAL_KEY: &str = "partition_interval";

/// Why an instance could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum InstanceError {
    /// The configured interval disagrees with the one this instance was
    /// initialised with (§11).
    #[error(
        "instance was initialised with partition interval `{recorded}` \
         but is configured for `{configured}`; \
         the interval cannot change once data exists"
    )]
    IntervalMismatch {
        /// What the instance recorded when it was initialised.
        recorded: PartitionInterval,
        /// What this process was configured with.
        configured: PartitionInterval,
    },
    /// Partitions exist but no record says which layout produced them.
    #[error(
        "`{DATA_DIR}` holds partitions but `{METADATA_PATH}` is missing, \
         so the layout cannot be determined; restore the metadata rather than \
         letting a second layout be written"
    )]
    UnknownLayout,
    /// The metadata exists but could not be understood.
    #[error("`{METADATA_PATH}` is not readable instance metadata: {reason}")]
    Corrupt {
        /// What was wrong with it.
        reason: String,
    },
    /// The store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// The settled, on-disk properties of one archive instance.
#[derive(Debug)]
pub struct InstanceMetadata {
    partition_interval: PartitionInterval,
}

impl InstanceMetadata {
    /// Opens the instance in `storage`, initialising it if it is new.
    ///
    /// Fails rather than adapting when `configured` disagrees with what is
    /// recorded — see the module documentation.
    pub fn open(
        storage: Arc<dyn Storage>,
        configured: PartitionInterval,
    ) -> Result<Self, InstanceError> {
        match read_recorded(storage.as_ref())? {
            Some(recorded) if recorded == configured => Ok(Self {
                partition_interval: recorded,
            }),
            Some(recorded) => Err(InstanceError::IntervalMismatch {
                recorded,
                configured,
            }),
            None if has_partitions(storage.as_ref())? => Err(InstanceError::UnknownLayout),
            None => {
                write_recorded(storage.as_ref(), configured)?;
                Ok(Self {
                    partition_interval: configured,
                })
            }
        }
    }

    /// The interval this instance's layout uses (§11).
    pub fn partition_interval(&self) -> PartitionInterval {
        self.partition_interval
    }
}

/// Reads the recorded interval, or `None` if the instance is uninitialised.
fn read_recorded(storage: &dyn Storage) -> Result<Option<PartitionInterval>, InstanceError> {
    let path = Path::new(METADATA_PATH);
    // An absent file is an uninitialised instance; any other failure is real.
    let bytes = match storage.read(path) {
        Ok(bytes) => bytes,
        Err(StorageError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    let text = String::from_utf8(bytes).map_err(|error| InstanceError::Corrupt {
        reason: format!("not UTF-8: {error}"),
    })?;

    let recorded = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .find(|(key, _)| key.trim() == PARTITION_INTERVAL_KEY)
        .map(|(_, value)| value.trim())
        .ok_or_else(|| InstanceError::Corrupt {
            reason: format!("no `{PARTITION_INTERVAL_KEY}` entry"),
        })?;

    recorded
        .parse()
        .map(Some)
        .map_err(|error| InstanceError::Corrupt {
            reason: format!("{error}"),
        })
}

/// Records `interval` durably, so the next start can validate against it.
fn write_recorded(storage: &dyn Storage, interval: PartitionInterval) -> Result<(), InstanceError> {
    let mut file = storage.open_append(Path::new(METADATA_PATH))?;
    file.append(format!("{PARTITION_INTERVAL_KEY}={interval}\n").as_bytes())?;
    // Synced before anything writes a partition, so a layout never exists on
    // disk without the record that explains it.
    file.sync()?;
    Ok(())
}

/// Whether any partition has been written yet.
fn has_partitions(storage: &dyn Storage) -> Result<bool, InstanceError> {
    Ok(!storage.list(Path::new(DATA_DIR))?.is_empty())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::partition::PartitionInterval;
    use crate::storage::{Fault, InMemoryStorage, Storage};

    fn write(storage: &InMemoryStorage, path: &str, contents: &str) {
        let mut file = storage.open_append(Path::new(path)).unwrap();
        file.append(contents.as_bytes()).unwrap();
        file.sync().unwrap();
    }

    fn open(
        storage: &InMemoryStorage,
        interval: PartitionInterval,
    ) -> Result<InstanceMetadata, InstanceError> {
        InstanceMetadata::open(Arc::new(storage.clone()), interval)
    }

    /// §11: the interval is fixed when an instance is initialised. A fresh store
    /// records what it was configured with.
    #[test]
    fn a_fresh_instance_records_its_interval() {
        let storage = InMemoryStorage::new();
        let metadata = open(&storage, PartitionInterval::Day).unwrap();

        assert_eq!(metadata.partition_interval(), PartitionInterval::Day);
        assert!(
            !storage.read(Path::new(METADATA_PATH)).unwrap().is_empty(),
            "the interval must be on disk, not just in memory"
        );
    }

    #[test]
    fn reopening_with_the_same_interval_succeeds() {
        let storage = InMemoryStorage::new();
        open(&storage, PartitionInterval::Hour).unwrap();

        let reopened = open(&storage, PartitionInterval::Hour).unwrap();
        assert_eq!(reopened.partition_interval(), PartitionInterval::Hour);
    }

    /// §11: this is the decision. A configuration that disagrees with the
    /// recorded interval is a startup error — never a silent second layout.
    #[test]
    fn reopening_with_a_different_interval_refuses_to_start() {
        let storage = InMemoryStorage::new();
        open(&storage, PartitionInterval::Month).unwrap();

        let error = open(&storage, PartitionInterval::Day).expect_err("must refuse");

        assert!(
            matches!(
                error,
                InstanceError::IntervalMismatch {
                    recorded: PartitionInterval::Month,
                    configured: PartitionInterval::Day,
                }
            ),
            "unexpected error: {error}"
        );
        // The message has to name both, or an operator cannot tell which side to
        // change.
        let message = error.to_string();
        assert!(
            message.contains("month") && message.contains("day"),
            "{message}"
        );
    }

    /// The dangerous case: metadata gone but data present. Initialising afresh
    /// would adopt whatever the configuration says and write a second layout
    /// beside the first, which §11 rules out.
    #[test]
    fn missing_metadata_beside_existing_data_refuses_to_start() {
        let storage = InMemoryStorage::new();
        write(&storage, "data/year=2026/month=08/part-0.parquet", "rows");

        let error = open(&storage, PartitionInterval::Day).expect_err("must refuse");

        assert!(matches!(error, InstanceError::UnknownLayout));
    }

    /// A store holding only a WAL has not written a partition yet, so its layout
    /// is not yet decided and initialising is correct.
    #[test]
    fn missing_metadata_beside_only_a_wal_initialises() {
        let storage = InMemoryStorage::new();
        write(&storage, "wal/000000000000.wal", "records");
        write(&storage, "registry/000000000000.wal", "registrations");

        assert_eq!(
            open(&storage, PartitionInterval::Year)
                .unwrap()
                .partition_interval(),
            PartitionInterval::Year
        );
    }

    /// §11: the recorded interval has to be readable back exactly, for every
    /// value — a round trip through the file, not just through the parser.
    #[test]
    fn every_interval_round_trips_through_the_file() {
        for interval in PartitionInterval::ALL {
            let storage = InMemoryStorage::new();
            open(&storage, interval).unwrap();

            assert_eq!(
                open(&storage, interval).unwrap().partition_interval(),
                interval
            );
        }
    }

    #[test]
    fn unreadable_metadata_is_reported_rather_than_ignored() {
        let storage = InMemoryStorage::new();
        write(&storage, METADATA_PATH, "partition_interval=fortnight\n");

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Corrupt { .. })
        ));
    }

    #[test]
    fn metadata_without_the_interval_is_corrupt() {
        let storage = InMemoryStorage::new();
        write(&storage, METADATA_PATH, "something_else=month\n");

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Corrupt { .. })
        ));
    }

    /// Unknown keys are tolerated, so a newer build may add fields without the
    /// older one refusing to start on them.
    #[test]
    fn unknown_keys_are_tolerated() {
        let storage = InMemoryStorage::new();
        write(
            &storage,
            METADATA_PATH,
            "partition_interval=day\nsomething_new=7\n",
        );

        assert_eq!(
            open(&storage, PartitionInterval::Day)
                .unwrap()
                .partition_interval(),
            PartitionInterval::Day
        );
    }

    #[test]
    fn a_failing_store_surfaces_as_an_error() {
        let storage = InMemoryStorage::new();
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Storage(_))
        ));
    }

    /// A torn metadata write must not leave an instance that starts with half a
    /// layout recorded (§4's tail rule applies here too).
    #[test]
    fn a_torn_metadata_write_is_not_mistaken_for_a_valid_one() {
        let storage = InMemoryStorage::new();
        storage.inject(Fault::TornWrite { keep: 12 });
        let _ = open(&storage, PartitionInterval::Month);

        // Whatever landed, a later start either reads it back correctly or
        // refuses — it must never silently adopt a different interval.
        match open(&storage, PartitionInterval::Month) {
            Ok(metadata) => {
                assert_eq!(metadata.partition_interval(), PartitionInterval::Month)
            }
            Err(error) => assert!(matches!(error, InstanceError::Corrupt { .. })),
        }
    }
}
