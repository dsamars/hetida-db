// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Instance metadata, the startup layout check (§11) and the on-disk format
//! gate (§12).
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
//!
//! [`FORMAT_VERSION`] is resolved before any of that, and before either log is
//! replayed (§12) — this is the archive-wide gate, and it is checked at the only
//! moment where refusing is still clean. A stamp higher than this build reads is
//! [`InstanceError::UnknownFormatVersion`], never an attempt; no stamp at all is
//! [`InstanceError::PredatesVersioning`], never an assumption that it means
//! version 1. Note the asymmetry the tolerance above creates: a build older than
//! §12 ignores the version field like any other unknown key, so it cannot be
//! stopped from opening a future archive. That is the cost of not having had a
//! version from the first commit, and it is why this landed before 0.1.0 rather
//! than after.

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

/// Key recording the on-disk format version (§12).
const FORMAT_VERSION_KEY: &str = "format_version";

/// The on-disk format this build writes and is the newest it can read (§12).
///
/// One number covers the whole archive — the logs, the Parquet files and this
/// metadata — and is bumped whenever any of them changes shape. Reading an
/// archive stamped higher than this is refused rather than attempted, because a
/// wrong guess about what bytes mean is not detectable, only its consequences
/// are.
pub const FORMAT_VERSION: u32 = 1;

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
    /// The archive was written by a build with a newer on-disk format (§12).
    #[error(
        "`{METADATA_PATH}` records on-disk format version {found}, \
         but this build reads at most {supported}; \
         run a build that understands version {found} rather than \
         downgrading, which would misread the archive"
    )]
    UnknownFormatVersion {
        /// What the archive is stamped with.
        found: u32,
        /// The newest this build can read.
        supported: u32,
    },
    /// The metadata records no format version at all (§12).
    #[error(
        "`{METADATA_PATH}` records no `{FORMAT_VERSION_KEY}`: it was written \
         either before format versioning existed or by a torn write. \
         There is nothing to repair — re-create the archive; \
         releases before 0.1.0 carry no migration path"
    )]
    PredatesVersioning,
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
    format_version: u32,
}

/// What one metadata file records.
#[derive(Debug, Clone, Copy)]
struct Recorded {
    partition_interval: PartitionInterval,
    format_version: u32,
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
            Some(recorded) if recorded.partition_interval == configured => Ok(Self {
                partition_interval: recorded.partition_interval,
                format_version: recorded.format_version,
            }),
            Some(recorded) => Err(InstanceError::IntervalMismatch {
                recorded: recorded.partition_interval,
                configured,
            }),
            None if has_partitions(storage.as_ref())? => Err(InstanceError::UnknownLayout),
            None => {
                write_recorded(storage.as_ref(), configured)?;
                Ok(Self {
                    partition_interval: configured,
                    format_version: FORMAT_VERSION,
                })
            }
        }
    }

    /// The interval this instance's layout uses (§11).
    pub fn partition_interval(&self) -> PartitionInterval {
        self.partition_interval
    }

    /// The on-disk format version this archive is stamped with (§12).
    ///
    /// Never greater than [`FORMAT_VERSION`]: a higher one refuses to open.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }
}

/// Reads what the metadata records, or `None` if the instance is uninitialised.
///
/// The format version is resolved before anything else (§12): a build that
/// cannot read the format can conclude nothing about what the other fields mean.
fn read_recorded(storage: &dyn Storage) -> Result<Option<Recorded>, InstanceError> {
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

    let value_of = |wanted: &str| {
        text.lines()
            .filter_map(|line| line.split_once('='))
            .find(|(key, _)| key.trim() == wanted)
            .map(|(_, value)| value.trim().to_owned())
    };

    // §12: the gate. Absent means the archive predates versioning, which is not
    // the same as a version that fails to parse — that is damage.
    let format_version: u32 = value_of(FORMAT_VERSION_KEY)
        .ok_or(InstanceError::PredatesVersioning)?
        .parse()
        .map_err(|error| InstanceError::Corrupt {
            reason: format!("`{FORMAT_VERSION_KEY}` is not a version number: {error}"),
        })?;
    if format_version > FORMAT_VERSION {
        return Err(InstanceError::UnknownFormatVersion {
            found: format_version,
            supported: FORMAT_VERSION,
        });
    }

    let partition_interval = value_of(PARTITION_INTERVAL_KEY)
        .ok_or_else(|| InstanceError::Corrupt {
            reason: format!("no `{PARTITION_INTERVAL_KEY}` entry"),
        })?
        .parse()
        .map_err(|error| InstanceError::Corrupt {
            reason: format!("{error}"),
        })?;

    Ok(Some(Recorded {
        partition_interval,
        format_version,
    }))
}

/// Records the format version and `interval` durably, so the next start can
/// validate against both.
fn write_recorded(storage: &dyn Storage, interval: PartitionInterval) -> Result<(), InstanceError> {
    let mut file = storage.open_append(Path::new(METADATA_PATH))?;
    // The version leads, so a write torn partway through loses the interval
    // rather than the field that says how to read it (§12).
    file.append(
        format!("{FORMAT_VERSION_KEY}={FORMAT_VERSION}\n{PARTITION_INTERVAL_KEY}={interval}\n")
            .as_bytes(),
    )?;
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
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
        write(
            &storage,
            METADATA_PATH,
            &format!("{FORMAT_VERSION_KEY}={FORMAT_VERSION}\npartition_interval=fortnight\n"),
        );

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Corrupt { .. })
        ));
    }

    #[test]
    fn metadata_without_the_interval_is_corrupt() {
        let storage = InMemoryStorage::new();
        write(
            &storage,
            METADATA_PATH,
            &format!("{FORMAT_VERSION_KEY}={FORMAT_VERSION}\nsomething_else=month\n"),
        );

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Corrupt { .. })
        ));
    }

    /// Unknown keys are tolerated, so a newer build may add fields without the
    /// older one refusing to start on them. Note the asymmetry §12 records: this
    /// tolerance is exactly why a build older than §12 cannot be protected from
    /// a future format — it would ignore the version field too.
    #[test]
    fn unknown_keys_are_tolerated() {
        let storage = InMemoryStorage::new();
        write(
            &storage,
            METADATA_PATH,
            &format!(
                "{FORMAT_VERSION_KEY}={FORMAT_VERSION}\npartition_interval=day\nsomething_new=7\n"
            ),
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
        //
        // A torn write can also cut the version line itself, which is
        // indistinguishable from metadata written before §12 existed. Both are
        // `PredatesVersioning`, and both mean the same thing to an operator:
        // there is nothing here to repair.
        match open(&storage, PartitionInterval::Month) {
            Ok(metadata) => {
                assert_eq!(metadata.partition_interval(), PartitionInterval::Month)
            }
            Err(error) => assert!(
                matches!(
                    error,
                    InstanceError::Corrupt { .. } | InstanceError::PredatesVersioning
                ),
                "unexpected error: {error}"
            ),
        }
    }

    /// §12: the version is recorded when the instance is initialised, on disk
    /// rather than only in memory — it has to survive the process that wrote it.
    #[test]
    fn a_fresh_instance_records_the_format_version() {
        let storage = InMemoryStorage::new();
        let metadata = open(&storage, PartitionInterval::Month).unwrap();

        assert_eq!(metadata.format_version(), FORMAT_VERSION);

        let written = String::from_utf8(storage.read(Path::new(METADATA_PATH)).unwrap()).unwrap();
        assert!(
            written.contains(&format!("{FORMAT_VERSION_KEY}={FORMAT_VERSION}")),
            "the version must be in the file: {written}"
        );
    }

    /// §12: this is the decision. A version the build does not know is refused
    /// by name — never adapted to, never guessed at.
    #[test]
    fn a_newer_format_version_refuses_to_start() {
        let storage = InMemoryStorage::new();
        let future = FORMAT_VERSION + 1;
        write(
            &storage,
            METADATA_PATH,
            &format!("partition_interval=month\n{FORMAT_VERSION_KEY}={future}\n"),
        );

        let error = open(&storage, PartitionInterval::Month).expect_err("must refuse");

        assert!(
            matches!(
                error,
                InstanceError::UnknownFormatVersion { found, supported }
                    if found == future && supported == FORMAT_VERSION
            ),
            "unexpected error: {error}"
        );
        // Both numbers, or an operator cannot tell which build to run.
        let message = error.to_string();
        assert!(
            message.contains(&future.to_string()) && message.contains(&FORMAT_VERSION.to_string()),
            "{message}"
        );
    }

    /// §12: an archive from before versioning existed is refused rather than
    /// assumed to be version 1. Assuming would be a compatibility claim owed
    /// forever, for archives only `0.1.0-alpha.1` could have written.
    #[test]
    fn metadata_predating_versioning_refuses_to_start() {
        let storage = InMemoryStorage::new();
        write(&storage, METADATA_PATH, "partition_interval=month\n");

        let error = open(&storage, PartitionInterval::Month).expect_err("must refuse");

        assert!(
            matches!(error, InstanceError::PredatesVersioning),
            "unexpected error: {error}"
        );
        // It has to say what to do, because there is nothing to repair.
        assert!(error.to_string().contains("re-create"), "{error}");
    }

    /// A version that is present but not a number is damage, not a version from
    /// the future — the two must not be reported as the same thing.
    #[test]
    fn an_unparsable_format_version_is_corrupt() {
        let storage = InMemoryStorage::new();
        write(
            &storage,
            METADATA_PATH,
            &format!("partition_interval=month\n{FORMAT_VERSION_KEY}=tuesday\n"),
        );

        assert!(matches!(
            open(&storage, PartitionInterval::Month),
            Err(InstanceError::Corrupt { .. })
        ));
    }

    /// The version is checked before the interval: a build that cannot read the
    /// format cannot conclude anything about what the interval means.
    #[test]
    fn an_unknown_version_is_reported_ahead_of_an_interval_mismatch() {
        let storage = InMemoryStorage::new();
        write(
            &storage,
            METADATA_PATH,
            &format!(
                "partition_interval=month\n{FORMAT_VERSION_KEY}={}\n",
                FORMAT_VERSION + 1
            ),
        );

        let error = open(&storage, PartitionInterval::Day).expect_err("must refuse");

        assert!(
            matches!(error, InstanceError::UnknownFormatVersion { .. }),
            "unexpected error: {error}"
        );
    }
}
