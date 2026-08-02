// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Opening an archive: the startup sequence (§4, §6, §10, §11).
//!
//! The order is not incidental. Each step depends on the one before it:
//!
//! 1. **Instance metadata** (§11) — validate the configured partition interval
//!    against what this instance was initialised with, and refuse before touching
//!    anything else. A mismatch here means every path derived later would be
//!    wrong.
//! 2. **Catalogue log** (§4) — registrations and schemas, replayed *before* the
//!    data log, so every id and every schema is known before any data referring
//!    to them is read.
//! 3. **Sequence source** (§1) — resume past the highest `seq` any log holds.
//!    Restarting the counter at zero would hand new versions numbers that
//!    already exist, and version resolution takes the highest `seq` per logical
//!    row: an old version would win over a newer one, permanently.
//! 4. **Data log** — replayed last. Reading its records back into memtables needs
//!    the batch encoding that arrives with `do_put`; until then this step
//!    recovers the sequence position and nothing else, and says so.
//!
//! The sequence position comes from *both* logs, because `seq` is issued from one
//! source across them (§4) and a catalogue change may hold the highest value —
//! an instance that registered a series and then crashed before any write has
//! its high-water mark in the catalogue log alone.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::{CatalogueError, TableCatalogue, TableConfig};
use crate::catalog_log::{CATALOG_LOG_DIR, DATA_LOG_DIR, WalCatalogLog, replay_catalog};
use crate::clock::Clock;
use crate::instance::{InstanceError, InstanceMetadata};
use crate::partition::PartitionInterval;
use crate::registry::{MetricRegistry, RegistryError};
use crate::seq::{AtomicSequence, Seq, SequenceSource};
use crate::stamp::Stamper;
use crate::storage::Storage;
use crate::table::PartitionedTable;
use crate::wal::{Wal, WalConfig, WalError, replay};

/// Why an archive could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    /// The instance's recorded layout disagrees with the configuration (§11).
    #[error(transparent)]
    Instance(#[from] InstanceError),
    /// A log could not be read or opened.
    #[error(transparent)]
    Wal(#[from] WalError),
    /// A recovered registration could not be restored.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// A recovered schema could not be restored.
    #[error(transparent)]
    Catalogue(#[from] CatalogueError),
}

/// How an archive is configured at startup.
///
/// Everything here is an operator's choice, re-read at every start. Nothing in it
/// is recovered from disk — what *is* recovered validates against it (§11).
#[derive(Debug, Clone, Default)]
pub struct ArchiveConfig {
    /// Layout granularity, validated against the instance metadata (§11).
    pub partition_interval: PartitionInterval,
    /// How both logs are sized and how long they group commits (§4).
    pub wal: WalConfig,
    /// Applies to any table no entry in `tables` names.
    pub default_table: TableConfig,
    /// Per-table configuration (§6): suppression reach, later retention.
    pub tables: Vec<(String, TableConfig)>,
}

/// What a startup recovered, for the operator to see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Recovered {
    /// Series restored from the catalogue log.
    pub registrations: usize,
    /// Schema records restored from the catalogue log.
    pub schemas: usize,
    /// Catalogue records skipped as undecodable (§4).
    pub undecodable: usize,
    /// Data records the data log still holds.
    ///
    /// Counted but not yet applied: reading them back into memtables needs the
    /// batch encoding that arrives with `do_put`.
    pub data_records: usize,
    /// Whether a log's tail was discarded as torn (§4).
    pub truncated: bool,
    /// The `seq` the sequence source resumed from.
    pub resumed_at: Seq,
}

/// One opened archive: its recovered catalogue and its ingestion state.
#[derive(Debug)]
pub struct Archive {
    interval: PartitionInterval,
    registry: Arc<MetricRegistry>,
    catalogue: Arc<TableCatalogue>,
    stamper: Arc<Stamper>,
    tables: dashmap::DashMap<String, Arc<PartitionedTable>>,
    data_log: Arc<Wal>,
    recovered: Recovered,
}

impl Archive {
    /// Opens the archive in `storage`, recovering what the logs hold.
    ///
    /// Refuses to start rather than adapting when the configured layout
    /// disagrees with the instance's (§11).
    pub fn open(
        storage: Arc<dyn Storage>,
        config: &ArchiveConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ArchiveError> {
        // 1. Layout first: everything derived later depends on it.
        let metadata = InstanceMetadata::open(Arc::clone(&storage), config.partition_interval)?;
        let interval = metadata.partition_interval();

        // 2. The catalogue, before any data.
        let catalog_dir = PathBuf::from(CATALOG_LOG_DIR);
        let data_dir = PathBuf::from(DATA_LOG_DIR);
        let recovered_catalogue = replay_catalog(storage.as_ref(), &catalog_dir)?;

        // 3. Resume the sequence past everything either log holds, before any
        //    component can issue a number.
        let catalogue_high = highest_seq(storage.as_ref(), &catalog_dir)?;
        let data = replay(storage.as_ref(), &data_dir)?;
        let data_high = data.records.iter().map(|record| record.seq).max();
        let resume_at = [catalogue_high, data_high]
            .into_iter()
            .flatten()
            .max()
            // The next number, not the one already used.
            .map_or(0, |highest| highest.saturating_add(1));

        let sequence: Arc<dyn SequenceSource> = Arc::new(AtomicSequence::starting_at(resume_at));

        // Both logs open after their replay, so neither is appended to before it
        // has been read — and opening always starts a fresh segment (§4).
        let catalog_log = Arc::new(Wal::open(Arc::clone(&storage), catalog_dir, config.wal)?);
        let data_log = Arc::new(Wal::open(Arc::clone(&storage), data_dir, config.wal)?);
        let catalog_sink = Arc::new(WalCatalogLog::new(catalog_log, Arc::clone(&sequence)));

        let registry = Arc::new(MetricRegistry::new(Arc::clone(&catalog_sink) as Arc<_>));
        registry.replay(recovered_catalogue.registrations.clone())?;

        let catalogue = Arc::new(TableCatalogue::new(
            Arc::clone(&catalog_sink) as Arc<_>,
            config.default_table,
        ));
        catalogue.replay(recovered_catalogue.schemas.clone())?;
        for (table, table_config) in &config.tables {
            catalogue.configure(table, *table_config);
        }

        let recovered = Recovered {
            registrations: recovered_catalogue.registrations.len(),
            schemas: recovered_catalogue.schemas.len(),
            undecodable: recovered_catalogue.undecodable,
            data_records: data.records.len(),
            truncated: recovered_catalogue.truncated.is_some() || data.truncated.is_some(),
            resumed_at: resume_at,
        };

        Ok(Self {
            interval,
            registry,
            catalogue,
            stamper: Arc::new(Stamper::new(clock, sequence)),
            tables: dashmap::DashMap::new(),
            data_log,
            recovered,
        })
    }

    /// What this startup recovered.
    pub fn recovered(&self) -> Recovered {
        self.recovered
    }

    /// The layout every partition path derives from (§11).
    pub fn interval(&self) -> PartitionInterval {
        self.interval
    }

    /// The metric registry (§10).
    pub fn registry(&self) -> &Arc<MetricRegistry> {
        &self.registry
    }

    /// The table catalogue (§6).
    pub fn catalogue(&self) -> &Arc<TableCatalogue> {
        &self.catalogue
    }

    /// The stamper issuing `system_time` and `seq` (§1).
    pub fn stamper(&self) -> &Arc<Stamper> {
        &self.stamper
    }

    /// The data log, the only one reclamation applies to (§4).
    pub fn data_log(&self) -> &Arc<Wal> {
        &self.data_log
    }

    /// The in-memory state of `table`, created on first use.
    ///
    /// The table must be known to the catalogue — `do_put` calls
    /// [`TableCatalogue::accept`] first, which is what admits it.
    pub fn table(&self, table: &str) -> Option<Arc<PartitionedTable>> {
        let schema = self.catalogue.schema(table)?;
        if let Some(held) = self.tables.get(table) {
            return Some(Arc::clone(held.value()));
        }

        let created = Arc::new(PartitionedTable::new(
            schema.storage(),
            self.interval,
            self.catalogue.config(table).suppression,
        ));
        Some(Arc::clone(
            self.tables
                .entry(table.to_owned())
                .or_insert(created)
                .value(),
        ))
    }
}

/// The highest `seq` any record in the log under `dir` holds.
fn highest_seq(storage: &dyn Storage, dir: &Path) -> Result<Option<Seq>, WalError> {
    Ok(replay(storage, dir)?
        .records
        .iter()
        .map(|record| record.seq)
        .max())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::clock::{Micros, TestClock};
    use crate::registry::SeriesKey;
    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
    use crate::storage::InMemoryStorage;
    use crate::suppression::SuppressionConfig;
    use crate::wal::{RecordKind, WalRecord, WalWriter};

    const HOUR: Micros = 3_600_000_000;

    fn config() -> ArchiveConfig {
        ArchiveConfig {
            wal: WalConfig {
                segment_bytes: 64 * 1024,
                max_commit_delay: Duration::ZERO,
            },
            ..ArchiveConfig::default()
        }
    }

    fn open(storage: &InMemoryStorage, config: &ArchiveConfig) -> Result<Archive, ArchiveError> {
        Archive::open(
            Arc::new(storage.clone()),
            config,
            Arc::new(TestClock::new(1_000)),
        )
    }

    fn series(name: &str) -> SeriesKey {
        SeriesKey::new(name, std::iter::empty())
    }

    fn client_schema() -> Schema {
        Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ])
    }

    #[test]
    fn a_fresh_archive_recovers_nothing() {
        let storage = InMemoryStorage::new();
        let archive = open(&storage, &config()).unwrap();

        assert_eq!(archive.recovered(), Recovered::default());
        assert_eq!(archive.interval(), PartitionInterval::Month);
    }

    /// §11: the layout is validated before anything else, so a mismatch stops the
    /// start rather than producing paths from the wrong granularity.
    #[test]
    fn a_layout_mismatch_refuses_to_start() {
        let storage = InMemoryStorage::new();
        open(&storage, &config()).unwrap();

        let mismatched = ArchiveConfig {
            partition_interval: PartitionInterval::Hour,
            ..config()
        };
        assert!(matches!(
            open(&storage, &mismatched),
            Err(ArchiveError::Instance(
                InstanceError::IntervalMismatch { .. }
            ))
        ));
    }

    /// §4, §6, §10: a restart recovers the catalogue from its log — both halves.
    #[test]
    fn a_restart_recovers_registrations_and_schemas() {
        let storage = InMemoryStorage::new();
        let assigned;
        {
            let archive = open(&storage, &config()).unwrap();
            assigned = archive.registry().register(series("temp")).unwrap();
            archive
                .catalogue()
                .accept("readings", &client_schema())
                .unwrap();
        }

        let archive = open(&storage, &config()).unwrap();

        assert_eq!(archive.recovered().registrations, 1);
        assert_eq!(archive.recovered().schemas, 1);
        assert_eq!(archive.recovered().undecodable, 0);
        assert_eq!(archive.registry().resolve(&series("temp")), Some(assigned));
        assert!(archive.catalogue().schema("readings").is_some());
    }

    /// §1: this is the one that would corrupt time travel silently. Restarting the
    /// counter at zero would issue `seq` values that already exist, and version
    /// resolution takes the highest `seq` per logical row — an old version would
    /// win over a newer one, for good.
    #[test]
    fn a_restart_resumes_the_sequence_past_everything_logged() {
        let storage = InMemoryStorage::new();

        // A data log holding high sequence numbers.
        let mut data = WalWriter::open(
            Arc::new(storage.clone()),
            PathBuf::from(DATA_LOG_DIR),
            config().wal,
        )
        .unwrap();
        data.write_all(&[WalRecord {
            kind: RecordKind::Data,
            seq: 5_000,
            payload: b"rows".to_vec(),
        }])
        .unwrap();
        drop(data);

        let archive = open(&storage, &config()).unwrap();

        assert_eq!(archive.recovered().resumed_at, 5_001);
        assert!(
            archive.stamper().issue(1).seq_start >= 5_001,
            "a reissued seq would let an old version outrank a newer one"
        );
    }

    /// §4: `seq` comes from one source across both logs, so an instance that only
    /// ever registered a series still has its high-water mark in the catalogue log.
    #[test]
    fn the_sequence_resumes_from_the_catalogue_log_alone() {
        let storage = InMemoryStorage::new();
        {
            let archive = open(&storage, &config()).unwrap();
            for name in ["a", "b", "c"] {
                archive.registry().register(series(name)).unwrap();
            }
        }

        let archive = open(&storage, &config()).unwrap();

        assert!(
            archive.recovered().resumed_at >= 3,
            "three registrations consumed three numbers, resumed at {}",
            archive.recovered().resumed_at
        );
        assert_eq!(archive.recovered().data_records, 0);
    }

    /// Two restarts must not reuse a number either — the high-water mark has to
    /// account for what the previous run appended, not just the first.
    #[test]
    fn repeated_restarts_never_reissue_a_sequence_number() {
        let storage = InMemoryStorage::new();
        let mut issued = Vec::new();

        for round in 0..4 {
            let archive = open(&storage, &config()).unwrap();
            archive
                .registry()
                .register(series(&format!("series-{round}")))
                .unwrap();
            issued.push(archive.stamper().issue(2).seq_start);
        }

        let mut sorted = issued.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            issued.len(),
            "a number was reissued: {issued:?}"
        );
    }

    /// §6, §1: a table's in-memory state takes the layout and the reach its
    /// configuration names, not an instance-wide default.
    #[test]
    fn a_table_takes_its_configured_reach_and_the_instance_layout() {
        let storage = InMemoryStorage::new();
        let config = ArchiveConfig {
            partition_interval: PartitionInterval::Day,
            tables: vec![(
                "forecast".to_owned(),
                TableConfig {
                    suppression: SuppressionConfig::for_recomputed(HOUR),
                },
            )],
            ..config()
        };

        let archive = open(&storage, &config).unwrap();
        archive
            .catalogue()
            .accept("forecast", &client_schema())
            .unwrap();
        archive
            .catalogue()
            .accept("readings", &client_schema())
            .unwrap();

        let forecast = archive.table("forecast").expect("forecast");
        assert_eq!(forecast.interval(), PartitionInterval::Day);
        assert_eq!(forecast.suppression().config().window, HOUR);

        let readings = archive.table("readings").expect("readings");
        assert_eq!(
            readings.suppression().config().window,
            SuppressionConfig::for_stream().window,
            "an unconfigured table falls back to the instance default"
        );
    }

    /// A table the catalogue has not admitted has no state: `do_put` accepts the
    /// schema first, and that is what creates the table.
    #[test]
    fn an_unknown_table_has_no_state() {
        let storage = InMemoryStorage::new();
        let archive = open(&storage, &config()).unwrap();

        assert!(archive.table("absent").is_none());
    }

    #[test]
    fn asking_for_the_same_table_twice_yields_the_same_state() {
        let storage = InMemoryStorage::new();
        let archive = open(&storage, &config()).unwrap();
        archive
            .catalogue()
            .accept("readings", &client_schema())
            .unwrap();

        assert!(Arc::ptr_eq(
            &archive.table("readings").unwrap(),
            &archive.table("readings").unwrap()
        ));
    }

    /// §4: a torn tail must not prevent a start, and it must be reported.
    #[test]
    fn a_torn_catalogue_tail_is_reported_and_does_not_prevent_a_start() {
        let storage = InMemoryStorage::new();
        {
            let archive = open(&storage, &config()).unwrap();
            archive.registry().register(series("intact")).unwrap();
            storage.inject(crate::storage::Fault::TornWrite { keep: 6 });
            archive.registry().register(series("torn")).unwrap();
        }

        let archive = open(&storage, &config()).expect("a torn tail must not stop a start");

        assert!(archive.recovered().truncated);
        assert_eq!(archive.recovered().registrations, 1);
    }
}
