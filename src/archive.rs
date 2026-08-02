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

use arrow::array::RecordBatch;

use crate::catalog::{CatalogueError, TableCatalogue, TableConfig};
use crate::catalog_log::{CATALOG_LOG_DIR, DATA_LOG_DIR, WalCatalogLog, replay_catalog};
use crate::clock::{Clock, Micros};
use crate::data_log::{encode_batch, replay_data};
use crate::instance::{InstanceError, InstanceMetadata};
use crate::memtable::{InsertOutcome, MemTableError};
use crate::partition::{PartitionError, PartitionInterval};
use crate::registry::{MetricRegistry, RegistryError};
use crate::retention::{cutoff, is_expired};
use crate::schema::{SEQ_COLUMN, TIMESTAMP_COLUMN};
use crate::seq::{AtomicSequence, Seq, SequenceSource};
use crate::stamp::StampError;
use crate::stamp::Stamper;
use crate::storage::Storage;
use crate::table::PartitionedTable;
use crate::table::TableError;
use crate::wal::{RecordKind, Wal, WalConfig, WalError, replay};

/// Why an ingest was refused.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The batch's schema is not compatible with the table's (§6).
    #[error(transparent)]
    Catalogue(#[from] CatalogueError),
    /// The batch could not be stamped with its system columns (§1).
    #[error(transparent)]
    Stamp(#[from] StampError),
    /// The batch could not be made durable, so it was not acknowledged (§4).
    #[error(transparent)]
    Wal(#[from] WalError),
    /// The batch could not be routed or admitted (§3).
    #[error(transparent)]
    Table(#[from] TableError),
    /// The memtables are at their cap and flushing is behind (§7).
    #[error(
        "memtables hold {held} bytes of a {cap} byte cap; \
         retry once the archive has flushed"
    )]
    Exhausted {
        /// Bytes currently held.
        held: usize,
        /// The configured cap.
        cap: usize,
    },
    /// The batch reaches into a partition retention has already released (§8).
    ///
    /// The one age-based rejection in the system, and an explicit configured
    /// policy — not the lateness watermark §3 rules out. Retrying unchanged
    /// never helps: the cutoff only moves further forward.
    #[error(
        "the oldest row is stamped {oldest} µs, in a partition entirely past the \
         retention cutoff of {cutoff} µs; that partition may be dropped at any \
         time, so the batch is refused rather than written and reaped"
    )]
    Expired {
        /// Valid time of the oldest row in the refused batch.
        oldest: Micros,
        /// The cutoff it fell behind.
        cutoff: Micros,
    },
    /// A row's timestamp could not be placed on the calendar (§11).
    #[error(transparent)]
    Partition(#[from] PartitionError),
}

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
    /// A replayed batch could not be routed back into memory.
    #[error(transparent)]
    Table(#[from] TableError),
}

/// How an archive is configured at startup.
///
/// Everything here is an operator's choice, re-read at every start. Nothing in it
/// is recovered from disk — what *is* recovered validates against it (§11).
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// Layout granularity, validated against the instance metadata (§11).
    pub partition_interval: PartitionInterval,
    /// How both logs are sized and how long they group commits (§4).
    pub wal: WalConfig,
    /// Applies to any table no entry in `tables` names.
    pub default_table: TableConfig,
    /// Per-table configuration (§6): suppression reach, later retention.
    pub tables: Vec<(String, TableConfig)>,
    /// Hard cap on bytes the memtables may hold before ingestion is refused (§7).
    ///
    /// Past this an insert answers `RESOURCE_EXHAUSTED` rather than buffering
    /// without bound or blocking indefinitely — a client then sees a cause
    /// instead of a timeout.
    pub max_memtable_bytes: usize,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            partition_interval: PartitionInterval::default(),
            wal: WalConfig::default(),
            default_table: TableConfig::default(),
            tables: Vec::new(),
            // A gigabyte: large enough that a healthy instance never meets it,
            // small enough that an overloaded one refuses before the host does.
            max_memtable_bytes: 1024 * 1024 * 1024,
        }
    }
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
    /// Batches replayed from the data log into memtables.
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
    clock: Arc<dyn Clock>,
    stamper: Arc<Stamper>,
    tables: dashmap::DashMap<String, Arc<PartitionedTable>>,
    data_log: Arc<Wal>,
    max_memtable_bytes: usize,
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
            data_records: 0,
            truncated: recovered_catalogue.truncated.is_some() || data.truncated.is_some(),
            resumed_at: resume_at,
        };

        let archive = Self {
            interval,
            registry,
            catalogue,
            clock: Arc::clone(&clock),
            stamper: Arc::new(Stamper::new(clock, sequence)),
            tables: dashmap::DashMap::new(),
            data_log,
            max_memtable_bytes: config.max_memtable_bytes,
            recovered,
        };

        // 4. The data log last, into the memtables the catalogue now describes.
        //    Batches replay stamped, so recovery reproduces the versions the
        //    original run created rather than issuing new ones (§1).
        let replayed_data = replay_data(storage.as_ref(), Path::new(DATA_LOG_DIR))?;
        let mut applied = 0;
        for record in &replayed_data.records {
            if let Some(state) = archive.table(&record.table) {
                state.insert(&record.batch)?;
                applied += 1;
            }
            // A batch whose table the catalogue does not know is damage: the
            // schema record that would describe it is missing. Counted below
            // rather than fatal, since §4 forbids failing a start.
        }

        let mut archive = archive;
        archive.recovered.data_records = applied;
        archive.recovered.undecodable +=
            replayed_data.undecodable + replayed_data.records.len().saturating_sub(applied);
        archive.recovered.truncated |= replayed_data.truncated;

        Ok(archive)
    }

    /// Admits one client batch into `table` (§1, §3, §4, §6).
    ///
    /// The order is the contract. The schema is admitted first, so a rejected
    /// batch never reaches the log. Then the batch is stamped, then made durable,
    /// and only then routed into memory — because §4 requires that an
    /// acknowledgement means the rows survive a crash, and a caller learns the
    /// outcome only after the fsync its record was part of returned.
    ///
    /// Refuses with [`IngestError::Exhausted`] past the memory cap rather than
    /// buffering without bound (§7).
    pub fn ingest(&self, table: &str, batch: &RecordBatch) -> Result<InsertOutcome, IngestError> {
        // §7: check before doing any work, so an overloaded instance spends
        // nothing on a batch it is about to refuse.
        let held = self.memtable_bytes();
        if held >= self.max_memtable_bytes {
            return Err(IngestError::Exhausted {
                held,
                cap: self.max_memtable_bytes,
            });
        }

        // §6: evolution and rejection happen before anything is logged. This
        // also settles that the timestamp column is present and typed, which the
        // retention check below reads.
        self.catalogue.accept(table, batch.schema().as_ref())?;
        self.refuse_if_expired(table, batch)?;
        let stamped = self.stamper.stamp(batch)?;

        // §4: durable before acknowledged, and before it is visible in memory —
        // a row in a memtable that no log holds would vanish on a crash.
        let seq = first_seq(&stamped);
        self.data_log.append(
            RecordKind::Data,
            seq,
            encode_batch(table, &stamped).map_err(IngestError::Catalogue)?,
        )?;

        let state = self.table(table).ok_or_else(|| {
            IngestError::Table(TableError::MemTable(MemTableError::WrongColumnType(
                table.to_owned(),
            )))
        })?;
        Ok(state.insert(&stamped)?)
    }

    /// Refuses a batch reaching into a partition retention has released (§8).
    ///
    /// Exactly the reaper's rule, applied to the batch's oldest row: if that
    /// row's partition is already droppable, writing it would mean acknowledging
    /// data the next reaping pass deletes. A row merely older than the horizon
    /// but sharing a partition with live data is accepted, because that
    /// partition is kept and served (§8, §11).
    ///
    /// The whole batch is refused rather than filtered: splitting it would copy
    /// it, which the hot-path rule forbids, and a client sending expired data has
    /// a bug worth reporting rather than silently trimming.
    ///
    /// A table without a retention duration — the default — costs one map lookup
    /// and no scan at all.
    fn refuse_if_expired(&self, table: &str, batch: &RecordBatch) -> Result<(), IngestError> {
        let Some(retention) = self.catalogue.config(table).retention else {
            return Ok(());
        };
        let Some(oldest) = oldest_timestamp(batch) else {
            return Ok(());
        };

        let cutoff = cutoff(self.clock.now_micros(), retention);
        if is_expired(self.interval, oldest, cutoff)? {
            return Err(IngestError::Expired { oldest, cutoff });
        }
        Ok(())
    }

    /// Bytes the memtables of every table currently hold (§7).
    pub fn memtable_bytes(&self) -> usize {
        self.tables
            .iter()
            .map(|entry| entry.value().memory_bytes())
            .sum()
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

/// The earliest valid time a client batch carries, if it carries any (§8).
///
/// An Arrow aggregate over the one column, not a row walk: the hot-path rule
/// bans per-row iteration, and this runs only for a table with a retention
/// duration.
fn oldest_timestamp(batch: &RecordBatch) -> Option<Micros> {
    arrow::compute::min(
        batch
            .column_by_name(TIMESTAMP_COLUMN)?
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?,
    )
}

/// The first `seq` a stamped batch carries, which numbers its log record.
fn first_seq(stamped: &RecordBatch) -> Seq {
    stamped
        .column_by_name(SEQ_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
        .and_then(|seqs| seqs.values().first().copied())
        .unwrap_or_default()
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
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
                    ..TableConfig::default()
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

    fn client_batch(rows: &[(u64, Micros, f64)]) -> arrow::array::RecordBatch {
        use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
        arrow::array::RecordBatch::try_new(
            Arc::new(client_schema()),
            vec![
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// §1, §4, §6: the whole ingest in one call — schema admitted, stamped, made
    /// durable, then routed.
    #[test]
    fn ingest_admits_stamps_and_routes_a_batch() {
        let storage = InMemoryStorage::new();
        let archive = open(&storage, &config()).unwrap();

        let outcome = archive
            .ingest(
                "readings",
                &client_batch(&[(1, 1_000, 1.5), (2, 2_000, 2.5)]),
            )
            .expect("ingest");

        assert_eq!(outcome.inserted, 2);
        assert!(archive.catalogue().schema("readings").is_some());
        assert_eq!(archive.table("readings").unwrap().num_rows(), 2);
    }

    /// §6: a rejected schema must never reach the log — otherwise a replay would
    /// admit what the live path refused.
    #[test]
    fn a_rejected_schema_is_not_logged() {
        let storage = InMemoryStorage::new();
        let archive = open(&storage, &config()).unwrap();
        archive
            .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
            .unwrap();

        let conflicting = Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Int64, true),
        ]);
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(conflicting),
            vec![
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1_000i64,
                ])),
                Arc::new(arrow::array::UInt64Array::from(vec![1u64])),
                Arc::new(arrow::array::Int64Array::from(vec![5i64])),
            ],
        )
        .unwrap();

        assert!(matches!(
            archive.ingest("readings", &batch),
            Err(IngestError::Catalogue(_))
        ));

        // The reopened archive must hold only the accepted batch.
        let reopened = open(&storage, &config()).unwrap();
        assert_eq!(reopened.recovered().data_records, 1);
    }

    /// §4: an acknowledged batch survives a crash, and replay reproduces the
    /// versions the original run created rather than issuing new ones.
    #[test]
    fn ingested_batches_replay_into_memory_after_a_restart() {
        let storage = InMemoryStorage::new();
        {
            let archive = open(&storage, &config()).unwrap();
            archive
                .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
                .unwrap();
            archive
                .ingest("readings", &client_batch(&[(2, 2_000, 2.5)]))
                .unwrap();
        }

        let archive = open(&storage, &config()).unwrap();

        assert_eq!(archive.recovered().data_records, 2);
        assert_eq!(
            archive.table("readings").expect("readings").num_rows(),
            2,
            "acknowledged rows must be back in memory"
        );
        assert_eq!(archive.recovered().undecodable, 0);
    }

    /// §1: a replayed batch keeps its `seq`, so a repeat after a restart is still
    /// recognised as unchanged rather than becoming a new version.
    #[test]
    fn suppression_still_works_against_replayed_rows() {
        let storage = InMemoryStorage::new();
        {
            let archive = open(&storage, &config()).unwrap();
            archive
                .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
                .unwrap();
        }

        let archive = open(&storage, &config()).unwrap();
        let outcome = archive
            .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
            .expect("ingest");

        assert_eq!(
            outcome.suppressed, 1,
            "a repeat of a replayed row must not manufacture a version"
        );
    }

    /// §7: past the cap ingestion is refused with a cause, rather than buffering
    /// without bound or blocking until the client times out.
    #[test]
    fn ingestion_past_the_memory_cap_is_refused() {
        let storage = InMemoryStorage::new();
        let archive = open(
            &storage,
            &ArchiveConfig {
                max_memtable_bytes: 1,
                ..config()
            },
        )
        .unwrap();

        // The first batch is admitted: the cap is checked against what is held,
        // and nothing is held yet.
        archive
            .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
            .unwrap();

        let error = archive
            .ingest("readings", &client_batch(&[(2, 2_000, 2.5)]))
            .expect_err("must be refused");

        assert!(matches!(error, IngestError::Exhausted { .. }), "{error}");
        let message = error.to_string();
        assert!(
            message.contains('1'),
            "the cause must name the cap: {message}"
        );
    }

    /// §7: a refusal must leave nothing behind — no log record, no rows.
    #[test]
    fn a_refused_batch_is_not_logged() {
        let storage = InMemoryStorage::new();
        {
            let archive = open(
                &storage,
                &ArchiveConfig {
                    max_memtable_bytes: 1,
                    ..config()
                },
            )
            .unwrap();
            archive
                .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
                .unwrap();
            assert!(
                archive
                    .ingest("readings", &client_batch(&[(2, 2_000, 2.5)]))
                    .is_err()
            );
        }

        let reopened = open(&storage, &config()).unwrap();
        assert_eq!(
            reopened.recovered().data_records,
            1,
            "the refused batch must not be in the log"
        );
    }

    /// §3, §11: one call may span partitions, and the routing survives a restart.
    #[test]
    fn a_batch_spanning_partitions_replays_into_both() {
        const SEPTEMBER: Micros = 1_788_220_800_000_000;
        const AUGUST: Micros = 1_785_681_432_000_000;

        let storage = InMemoryStorage::new();
        {
            let archive = open(&storage, &config()).unwrap();
            archive
                .ingest(
                    "readings",
                    &client_batch(&[(1, AUGUST, 1.5), (1, SEPTEMBER, 2.5)]),
                )
                .unwrap();
            assert_eq!(
                archive.table("readings").unwrap().partition_starts().len(),
                2
            );
        }

        let archive = open(&storage, &config()).unwrap();
        assert_eq!(
            archive.table("readings").unwrap().partition_starts().len(),
            2,
            "both partitions must come back"
        );
    }

    // ---- retention (§8) ----

    /// 2026-08-02T14:37:12Z, and two months before it.
    const AUGUST: Micros = 1_785_681_432_000_000;
    const JUNE: Micros = 1_781_481_600_000_000;
    const DAY: Micros = 86_400_000_000;

    fn retaining(days: Micros) -> ArchiveConfig {
        ArchiveConfig {
            tables: vec![(
                "readings".to_owned(),
                TableConfig {
                    retention: Some(days * DAY),
                    ..TableConfig::default()
                },
            )],
            ..config()
        }
    }

    fn at(storage: &InMemoryStorage, config: &ArchiveConfig, now: Micros) -> Archive {
        Archive::open(
            Arc::new(storage.clone()),
            config,
            Arc::new(TestClock::new(now)),
        )
        .expect("archive")
    }

    /// §8: data whose partition the reaper is already entitled to drop is
    /// refused outright, rather than written and deleted moments later.
    #[test]
    fn ingesting_already_expired_data_is_refused() {
        let storage = InMemoryStorage::new();
        let archive = at(&storage, &retaining(30), AUGUST);

        let error = archive
            .ingest("readings", &client_batch(&[(1, JUNE, 1.5)]))
            .expect_err("must be refused");

        assert!(matches!(error, IngestError::Expired { .. }), "{error}");
        assert!(archive.table("readings").is_none_or(|t| t.num_rows() == 0));
    }

    /// §8: the refusal is exactly the reaper's rule. A row inside a partition
    /// still partly live is accepted, because that partition will be served —
    /// refusing it would reject data the archive would in fact have kept.
    #[test]
    fn a_row_older_than_the_horizon_but_in_a_live_partition_is_accepted() {
        let storage = InMemoryStorage::new();
        // A one-day horizon at 2026-08-02T14:37 puts the cutoff at
        // 2026-08-01T14:37 — inside August, whose partition therefore survives.
        let archive = at(&storage, &retaining(1), AUGUST);
        let first_of_august = PartitionInterval::Month.start(AUGUST).unwrap();

        let outcome = archive
            .ingest("readings", &client_batch(&[(1, first_of_august, 1.5)]))
            .expect("a live partition must accept a row behind the horizon");

        assert_eq!(outcome.inserted, 1, "the row is older than the horizon");
        assert!(
            first_of_august < crate::retention::cutoff(AUGUST, DAY),
            "the test is only meaningful if the row is behind the cutoff"
        );
    }

    /// §8: unset retention keeps forever, so nothing is ever too old.
    #[test]
    fn without_retention_no_data_is_too_old() {
        let storage = InMemoryStorage::new();
        let archive = at(&storage, &config(), AUGUST);

        assert!(
            archive
                .ingest("readings", &client_batch(&[(1, 0, 1.5)]))
                .is_ok(),
            "1970 must be accepted by a table that keeps everything"
        );
    }

    /// §4: a refused batch must leave nothing behind — no log record, no rows.
    #[test]
    fn an_expired_batch_is_not_logged() {
        let storage = InMemoryStorage::new();
        {
            let archive = at(&storage, &retaining(30), AUGUST);
            archive
                .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
                .expect("live");
            assert!(
                archive
                    .ingest("readings", &client_batch(&[(1, JUNE, 2.5)]))
                    .is_err()
            );
        }

        let reopened = at(&storage, &config(), AUGUST);
        assert_eq!(reopened.recovered().data_records, 1);
    }

    /// The batch is passed through uncopied (hot-path rule), so it is refused
    /// whole rather than split — but the refusal must name the row that caused
    /// it, or an operator cannot tell which of a thousand rows was too old.
    #[test]
    fn a_batch_with_one_expired_row_is_refused_whole_and_names_it() {
        let storage = InMemoryStorage::new();
        let archive = at(&storage, &retaining(30), AUGUST);

        let error = archive
            .ingest(
                "readings",
                &client_batch(&[(1, AUGUST, 1.5), (1, JUNE, 2.5)]),
            )
            .expect_err("must be refused");

        assert!(matches!(error, IngestError::Expired { oldest, .. } if oldest == JUNE));
        assert_eq!(
            archive.table("readings").map_or(0, |t| t.num_rows()),
            0,
            "no part of a refused batch may land"
        );
    }

    /// §8: the cutoff moves with the clock, so the same batch that is accepted
    /// today is refused once its partition has cleared.
    #[test]
    fn the_same_batch_is_refused_once_its_partition_has_cleared() {
        let storage = InMemoryStorage::new();
        let clock = Arc::new(TestClock::new(AUGUST));
        let archive = Archive::open(
            Arc::new(storage.clone()),
            &retaining(30),
            Arc::clone(&clock) as Arc<_>,
        )
        .unwrap();

        archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .expect("august is live");

        clock.set(AUGUST + 120 * DAY);
        assert!(
            archive
                .ingest("readings", &client_batch(&[(1, AUGUST, 2.5)]))
                .is_err(),
            "august has cleared the cutoff by December"
        );
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
