// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The table catalogue (§6).
//!
//! Maps a table name to the two things a table has, which are of different
//! natures and come from different places:
//!
//! - Its **schema**, which is state. It grows through evolution on ingest, so
//!   nothing outside the archive knows what it currently is, and it is recorded
//!   in the catalogue log (§4). Without that record the type-change rejection
//!   below stops working across a restart — a client sending a conflicting type
//!   would be admitted and the clash would surface much later as Parquet that
//!   cannot be read across files.
//! - Its **configuration**, which is an operator's choice: the suppression window
//!   and cap (§1), and later the retention duration (§8). It comes from instance
//!   configuration and is re-read at every start, never from the log.
//!
//! [`TableCatalogue::accept`] is the ingest path's entry point. It applies §6's
//! evolution rules, records an accepted change before publishing it, and hands
//! back the storage schema the memtable expects. An unchanged schema is not a
//! change and never reaches the log, so a steady stream appends nothing.
//!
//! # Durable before visible
//!
//! An evolution is published only once its record is durable. Publishing first
//! would let the server accept batches against a schema a restart then loses,
//! and those batches are already in Parquet by then.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use dashmap::DashMap;

use crate::catalog_log::SchemaRecord;
use crate::clock::Micros;
use crate::schema::{SchemaError, TableSchema};
use crate::suppression::SuppressionConfig;

/// Why a catalogue operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CatalogueError {
    /// The batch's schema is not compatible with the table's (§6).
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// The schema could not be encoded for the log, so nothing was written.
    #[error("could not encode the schema: {0}")]
    Encoding(#[from] arrow::error::ArrowError),
    /// The schema change could not be made durable, so it was not published.
    #[error("could not persist the schema: {0}")]
    Persistence(Box<dyn std::error::Error + Send + Sync>),
}

/// Sink making an accepted schema durable.
///
/// The seam between the catalogue and the catalogue log (§4), so schema
/// durability can be tested with injected failures.
pub trait SchemaLog: std::fmt::Debug + Send + Sync {
    /// Persists `record`, returning only once it is durable.
    fn append(&self, record: &SchemaRecord) -> Result<(), CatalogueError>;
}

/// What differs between tables by workload rather than by deployment (§6).
#[derive(Debug, Clone, Copy, Default)]
pub struct TableConfig {
    /// Reach of unchanged-value suppression for this table (§1).
    pub suppression: SuppressionConfig,
    /// How long this table's data is kept, by event time (§8).
    ///
    /// `None` — the default — keeps it forever. Retention deletes, so it is
    /// never inferred: an archive nobody configured a horizon for must not start
    /// dropping partitions because a duration looked plausible.
    ///
    /// Resolution can never be finer than the partition interval, since whole
    /// directories are dropped (§8, §11). Choose the two together.
    pub retention: Option<Micros>,
}

/// Maps table names to their schema and configuration (§6).
#[derive(Debug)]
pub struct TableCatalogue {
    log: Arc<dyn SchemaLog>,
    tables: DashMap<String, TableSchema>,
    configured: DashMap<String, TableConfig>,
    default_config: TableConfig,
}

impl TableCatalogue {
    /// Creates an empty catalogue recording through `log`.
    ///
    /// `default_config` applies to any table no configuration names, so a table
    /// appearing for the first time under load has a defined reach rather than
    /// none.
    pub fn new(log: Arc<dyn SchemaLog>, default_config: TableConfig) -> Self {
        Self {
            log,
            tables: DashMap::new(),
            configured: DashMap::new(),
            default_config,
        }
    }

    /// Admits a batch schema for `table`, evolving the table if §6 allows.
    ///
    /// Returns the storage schema — the table's logical schema plus the system
    /// columns — which is what the memtable and the stamper expect. Refuses a
    /// type change or a reserved column without touching the table, and records
    /// an accepted change before publishing it.
    pub fn accept(&self, table: &str, incoming: &Schema) -> Result<SchemaRef, CatalogueError> {
        // The whole read-merge-record-publish runs under the entry's lock, so two
        // writers meeting on a new table cannot split it across two schemas or
        // record it twice.
        let mut entry = self.tables.entry(table.to_owned());

        let merged = match &entry {
            dashmap::mapref::entry::Entry::Occupied(held) => held.get().merge(incoming)?,
            dashmap::mapref::entry::Entry::Vacant(_) => TableSchema::new(incoming)?,
        };

        let unchanged = matches!(&entry, dashmap::mapref::entry::Entry::Occupied(held)
            if held.get().logical() == merged.logical());

        if !unchanged {
            // Durable before visible: on failure nothing is published, so a
            // restart cannot lose a schema batches were accepted against.
            self.log.append(&SchemaRecord {
                table: table.to_owned(),
                schema: Arc::clone(merged.logical()),
            })?;
        }

        let storage = merged.storage();
        match &mut entry {
            dashmap::mapref::entry::Entry::Occupied(held) => {
                held.insert(merged);
            }
            dashmap::mapref::entry::Entry::Vacant(_) => {
                // Vacant cannot be consumed through a shared borrow, so the
                // insert goes through the map directly. Still under the shard's
                // lock, which the entry guard holds.
                drop(entry);
                self.tables.insert(table.to_owned(), merged);
            }
        }

        Ok(storage)
    }

    /// The table's schema, if it exists.
    pub fn schema(&self, table: &str) -> Option<TableSchema> {
        self.tables.get(table).map(|entry| entry.value().clone())
    }

    /// The table's configuration, falling back to the instance default.
    pub fn config(&self, table: &str) -> TableConfig {
        self.configured
            .get(table)
            .map(|entry| *entry.value())
            .unwrap_or(self.default_config)
    }

    /// Sets a table's configuration.
    ///
    /// May be called before or after the table first appears, since it comes from
    /// instance configuration read at startup.
    pub fn configure(&self, table: &str, config: TableConfig) {
        self.configured.insert(table.to_owned(), config);
    }

    /// Every table the catalogue knows, in name order.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        names.sort_unstable();
        names
    }

    /// Restores schemas read back from the catalogue log.
    ///
    /// Records are applied in order, so the last one for a table wins — that is
    /// what makes the newest record its current schema (§6). Does not write to
    /// the log: this *is* the log being read.
    pub fn replay(
        &self,
        records: impl IntoIterator<Item = SchemaRecord>,
    ) -> Result<(), CatalogueError> {
        for record in records {
            self.tables
                .insert(record.table, TableSchema::new(record.schema.as_ref())?);
        }
        Ok(())
    }
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
    use std::sync::Mutex;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::clock::Micros;
    use crate::schema::{
        METRIC_ID_COLUMN, SEQ_COLUMN, SYSTEM_TIME_COLUMN, TIMESTAMP_COLUMN, timestamp_type,
    };
    use crate::suppression::SuppressionConfig;

    /// Recording [`SchemaLog`] that can be told to fail.
    #[derive(Debug, Default)]
    struct RecordingLog {
        appended: Mutex<Vec<SchemaRecord>>,
        fail: Mutex<bool>,
    }

    impl RecordingLog {
        fn appended(&self) -> Vec<SchemaRecord> {
            self.appended.lock().unwrap().clone()
        }

        fn set_failing(&self, failing: bool) {
            *self.fail.lock().unwrap() = failing;
        }
    }

    impl SchemaLog for RecordingLog {
        fn append(&self, record: &SchemaRecord) -> Result<(), CatalogueError> {
            if *self.fail.lock().unwrap() {
                return Err(CatalogueError::Persistence("log is failing".into()));
            }
            self.appended.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn fresh() -> (Arc<RecordingLog>, TableCatalogue) {
        let log = Arc::new(RecordingLog::default());
        (
            Arc::clone(&log),
            TableCatalogue::new(log, TableConfig::default()),
        )
    }

    /// A client batch's schema: identity columns plus whatever payload it sends.
    fn client(payload: &[(&str, DataType, bool)]) -> Schema {
        let mut fields = vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        ];
        for (name, data_type, nullable) in payload {
            fields.push(Field::new(*name, data_type.clone(), *nullable));
        }
        Schema::new(fields)
    }

    fn value_only() -> Schema {
        client(&[("value", DataType::Float64, true)])
    }

    /// §6: multiple named tables, each with its own schema.
    #[test]
    fn a_new_table_is_created_on_first_use() {
        let (log, catalogue) = fresh();
        let storage = catalogue.accept("readings", &value_only()).unwrap();

        assert!(storage.field_with_name("value").is_ok());
        assert!(
            storage.field_with_name(SYSTEM_TIME_COLUMN).is_ok(),
            "the catalogue must hand back the storage schema, system columns included"
        );
        assert_eq!(log.appended().len(), 1, "a new table must be recorded");
        assert_eq!(catalogue.table_names(), vec!["readings".to_owned()]);
    }

    #[test]
    fn distinct_tables_keep_distinct_schemas() {
        let (_log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();
        catalogue
            .accept("events", &client(&[("kind", DataType::Utf8, true)]))
            .unwrap();

        assert!(
            catalogue
                .schema("events")
                .unwrap()
                .logical()
                .field_with_name("value")
                .is_err(),
            "one table's column must not appear in another"
        );
    }

    /// §6: an unchanged schema is not a change, so it must not reach the log —
    /// otherwise every batch would append a record.
    #[test]
    fn an_unchanged_schema_is_not_recorded_again() {
        let (log, catalogue) = fresh();
        for _ in 0..5 {
            catalogue.accept("readings", &value_only()).unwrap();
        }

        assert_eq!(log.appended().len(), 1);
    }

    /// §6: adding a nullable column is accepted, and the evolution is recorded.
    #[test]
    fn a_new_nullable_column_evolves_the_table() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();

        let storage = catalogue
            .accept(
                "readings",
                &client(&[
                    ("value", DataType::Float64, true),
                    ("quality", DataType::Int32, true),
                ]),
            )
            .unwrap();

        assert!(storage.field_with_name("quality").is_ok());
        assert_eq!(log.appended().len(), 2, "the evolution must be recorded");
    }

    /// §6: a missing column reads as null, so a batch that omits one is accepted
    /// and the column stays in the table.
    #[test]
    fn a_batch_omitting_a_column_is_accepted_and_keeps_it() {
        let (log, catalogue) = fresh();
        catalogue
            .accept(
                "readings",
                &client(&[
                    ("value", DataType::Float64, true),
                    ("quality", DataType::Int32, true),
                ]),
            )
            .unwrap();

        let storage = catalogue.accept("readings", &value_only()).unwrap();

        assert!(
            storage.field_with_name("quality").is_ok(),
            "an omitted column must stay in the table, not be dropped"
        );
        assert_eq!(log.appended().len(), 1, "omitting a column is not a change");
    }

    /// §6: a type change on an existing column rejects the batch.
    #[test]
    fn a_type_change_is_rejected() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();

        let error = catalogue
            .accept("readings", &client(&[("value", DataType::Int64, true)]))
            .expect_err("must be rejected");

        assert!(matches!(
            error,
            CatalogueError::Schema(SchemaError::WrongType { .. })
        ));
        assert_eq!(log.appended().len(), 1, "a rejection must not be recorded");
        assert_eq!(
            catalogue
                .schema("readings")
                .unwrap()
                .logical()
                .field_with_name("value")
                .unwrap()
                .data_type(),
            &DataType::Float64,
            "a rejected batch must not have altered the table"
        );
    }

    /// §6: the reserved system columns are the server's. A client supplying one
    /// is rejected before anything is recorded.
    #[test]
    fn a_batch_supplying_a_reserved_column_is_rejected() {
        let (log, catalogue) = fresh();
        let intrusive = client(&[(SEQ_COLUMN, DataType::UInt64, true)]);

        assert!(catalogue.accept("readings", &intrusive).is_err());
        assert!(log.appended().is_empty());
    }

    /// §6, §10: durable before visible. If the log fails, the evolution must not
    /// be published — otherwise a restart would lose a schema the server had
    /// already accepted batches against.
    #[test]
    fn a_failing_log_does_not_publish_the_evolution() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();
        log.set_failing(true);

        let wider = client(&[
            ("value", DataType::Float64, true),
            ("quality", DataType::Int32, true),
        ]);
        assert!(matches!(
            catalogue.accept("readings", &wider).expect_err("must fail"),
            CatalogueError::Persistence(_)
        ));
        assert!(
            catalogue
                .schema("readings")
                .unwrap()
                .logical()
                .field_with_name("quality")
                .is_err(),
            "the column must not be visible after a failed append"
        );

        // And once the log recovers, the same batch evolves the table.
        log.set_failing(false);
        assert!(catalogue.accept("readings", &wider).is_ok());
    }

    #[test]
    fn a_failing_log_does_not_create_the_table() {
        let (log, catalogue) = fresh();
        log.set_failing(true);

        assert!(catalogue.accept("readings", &value_only()).is_err());
        assert_eq!(catalogue.schema("readings"), None);
        assert!(catalogue.table_names().is_empty());
    }

    /// §4, §6: a restart rebuilds the catalogue from the log, and the last record
    /// for a table is its current schema.
    #[test]
    fn replay_restores_the_newest_schema_per_table() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();
        catalogue
            .accept(
                "readings",
                &client(&[
                    ("value", DataType::Float64, true),
                    ("quality", DataType::Int32, true),
                ]),
            )
            .unwrap();
        catalogue
            .accept("events", &client(&[("kind", DataType::Utf8, true)]))
            .unwrap();

        let (_fresh_log, restarted) = fresh();
        restarted.replay(log.appended()).unwrap();

        let readings = restarted.schema("readings").expect("readings");
        assert!(readings.logical().field_with_name("quality").is_ok());
        assert!(restarted.schema("events").is_some());
        assert_eq!(restarted.table_names().len(), 2);
    }

    #[test]
    fn replay_does_not_write_back_to_the_log() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();
        let recorded = log.appended();

        let (fresh_log, restarted) = fresh();
        restarted.replay(recorded).unwrap();

        assert!(fresh_log.appended().is_empty(), "replay must not re-log");
    }

    /// §6: this is why the schema is durable at all. After a restart the
    /// type-change rejection has to still work — without a recorded schema the
    /// server would accept the conflicting batch and the clash would surface much
    /// later as Parquet unreadable across files.
    #[test]
    fn a_type_change_is_still_rejected_after_a_restart() {
        let (log, catalogue) = fresh();
        catalogue.accept("readings", &value_only()).unwrap();

        let (_fresh, restarted) = fresh();
        restarted.replay(log.appended()).unwrap();

        assert!(
            restarted
                .accept("readings", &client(&[("value", DataType::Int64, true)]))
                .is_err(),
            "the recorded schema must still refuse a conflicting type"
        );
    }

    /// §1, §6: the per-table configuration is an operator's choice and does not
    /// come from the log. A table configured for recomputation keeps its window
    /// while another keeps the stream default.
    #[test]
    fn per_table_configuration_is_independent() {
        const HOUR: Micros = 3_600_000_000;

        let log = Arc::new(RecordingLog::default());
        let catalogue = TableCatalogue::new(
            Arc::clone(&log) as Arc<dyn SchemaLog>,
            TableConfig::default(),
        );
        catalogue.configure(
            "forecast",
            TableConfig {
                suppression: SuppressionConfig::for_recomputed(HOUR),
                ..TableConfig::default()
            },
        );

        catalogue.accept("forecast", &value_only()).unwrap();
        catalogue.accept("readings", &value_only()).unwrap();

        assert_eq!(catalogue.config("forecast").suppression.window, HOUR);
        assert_eq!(
            catalogue.config("readings").suppression.window,
            SuppressionConfig::for_stream().window,
            "an unconfigured table must fall back to the instance default"
        );
    }

    /// §8: retention is configured per table as a duration, and unset means keep
    /// forever. An archive without an explicit policy must never start deleting.
    #[test]
    fn retention_is_per_table_and_unset_means_forever() {
        const NINETY_DAYS: Micros = 90 * 86_400_000_000;

        let (_log, catalogue) = fresh();
        catalogue.configure(
            "readings",
            TableConfig {
                retention: Some(NINETY_DAYS),
                ..TableConfig::default()
            },
        );

        assert_eq!(catalogue.config("readings").retention, Some(NINETY_DAYS));
        assert_eq!(
            catalogue.config("audit").retention,
            None,
            "an unconfigured table must keep everything"
        );
        assert_eq!(
            TableConfig::default().retention,
            None,
            "the default must not delete anything"
        );
    }

    /// Configuration may be set before or after the table first appears, since it
    /// comes from instance configuration read at startup.
    #[test]
    fn configuration_applies_to_an_existing_table() {
        const HOUR: Micros = 3_600_000_000;

        let (_log, catalogue) = fresh();
        catalogue.accept("forecast", &value_only()).unwrap();
        catalogue.configure(
            "forecast",
            TableConfig {
                suppression: SuppressionConfig::for_recomputed(HOUR),
                ..TableConfig::default()
            },
        );

        assert_eq!(catalogue.config("forecast").suppression.window, HOUR);
    }

    #[test]
    fn an_unknown_table_has_no_schema() {
        let (_log, catalogue) = fresh();
        assert_eq!(catalogue.schema("absent"), None);
    }

    /// The ingestion path is concurrent (§7), so two writers meeting on a new
    /// table must not split it into two schemas or record it twice.
    #[test]
    fn concurrent_first_use_of_one_table_records_it_once() {
        use std::thread;

        let (log, catalogue) = fresh();
        let catalogue = Arc::new(catalogue);

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let catalogue = Arc::clone(&catalogue);
                thread::spawn(move || catalogue.accept("readings", &value_only()).unwrap())
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker panicked");
        }

        assert_eq!(log.appended().len(), 1, "the table was recorded twice");
    }
}
