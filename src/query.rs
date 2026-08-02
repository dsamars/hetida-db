// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The query engine: as-of resolution over the archive (§1, §5, §10).
//!
//! Every query names an optional **observation point**. A [`QueryEngine`] builds
//! a DataFusion session for it and registers, for each table:
//!
//! | Name | What it holds |
//! |---|---|
//! | `<table>` | one row per `(metric_id, timestamp)` — the version current at the observation point |
//! | `"<table>$history"` | every version, with `system_time` and `seq` as ordinary columns |
//! | `"<table>$raw"` | the same, before the observation point is applied — internal |
//!
//! plus [`SERIES_TABLE`] and [`LABELS_TABLE`], through which labels are reachable
//! from SQL rather than only from the registration action (§10).
//!
//! `$` is not an identifier character in SQL, so a client writes
//! `SELECT … FROM "readings$history"` with the quotes.
//!
//! # Resolution is a streaming merge, not a sort
//!
//! §1 settles what a query must answer: of the versions of a logical row whose
//! `system_time` is at or before the observation point, the one with the highest
//! `seq`. Stated in SQL that is a grouped `last_value(… ORDER BY seq)`, and the
//! naive plan for it sorts the whole archive.
//!
//! It does not have to. [`ArchiveTable`] hands DataFusion a stream already in
//! `(metric_id, timestamp, seq)` order and says so, and the group key is a prefix
//! of that order — so the aggregate runs in one pass over the merged stream, with
//! no sort and nothing buffered beyond the group in hand. A test asserts the
//! plan contains no `SortExec`, because the difference is invisible in the
//! answer and total in the cost.
//!
//! # The observation point also bounds `$history`
//!
//! Seeing versions written after the point one is observing from would contradict
//! the point. `$history` is therefore filtered too; it differs from the resolved
//! view in keeping every version *up to* the point, not in reaching past it.
//!
//! # A fresh session per query
//!
//! The observation point is baked into the registered views, so a session belongs
//! to one query. Registering a handful of providers costs nothing next to opening
//! a Parquet footer, and the alternative — a session-wide variable read by a
//! user-defined function — would put the archive's correctness inside an
//! expression a client's SQL could shadow.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable as DataFusionMemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::archive::Archive;
use crate::clock::Micros;
use crate::labels::{LabelPushdown, has_label_udf};
use crate::object_store::StorageObjectStore;
use crate::scan::{ArchiveTable, STORE_URL};
use crate::schema::{METRIC_ID_COLUMN, SEQ_COLUMN, SYSTEM_TIME_COLUMN, TIMESTAMP_COLUMN};
use crate::storage::Storage;

/// Suffix of the view carrying every version (§1, §5).
pub const HISTORY_SUFFIX: &str = "$history";
/// Suffix of the unfiltered provider the two views are built on.
pub const RAW_SUFFIX: &str = "$raw";
/// One row per registered series (§10).
pub const SERIES_TABLE: &str = "hetida_series";
/// One row per label of a registered series (§10).
pub const LABELS_TABLE: &str = "hetida_labels";

/// Why a query could not be planned or run.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// DataFusion refused the SQL, or the scan failed.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    /// The archive holds no table by that name (§6).
    #[error("no table named `{0}`")]
    UnknownTable(String),
}

/// Plans and runs SQL against one archive at an observation point (§1, §5).
#[derive(Debug)]
pub struct QueryEngine {
    archive: Arc<Archive>,
    storage: Arc<dyn Storage>,
}

impl QueryEngine {
    /// Queries `archive`, reading its Parquet through `storage`.
    pub fn new(archive: Arc<Archive>, storage: Arc<dyn Storage>) -> Self {
        Self { archive, storage }
    }

    /// A session with every table registered, resolved at `as_of`.
    ///
    /// `None` observes now — the current state, every version included (§1).
    pub async fn session(&self, as_of: Option<Micros>) -> Result<SessionContext, QueryError> {
        // §10: the label marker is resolved against the registry before the
        // plan is optimised, so what reaches the scan is a filter on
        // `metric_id` that row-group statistics can prune.
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(streaming_config())
            .with_default_features()
            .with_analyzer_rule(Arc::new(LabelPushdown::new(Arc::clone(
                self.archive.registry(),
            ))))
            .build();
        let context = SessionContext::new_with_state(state);
        context.register_udf(has_label_udf());
        context.runtime_env().register_object_store(
            ObjectStoreUrl::parse(STORE_URL)?.as_ref(),
            Arc::new(StorageObjectStore::new(Arc::clone(&self.storage))),
        );

        for table in self.archive.catalogue().table_names() {
            self.register_table(&context, &table, as_of).await?;
        }
        self.register_registry(&context)?;

        Ok(context)
    }

    /// Runs `sql` at `as_of` and streams the result (§5).
    ///
    /// Streamed rather than collected: `do_get` writes batches to the client as
    /// they are produced, and a query over an archive does not fit in memory
    /// merely because someone asked for it.
    pub async fn stream(
        &self,
        sql: &str,
        as_of: Option<Micros>,
    ) -> Result<SendableRecordBatchStream, QueryError> {
        let context = self.session(as_of).await?;
        Ok(context.sql(sql).await?.execute_stream().await?)
    }

    /// Runs `sql` at `as_of` and collects the result.
    ///
    /// For callers that need the whole answer — `get_schema`, and tests.
    pub async fn collect(
        &self,
        sql: &str,
        as_of: Option<Micros>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let context = self.session(as_of).await?;
        Ok(context.sql(sql).await?.collect().await?)
    }

    /// The schema `sql` would answer with, without running it (§5).
    pub async fn schema_of(
        &self,
        sql: &str,
        as_of: Option<Micros>,
    ) -> Result<SchemaRef, QueryError> {
        let context = self.session(as_of).await?;
        let frame = context.sql(sql).await?;
        Ok(Arc::new(frame.schema().as_arrow().clone()))
    }

    /// The physical plan for `sql`, for tests and `EXPLAIN`-shaped checks.
    pub async fn plan_text(&self, sql: &str, as_of: Option<Micros>) -> Result<String, QueryError> {
        let context = self.session(as_of).await?;
        let plan = context.sql(sql).await?.create_physical_plan().await?;
        Ok(datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string())
    }

    /// Registers `table`'s raw provider and its two views.
    async fn register_table(
        &self,
        context: &SessionContext,
        table: &str,
        as_of: Option<Micros>,
    ) -> Result<(), QueryError> {
        let schema = self
            .archive
            .catalogue()
            .schema(table)
            .ok_or_else(|| QueryError::UnknownTable(table.to_owned()))?;

        context.register_table(
            quoted(&format!("{table}{RAW_SUFFIX}")),
            Arc::new(ArchiveTable::new(
                table,
                schema.storage(),
                self.archive.interval(),
                Arc::clone(&self.storage),
                self.archive.table(table),
            )),
        )?;

        // The observation point is applied once, here, and both views inherit it.
        register_view(
            context,
            &format!("{table}{HISTORY_SUFFIX}"),
            &history_sql(table, as_of),
        )
        .await?;
        register_view(
            context,
            table,
            &resolved_sql(table, schema.logical().as_ref()),
        )
        .await?;

        Ok(())
    }

    /// Registers the registry as two catalog tables (§10).
    ///
    /// Two, because one cannot answer both questions: a series with no labels
    /// has no row in [`LABELS_TABLE`], and a label predicate against
    /// [`SERIES_TABLE`] would be string matching on a canonical form.
    fn register_registry(&self, context: &SessionContext) -> Result<(), QueryError> {
        let (series, labels) = registry_batches(&self.archive);

        context.register_table(
            SERIES_TABLE,
            Arc::new(DataFusionMemTable::try_new(
                series.schema(),
                vec![vec![series]],
            )?),
        )?;
        context.register_table(
            LABELS_TABLE,
            Arc::new(DataFusionMemTable::try_new(
                labels.schema(),
                vec![vec![labels]],
            )?),
        )?;
        Ok(())
    }
}

/// Session settings that keep as-of resolution a streaming merge (§1).
///
/// Both switches turn off repartitioning *above* the merge, and for the same
/// reason: the merge's output is one globally ordered stream, and that order is
/// what lets the aggregate resolve versions in one pass. Splitting it across
/// workers discards the order and then pays a `SortExec` per worker to rebuild
/// enough of it — the full sort §1 exists to avoid, arrived at sideways.
///
/// Nothing is given up. The parallelism lives *below* the merge, which polls
/// every file and every memtable concurrently; only the pass over the already
/// ordered result is serial, which by definition it must be.
fn streaming_config() -> SessionConfig {
    SessionConfig::new()
        .with_round_robin_repartition(false)
        .with_repartition_aggregations(false)
}

/// Registers `sql` as a view named `name`.
async fn register_view(context: &SessionContext, name: &str, sql: &str) -> Result<(), QueryError> {
    let frame = context.sql(sql).await?;
    context.register_table(
        quoted(name),
        Arc::new(datafusion::datasource::ViewTable::new(
            frame.into_unoptimized_plan(),
            None,
        )),
    )?;
    Ok(())
}

/// A SQL identifier, quoted so `$` and any user column name survive parsing.
fn quoted(name: &str) -> datafusion::sql::TableReference {
    datafusion::sql::TableReference::bare(name.to_owned())
}

/// An identifier as it appears inside generated SQL.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A microsecond instant as a SQL timestamp literal.
fn timestamp_literal(micros: Micros) -> String {
    format!("arrow_cast({micros}, 'Timestamp(Microsecond, None)')")
}

/// `<table>$history`: every version up to the observation point.
fn history_sql(table: &str, as_of: Option<Micros>) -> String {
    let raw = ident(&format!("{table}{RAW_SUFFIX}"));
    match as_of {
        None => format!("SELECT * FROM {raw}"),
        Some(point) => format!(
            "SELECT * FROM {raw} WHERE {} <= {}",
            ident(SYSTEM_TIME_COLUMN),
            timestamp_literal(point),
        ),
    }
}

/// `<table>`: the version current at the observation point, per logical row.
///
/// Grouped by `(metric_id, timestamp)`, which is a prefix of the input ordering
/// — that is what makes this a streaming merge rather than a sort (§1). The
/// system columns are the resolution's input, not its output, so they do not
/// appear: `$history` is where they are ordinary columns (§5).
fn resolved_sql(table: &str, logical: &Schema) -> String {
    let seq = ident(SEQ_COLUMN);
    let payload: Vec<String> = logical
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| name != TIMESTAMP_COLUMN && name != METRIC_ID_COLUMN)
        .map(|name| {
            let column = ident(&name);
            format!("last_value({column} ORDER BY {seq}) AS {column}")
        })
        .collect();

    let mut selected = vec![ident(TIMESTAMP_COLUMN), ident(METRIC_ID_COLUMN)];
    selected.extend(payload);

    format!(
        "SELECT {} FROM {} GROUP BY {}, {}",
        selected.join(", "),
        ident(&format!("{table}{HISTORY_SUFFIX}")),
        ident(METRIC_ID_COLUMN),
        ident(TIMESTAMP_COLUMN),
    )
}

/// The registry as one batch of series and one of labels (§10).
fn registry_batches(archive: &Archive) -> (RecordBatch, RecordBatch) {
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut canonical = Vec::new();

    let mut label_ids = Vec::new();
    let mut label_names = Vec::new();
    let mut label_values = Vec::new();

    for (metric_id, key) in archive.registry().all() {
        ids.push(metric_id);
        names.push(key.name().to_owned());
        canonical.push(
            key.labels()
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join(","),
        );

        for (name, value) in key.labels() {
            label_ids.push(metric_id);
            label_names.push(name.clone());
            label_values.push(value.clone());
        }
    }

    let series_schema = Arc::new(Schema::new(vec![
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("labels", DataType::Utf8, false),
    ]));
    let labels_schema = Arc::new(Schema::new(vec![
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // `try_new` only fails on a length mismatch, which the loop above cannot
    // produce; an empty batch is the honest fallback and never a panic.
    let series = RecordBatch::try_new(
        Arc::clone(&series_schema),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(canonical)),
        ],
    )
    .unwrap_or_else(|_| RecordBatch::new_empty(series_schema));
    let labels = RecordBatch::try_new(
        Arc::clone(&labels_schema),
        vec![
            Arc::new(UInt64Array::from(label_ids)),
            Arc::new(StringArray::from(label_names)),
            Arc::new(StringArray::from(label_values)),
        ],
    )
    .unwrap_or_else(|_| RecordBatch::new_empty(labels_schema));

    (series, labels)
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

    use arrow::array::{Float64Array, TimestampMicrosecondArray};
    use arrow::datatypes::Field;

    use crate::archive::{Archive, ArchiveConfig};
    use crate::clock::TestClock;
    use crate::flush::Flusher;
    use crate::parquet::ParquetConfig;
    use crate::registry::SeriesKey;
    use crate::schema::timestamp_type;
    use crate::storage::InMemoryStorage;
    use crate::wal::WalConfig;

    /// 2026-08-02T14:37:12Z.
    const AUGUST: Micros = 1_785_681_432_000_000;
    const HOUR: Micros = 3_600_000_000;

    struct Fixture {
        engine: QueryEngine,
        archive: Arc<Archive>,
        storage: InMemoryStorage,
        clock: Arc<TestClock>,
    }

    fn fixture() -> Fixture {
        let storage = InMemoryStorage::new();
        let clock = Arc::new(TestClock::new(AUGUST));
        let archive = Arc::new(
            Archive::open(
                Arc::new(storage.clone()),
                &ArchiveConfig {
                    wal: WalConfig {
                        segment_bytes: 64 * 1024,
                        max_commit_delay: Duration::ZERO,
                    },
                    ..ArchiveConfig::default()
                },
                Arc::clone(&clock) as Arc<_>,
            )
            .expect("archive"),
        );
        Fixture {
            engine: QueryEngine::new(Arc::clone(&archive), Arc::new(storage.clone())),
            archive,
            storage,
            clock,
        }
    }

    fn client_batch(rows: &[(u64, Micros, f64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
                Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
                Field::new("value", DataType::Float64, true),
            ])),
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
        .expect("batch")
    }

    /// Every `(metric_id, timestamp, value)` a query answered with, sorted.
    async fn rows(fixture: &Fixture, sql: &str, as_of: Option<Micros>) -> Vec<(u64, Micros, f64)> {
        let batches = fixture.engine.collect(sql, as_of).await.expect("query");
        let mut found = Vec::new();
        for batch in batches {
            let ids = batch
                .column_by_name(METRIC_ID_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .expect("metric_id");
            let stamps = batch
                .column_by_name(TIMESTAMP_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .expect("timestamp");
            let values = batch
                .column_by_name("value")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .expect("value");
            for row in 0..batch.num_rows() {
                found.push((ids.value(row), stamps.value(row), values.value(row)));
            }
        }
        found.sort_by(|a, b| a.partial_cmp(b).expect("total order"));
        found
    }

    /// Flushes every partition of `table` to Parquet.
    fn flush_all(fixture: &Fixture, table: &str) {
        let state = fixture.archive.table(table).expect("table");
        let flusher = Flusher::new(
            Arc::new(fixture.storage.clone()),
            table,
            fixture.archive.interval(),
            ParquetConfig::default(),
        );
        for start in state.partition_starts() {
            flusher.flush(&state, start).expect("flush");
        }
    }

    // ---- the pipeline across memory and disk (§5) ----

    /// §5: rows in memory are queryable before they ever reach Parquet.
    #[tokio::test]
    async fn a_query_sees_rows_still_in_memory() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();

        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", None).await,
            vec![(1, AUGUST, 1.5)]
        );
    }

    /// §5: and the same rows once they are on disk and gone from memory —
    /// combining the two is the whole point of the pipeline.
    #[tokio::test]
    async fn a_query_spans_memory_and_parquet() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();
        flush_all(&fixture, "readings");
        assert_eq!(
            fixture.archive.table("readings").unwrap().num_rows(),
            0,
            "the first row must now be on disk only"
        );

        fixture
            .archive
            .ingest("readings", &client_batch(&[(2, AUGUST + HOUR, 2.5)]))
            .unwrap();

        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", None).await,
            vec![(1, AUGUST, 1.5), (2, AUGUST + HOUR, 2.5)],
            "a query missing either side is wrong, not slow"
        );
    }

    /// §2, §5: the window between a freeze and its file. The rows are in
    /// neither the active table nor on disk, and a query must still see them.
    #[tokio::test]
    async fn a_query_sees_rows_a_flush_has_in_flight() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();

        let state = fixture.archive.table("readings").unwrap();
        let start = fixture.archive.interval().start(AUGUST).unwrap();
        state.freeze(start).unwrap().expect("frozen");

        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", None).await,
            vec![(1, AUGUST, 1.5)]
        );
    }

    // ---- as-of resolution (§1) ----

    /// §1: the archive is bitemporal. A correction is a new version, and each
    /// observation point answers with the version current at it — before the
    /// first write, between the two, and after.
    #[tokio::test]
    async fn each_observation_point_answers_with_the_version_current_at_it() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();
        fixture.clock.advance(HOUR);
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 9.9)]))
            .unwrap();

        let sql = "SELECT * FROM readings";
        assert!(
            rows(&fixture, sql, Some(AUGUST - 1)).await.is_empty(),
            "before the first write the archive knew nothing"
        );
        assert_eq!(
            rows(&fixture, sql, Some(AUGUST)).await,
            vec![(1, AUGUST, 1.5)],
            "between the two the first value stood"
        );
        assert_eq!(
            rows(&fixture, sql, Some(AUGUST + HOUR)).await,
            vec![(1, AUGUST, 9.9)],
            "after the correction it is the correction"
        );
        assert_eq!(
            rows(&fixture, sql, None).await,
            vec![(1, AUGUST, 9.9)],
            "no observation point means now"
        );
    }

    /// §1: the same, with the original version already compacted into Parquet
    /// and the correction still in memory — resolution spans the two.
    #[tokio::test]
    async fn a_correction_resolves_against_a_version_already_on_disk() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();
        flush_all(&fixture, "readings");

        fixture.clock.advance(HOUR);
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 9.9)]))
            .unwrap();

        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", Some(AUGUST)).await,
            vec![(1, AUGUST, 1.5)],
            "the flushed version is still what that point saw"
        );
        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", None).await,
            vec![(1, AUGUST, 9.9)]
        );
    }

    /// §1, §5: `$history` keeps every version, with the system columns as
    /// ordinary ones — that is what distinguishes it from the resolved view.
    #[tokio::test]
    async fn history_keeps_every_version_and_exposes_the_system_columns() {
        let fixture = fixture();
        for value in [1.5, 2.5, 3.5] {
            fixture
                .archive
                .ingest("readings", &client_batch(&[(1, AUGUST, value)]))
                .unwrap();
            fixture.clock.advance(HOUR);
        }

        let history = rows(&fixture, "SELECT * FROM \"readings$history\"", None).await;
        assert_eq!(history.len(), 3, "every version must be there");

        let schema = fixture
            .engine
            .schema_of("SELECT * FROM \"readings$history\"", None)
            .await
            .unwrap();
        assert!(schema.field_with_name(SYSTEM_TIME_COLUMN).is_ok());
        assert!(schema.field_with_name(SEQ_COLUMN).is_ok());

        // And the resolved view hides them, answering one row.
        let resolved = fixture
            .engine
            .schema_of("SELECT * FROM readings", None)
            .await
            .unwrap();
        assert!(resolved.field_with_name(SYSTEM_TIME_COLUMN).is_err());
        assert_eq!(
            rows(&fixture, "SELECT * FROM readings", None).await.len(),
            1
        );
    }

    /// §1: an observation point bounds `$history` too. Seeing versions written
    /// after the point one observes from would contradict the point.
    #[tokio::test]
    async fn history_stops_at_the_observation_point() {
        let fixture = fixture();
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 1.5)]))
            .unwrap();
        fixture.clock.advance(HOUR);
        fixture
            .archive
            .ingest("readings", &client_batch(&[(1, AUGUST, 9.9)]))
            .unwrap();

        assert_eq!(
            rows(&fixture, "SELECT * FROM \"readings$history\"", Some(AUGUST)).await,
            vec![(1, AUGUST, 1.5)]
        );
    }

    /// §1: this is the decision the plan has to honour. Resolution is a
    /// streaming merge over sorted inputs; a `SortExec` over the archive would
    /// give the same answer at a cost that grows with the whole table.
    #[tokio::test]
    async fn resolution_never_sorts_the_archive() {
        let fixture = fixture();
        fixture
            .archive
            .ingest(
                "readings",
                &client_batch(&[(1, AUGUST, 1.5), (2, AUGUST + HOUR, 2.5)]),
            )
            .unwrap();
        flush_all(&fixture, "readings");
        fixture
            .archive
            .ingest("readings", &client_batch(&[(3, AUGUST + 2 * HOUR, 3.5)]))
            .unwrap();

        let plan = fixture
            .engine
            .plan_text("SELECT * FROM readings", None)
            .await
            .unwrap();

        assert!(
            !plan.contains("SortExec"),
            "resolution degraded into a full sort:\n{plan}"
        );
        assert!(
            plan.contains("SortPreservingMergeExec"),
            "the merge is what keeps it streaming:\n{plan}"
        );
        assert!(
            plan.contains("ordering_mode=Sorted"),
            "the aggregate must resolve in one pass over the merged stream:\n{plan}"
        );
    }

    // ---- the registry as a catalog (§10) ----

    /// §10: labels are reachable from SQL, not only through the registration
    /// action.
    #[tokio::test]
    async fn the_registry_is_queryable() {
        let fixture = fixture();
        let temp = fixture
            .archive
            .registry()
            .register(SeriesKey::new(
                "temp",
                [("plant".to_owned(), "a".to_owned())],
            ))
            .unwrap();
        fixture
            .archive
            .registry()
            .register(SeriesKey::new("flow", std::iter::empty()))
            .unwrap();

        let batches = fixture
            .engine
            .collect(
                &format!(
                    "SELECT {METRIC_ID_COLUMN} FROM {LABELS_TABLE} \
                          WHERE label = 'plant' AND value = 'a'"
                ),
                None,
            )
            .await
            .unwrap();
        let ids: Vec<u64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("ids")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![temp]);

        // A series with no labels has no row there, which is why the series
        // table exists beside it.
        let series = fixture
            .engine
            .collect(&format!("SELECT * FROM {SERIES_TABLE}"), None)
            .await
            .unwrap();
        assert_eq!(
            series.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2,
            "both series must be listed, labelled or not"
        );
    }

    // ---- pruning and projection (§10, §11) ----

    /// Registers `count` series, each with a `plant` label, and writes one row
    /// per series into Parquet.
    async fn many_metrics(fixture: &Fixture, count: u64) {
        for index in 0..count {
            let id = fixture
                .archive
                .registry()
                .register(SeriesKey::new(
                    format!("metric-{index}"),
                    [("plant".to_owned(), format!("p{index}"))],
                ))
                .unwrap();
            fixture
                .archive
                .ingest("readings", &client_batch(&[(id, AUGUST, index as f64)]))
                .unwrap();
        }
        flush_all(fixture, "readings");
    }

    /// §10, §11: a label predicate becomes literals on `metric_id`, which is
    /// what row-group statistics prune on. A join would reach the scan as a
    /// join and read everything.
    #[tokio::test]
    async fn a_label_predicate_reaches_the_scan_as_a_filter_on_metric_id() {
        let fixture = fixture();
        many_metrics(&fixture, 8).await;

        let plan = fixture
            .engine
            .plan_text(
                "SELECT * FROM readings WHERE hetida_label('plant', 'p3')",
                None,
            )
            .await
            .unwrap();

        assert!(
            plan.contains(METRIC_ID_COLUMN) && !plan.contains("hetida_label"),
            "the marker must be resolved away, not carried into the plan:\n{plan}"
        );
        assert_eq!(
            rows(
                &fixture,
                "SELECT * FROM readings WHERE hetida_label('plant', 'p3')",
                None
            )
            .await
            .len(),
            1,
            "and it must still answer the right rows"
        );
    }

    /// §10: a label nothing carries answers nothing. `true` would silently
    /// widen the question.
    #[tokio::test]
    async fn an_unknown_label_answers_nothing() {
        let fixture = fixture();
        many_metrics(&fixture, 4).await;

        assert!(
            rows(
                &fixture,
                "SELECT * FROM readings WHERE hetida_label('plant', 'nowhere')",
                None
            )
            .await
            .is_empty()
        );
    }

    /// §11: pruning is measured by bytes scanned, not by wall time. A query
    /// over one partition must not read the other one's file at all.
    #[tokio::test]
    async fn a_bounded_predicate_leaves_the_other_partition_unopened() {
        const SEPTEMBER: Micros = 1_788_220_800_000_000;

        let fixture = fixture();
        fixture
            .archive
            .ingest(
                "readings",
                &client_batch(&[(1, AUGUST, 1.5), (2, SEPTEMBER, 2.5)]),
            )
            .unwrap();
        flush_all(&fixture, "readings");

        let plan = fixture
            .engine
            .plan_text(
                &format!(
                    "SELECT * FROM readings WHERE \"timestamp\" < \
                     arrow_cast({SEPTEMBER}, 'Timestamp(Microsecond, None)')"
                ),
                None,
            )
            .await
            .unwrap();

        assert!(plan.contains("month=08"), "august must be scanned:\n{plan}");
        assert!(
            !plan.contains("month=09"),
            "september is entirely past the predicate and must not be opened:\n{plan}"
        );
    }

    /// §11: a projection must reach the Parquet reader, so a query for one
    /// column does not pay for the others. Measured in bytes, deterministically.
    #[tokio::test]
    async fn a_narrow_projection_reads_fewer_bytes() {
        let fixture = fixture();
        many_metrics(&fixture, 64).await;

        fixture.storage.reset_bytes_read();
        fixture
            .engine
            .collect("SELECT * FROM \"readings$history\"", None)
            .await
            .unwrap();
        let whole = fixture.storage.bytes_read();

        fixture.storage.reset_bytes_read();
        fixture
            .engine
            .collect(
                &format!("SELECT {METRIC_ID_COLUMN} FROM \"readings$history\""),
                None,
            )
            .await
            .unwrap();
        let narrow = fixture.storage.bytes_read();

        assert!(
            narrow < whole,
            "projection did not reach the reader: {narrow} bytes vs {whole}"
        );
    }

    #[tokio::test]
    async fn a_query_against_an_unknown_table_is_an_error() {
        let fixture = fixture();
        assert!(
            fixture
                .engine
                .collect("SELECT * FROM absent", None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_empty_archive_answers_nothing_rather_than_failing() {
        let fixture = fixture();
        fixture
            .archive
            .catalogue()
            .accept(
                "readings",
                &Schema::new(vec![
                    Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
                    Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
                    Field::new("value", DataType::Float64, true),
                ]),
            )
            .unwrap();

        assert!(
            rows(&fixture, "SELECT * FROM readings", None)
                .await
                .is_empty()
        );
    }
}
