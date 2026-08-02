// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Scanning one table across memory and Parquet (§1, §2, §5, §11).
//!
//! [`ArchiveTable`] is the DataFusion `TableProvider` behind every table. It
//! answers with the raw versions — `system_time` and `seq` as ordinary columns —
//! from **both** places a row can be:
//!
//! - the partitions' active memtables and the frozen ones a flush has in flight
//!   ([`ActiveMemTable`](crate::active::ActiveMemTable)), and
//! - the Parquet files under `data/<table>/…` (§11).
//!
//! Missing either side gives a wrong answer rather than a slow one, which is why
//! frozen tables are held until their file exists.
//!
//! # The output is sorted, and stays sorted
//!
//! Every input is individually in `(metric_id, timestamp, seq)` order: a file
//! because the writer refuses anything else (§1), a memtable because
//! [`MemTable::sorted_batch`](crate::memtable::MemTable::sorted_batch) produces it. The plan therefore ends in a
//! *sort-preserving merge* rather than a sort — which is what lets as-of
//! resolution be a streaming merge instead of a full sort (§1).
//!
//! The ordering is declared on both sources, so DataFusion can see it. If it
//! were not, the optimiser would insert a `SortExec` over the whole archive and
//! the property §1 rests on would be paid for on every query.
//!
//! # Pruning happens three times
//!
//! 1. **Whole partitions**, from the directory name. A partition covers exactly
//!    `[start, end)` of valid time (§11), so a predicate bounding `timestamp`
//!    rules directories out before a single footer is read. This is the cheap
//!    one, and the reason the layout is calendar-aligned.
//! 2. **Row groups and pages**, from the statistics the writer puts on every
//!    column — on `metric_id` because the sort is metric-major, on `system_time`
//!    so an old observation point skips rows written after it (§1, §11).
//! 3. **Columns**, by projection. The three key columns are always read, because
//!    the merge orders by them, and only those.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DataFusionResult, Statistics};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{empty::EmptyExec, expressions::col};

use crate::clock::Micros;
use crate::compact::partition_dirs;
use crate::partition::PartitionInterval;
use crate::schema::{METRIC_ID_COLUMN, SEQ_COLUMN, TIMESTAMP_COLUMN};
use crate::storage::Storage;
use crate::table::PartitionedTable;

/// The URL the archive's own store is registered under.
///
/// Opaque: it names the [`Storage`] adapter, never a filesystem location, so no
/// path outside the archive can be reached through a query.
pub const STORE_URL: &str = "hetida://archive/";

/// The three columns every file and every memtable is ordered by (§1).
pub const KEY_COLUMNS: [&str; 3] = [METRIC_ID_COLUMN, TIMESTAMP_COLUMN, SEQ_COLUMN];

/// One table's raw versions, across memory and Parquet.
#[derive(Debug)]
pub struct ArchiveTable {
    schema: SchemaRef,
    table: String,
    interval: PartitionInterval,
    storage: Arc<dyn Storage>,
    state: Option<Arc<PartitionedTable>>,
}

impl ArchiveTable {
    /// Scans `table`, whose storage schema is `schema`.
    ///
    /// `state` is the table's in-memory side, absent for a table whose rows are
    /// all in Parquet.
    pub fn new(
        table: impl Into<String>,
        schema: SchemaRef,
        interval: PartitionInterval,
        storage: Arc<dyn Storage>,
        state: Option<Arc<PartitionedTable>>,
    ) -> Self {
        Self {
            schema,
            table: table.into(),
            interval,
            storage,
            state,
        }
    }

    /// One sorted batch per memtable, active and frozen alike.
    ///
    /// Each becomes its own DataFusion partition, so the merge above can stream
    /// them rather than concatenating and re-sorting.
    fn memory_partitions(&self) -> DataFusionResult<Vec<Vec<arrow::array::RecordBatch>>> {
        let Some(state) = &self.state else {
            return Ok(Vec::new());
        };

        let mut partitions = Vec::new();
        for start in state.partition_starts() {
            let tables = state
                .partition_table(start)
                .map(|active| {
                    let mut held = active.frozen();
                    held.push(active.current());
                    held
                })
                .unwrap_or_default();

            for table in tables {
                if let Some(batch) = table
                    .sorted_batch()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
                {
                    partitions.push(vec![batch]);
                }
            }
        }
        Ok(partitions)
    }

    /// The files to scan, with partitions ruled out by `bounds` left unopened.
    ///
    /// The first of the three prunings — see the module documentation. A
    /// directory whose name this layout would not have written is skipped
    /// rather than guessed at, exactly as retention skips it (§8).
    fn files(&self, bounds: TimeBounds) -> DataFusionResult<Vec<PartitionedFile>> {
        let directories = partition_dirs(self.storage.as_ref(), &self.table, self.interval)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let mut files = Vec::new();
        for directory in directories {
            let Some(start) = self.interval.start_of_path(&directory) else {
                continue;
            };
            let end = self
                .interval
                .end(start)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if !bounds.overlaps(start, end) {
                continue;
            }

            for entry in self
                .storage
                .list(&directory)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
            {
                let Some(size) = self
                    .storage
                    .size(&entry)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
                else {
                    continue;
                };
                files.push(PartitionedFile::new(
                    entry.to_string_lossy().into_owned(),
                    size,
                ));
            }
        }

        files.sort_by(|left, right| left.object_meta.location.cmp(&right.object_meta.location));
        Ok(files)
    }

    /// Column indices to read: what was asked for, plus the three keys.
    ///
    /// The keys are not optional — the merge orders by them — so a projection
    /// that omits them is widened here and narrowed again by a projection above
    /// the merge.
    fn scan_projection(&self, requested: Option<&Vec<usize>>) -> Option<Vec<usize>> {
        let requested = requested?;
        let mut indices: Vec<usize> = requested.clone();
        for name in KEY_COLUMNS {
            if let Ok(index) = self.schema.index_of(name)
                && !indices.contains(&index)
            {
                indices.push(index);
            }
        }
        indices.sort_unstable();
        Some(indices)
    }
}

#[async_trait]
impl TableProvider for ArchiveTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Every filter is taken inexactly: it prunes here and is still applied
    /// above, so a predicate this provider understands only approximately —
    /// a partition overlapping the range, a row group whose statistics do not
    /// rule it out — can never change the answer.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let scanned = self.scan_projection(projection);
        let scanned_schema = match &scanned {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        // Two forms of the same order. A source validates and projects its sort
        // information against the *unprojected* schema it was given, where the
        // merge above works on what actually comes out.
        let source_ordering = key_ordering(&self.schema)?;
        let ordering = key_ordering(&scanned_schema)?;

        let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        let memory = self.memory_partitions()?;
        if !memory.is_empty() {
            let source =
                MemorySourceConfig::try_new(&memory, Arc::clone(&self.schema), scanned.clone())?
                    .try_with_sort_information(vec![source_ordering.clone()])?;
            inputs.push(DataSourceExec::from_data_source(source));
        }

        let files = self.files(TimeBounds::of(filters))?;
        if !files.is_empty() {
            let dfschema = datafusion::common::DFSchema::try_from(Arc::clone(&self.schema))?;
            let predicate = session
                .create_physical_expr(conjunction(filters), &dfschema)
                .ok();
            let mut source = ParquetSource::new(Arc::clone(&self.schema))
                // The page index is what turns the writer's page-level
                // statistics into skipped pages rather than skipped groups.
                .with_enable_page_index(true)
                // Rows that survive group and page pruning are still filtered
                // inside the reader, so they never become Arrow arrays.
                .with_pushdown_filters(true);
            if let Some(predicate) = predicate {
                source = source.with_predicate(predicate);
            }

            let config = FileScanConfigBuilder::new(store_url()?, Arc::new(source))
                // One group per file: each is sorted on its own, and the merge
                // above interleaves them without a sort.
                .with_file_groups(files.into_iter().map(|file| vec![file].into()).collect())
                .with_projection_indices(scanned.clone())?
                .with_output_ordering(vec![source_ordering.clone()])
                .with_preserve_order(true)
                .with_limit(limit)
                .build();
            inputs.push(DataSourceExec::from_data_source(config));
        }

        let combined: Arc<dyn ExecutionPlan> = match inputs.len() {
            0 => {
                return Ok(Arc::new(EmptyExec::new(projected(
                    &self.schema,
                    projection,
                )?)));
            }
            1 => inputs.swap_remove(0),
            _ => UnionExec::try_new(inputs)?,
        };
        let merged: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, combined));

        // Narrow back to what was asked for, now that the merge has used the
        // key columns it needed.
        let Some(requested) = projection else {
            return Ok(merged);
        };
        let wanted = self.schema.project(requested)?;
        if wanted.fields() == merged.schema().fields() {
            return Ok(merged);
        }

        let expressions = wanted
            .fields()
            .iter()
            .map(|field| Ok((col(field.name(), &merged.schema())?, field.name().clone())))
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(expressions, merged)?))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

/// The projected schema a scan answers with.
fn projected(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> DataFusionResult<SchemaRef> {
    Ok(match projection {
        Some(indices) => Arc::new(schema.project(indices)?),
        None => Arc::clone(schema),
    })
}

/// `(metric_id, timestamp, seq)` as a physical ordering over `schema` (§1).
fn key_ordering(schema: &SchemaRef) -> DataFusionResult<LexOrdering> {
    let exprs = KEY_COLUMNS
        .into_iter()
        .map(|name| Ok(PhysicalSortExpr::new_default(col(name, schema)?)))
        .collect::<DataFusionResult<Vec<_>>>()?;
    LexOrdering::new(exprs).ok_or_else(|| {
        DataFusionError::Internal("the sort key must have at least one column".to_owned())
    })
}

/// The archive's own object store URL.
fn store_url() -> DataFusionResult<ObjectStoreUrl> {
    ObjectStoreUrl::parse(STORE_URL)
}

/// `filters` combined with AND, or `true` if there are none.
fn conjunction(filters: &[Expr]) -> Expr {
    filters
        .iter()
        .cloned()
        .reduce(Expr::and)
        .unwrap_or_else(|| datafusion::prelude::lit(true))
}

/// What a predicate says about the valid time a scan can reach (§11).
///
/// Only closed comparisons of `timestamp` against a literal are read; anything
/// else widens the bounds rather than narrowing them, so an unrecognised
/// predicate costs a wider scan and never a wrong answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeBounds {
    /// Earliest valid time the query can reach, inclusive.
    pub low: Micros,
    /// Latest valid time the query can reach, inclusive.
    pub high: Micros,
}

impl Default for TimeBounds {
    fn default() -> Self {
        Self {
            low: Micros::MIN,
            high: Micros::MAX,
        }
    }
}

impl TimeBounds {
    /// The tightest bounds `filters` imply.
    pub fn of(filters: &[Expr]) -> Self {
        let mut bounds = Self::default();
        for filter in filters {
            bounds.narrow(filter);
        }
        bounds
    }

    /// Whether a partition covering `[start, end)` can hold a matching row.
    pub fn overlaps(&self, start: Micros, end: Micros) -> bool {
        start <= self.high && end > self.low
    }

    /// Tightens the bounds by one predicate, ignoring what it cannot read.
    fn narrow(&mut self, filter: &Expr) {
        use datafusion::logical_expr::Operator;

        match filter {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.narrow(&binary.left);
                self.narrow(&binary.right);
            }
            Expr::BinaryExpr(binary) => {
                let Some((operator, value)) = timestamp_comparison(binary) else {
                    return;
                };
                // Strict comparisons exclude the bound itself. Treating `<` as
                // `<=` would keep a partition that begins exactly where the
                // predicate ends — the commonest case there is, since a client
                // asks for a month by naming the next month's first instant.
                match operator {
                    Operator::Eq => {
                        self.low = self.low.max(value);
                        self.high = self.high.min(value);
                    }
                    Operator::Gt => self.low = self.low.max(value.saturating_add(1)),
                    Operator::GtEq => self.low = self.low.max(value),
                    Operator::Lt => self.high = self.high.min(value.saturating_sub(1)),
                    Operator::LtEq => self.high = self.high.min(value),
                    _ => {}
                }
            }
            Expr::Between(between) if !between.negated && names_timestamp(&between.expr) => {
                if let Some(low) = literal_micros(&between.low) {
                    self.low = self.low.max(low);
                }
                if let Some(high) = literal_micros(&between.high) {
                    self.high = self.high.min(high);
                }
            }
            _ => {}
        }
    }
}

/// A comparison of `timestamp` against a literal, oriented so the column is on
/// the left.
fn timestamp_comparison(
    binary: &datafusion::logical_expr::BinaryExpr,
) -> Option<(datafusion::logical_expr::Operator, Micros)> {
    use datafusion::logical_expr::Operator;

    if names_timestamp(&binary.left) {
        return literal_micros(&binary.right).map(|value| (binary.op, value));
    }
    if names_timestamp(&binary.right) {
        // `1000 < timestamp` bounds the same side as `timestamp > 1000`.
        let flipped = match binary.op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        };
        return literal_micros(&binary.left).map(|value| (flipped, value));
    }
    None
}

/// Whether an expression is the `timestamp` column.
fn names_timestamp(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(column) if column.name == TIMESTAMP_COLUMN)
}

/// A literal read as microseconds of valid time, if it is one.
fn literal_micros(expr: &Expr) -> Option<Micros> {
    use datafusion::scalar::ScalarValue;

    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::TimestampMicrosecond(Some(micros), _) => Some(*micros),
        ScalarValue::TimestampMillisecond(Some(millis), _) => millis.checked_mul(1_000),
        ScalarValue::TimestampSecond(Some(seconds), _) => seconds.checked_mul(1_000_000),
        ScalarValue::TimestampNanosecond(Some(nanos), _) => Some(nanos / 1_000),
        ScalarValue::Int64(Some(value)) => Some(*value),
        _ => None,
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

    use datafusion::prelude::{col as col_expr, lit};
    use datafusion::scalar::ScalarValue;

    fn stamp(micros: Micros) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(Some(micros), None))
    }

    #[test]
    fn no_predicate_reaches_every_partition() {
        let bounds = TimeBounds::of(&[]);
        assert!(bounds.overlaps(Micros::MIN, Micros::MAX));
        assert!(bounds.overlaps(0, 1));
    }

    /// §11: this is the cheap pruning — a partition covers exactly `[start,
    /// end)`, so a bounded predicate rules directories out before a footer is
    /// read.
    #[test]
    fn a_bounded_predicate_rules_out_partitions_outside_it() {
        let bounds = TimeBounds::of(&[
            col_expr(TIMESTAMP_COLUMN).gt_eq(stamp(1_000)),
            col_expr(TIMESTAMP_COLUMN).lt(stamp(2_000)),
        ]);

        assert!(!bounds.overlaps(0, 1_000), "ends before the range begins");
        assert!(bounds.overlaps(0, 1_001), "reaches into it");
        assert!(bounds.overlaps(1_500, 1_600));
        assert!(bounds.overlaps(1_999, 3_000));
        assert!(!bounds.overlaps(2_001, 3_000), "begins after it ends");
        assert!(
            !bounds.overlaps(2_000, 3_000),
            "a partition beginning exactly where `< 2000` ends holds nothing"
        );
    }

    #[test]
    fn a_between_predicate_bounds_both_sides() {
        let bounds = TimeBounds::of(&[col_expr(TIMESTAMP_COLUMN).between(stamp(10), stamp(20))]);
        assert_eq!(bounds, TimeBounds { low: 10, high: 20 });
    }

    #[test]
    fn a_conjunction_narrows_from_both_halves() {
        let bounds = TimeBounds::of(&[col_expr(TIMESTAMP_COLUMN)
            .gt_eq(stamp(10))
            .and(col_expr(TIMESTAMP_COLUMN).lt_eq(stamp(20)))]);
        assert_eq!(bounds, TimeBounds { low: 10, high: 20 });
    }

    /// The column may be on either side of the comparison, and flipping it must
    /// bound the same end — otherwise `1000 < timestamp` would prune the wrong
    /// partitions.
    #[test]
    fn the_column_may_sit_on_either_side() {
        let written = TimeBounds::of(&[col_expr(TIMESTAMP_COLUMN).gt(stamp(1_000))]);
        let flipped = TimeBounds::of(&[stamp(1_000).lt(col_expr(TIMESTAMP_COLUMN))]);
        assert_eq!(written, flipped);
        assert_eq!(written.low, 1_001, "`>` excludes its own bound");
    }

    /// An unreadable predicate must widen, never narrow: a wrong reading here
    /// skips a partition holding matching rows, which is a wrong answer rather
    /// than a slow one.
    #[test]
    fn an_unreadable_predicate_prunes_nothing() {
        for filter in [
            col_expr("value").gt(lit(3.0)),
            col_expr(TIMESTAMP_COLUMN).gt(col_expr("value")),
            col_expr(TIMESTAMP_COLUMN).is_null(),
            col_expr(TIMESTAMP_COLUMN).not_between(stamp(10), stamp(20)),
        ] {
            assert_eq!(
                TimeBounds::of(std::slice::from_ref(&filter)),
                TimeBounds::default(),
                "{filter} narrowed the bounds"
            );
        }
    }

    /// A predicate on another column must not be read as one on `timestamp`.
    #[test]
    fn a_predicate_on_system_time_does_not_bound_valid_time() {
        let bounds = TimeBounds::of(&[col_expr(crate::schema::SYSTEM_TIME_COLUMN).lt(stamp(10))]);
        assert_eq!(bounds, TimeBounds::default());
    }
}
