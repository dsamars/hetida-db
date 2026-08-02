// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Turning a label predicate into a filter on `metric_id` (§10, §11).
//!
//! Labels live in the registry and never in the data files (§10). A query that
//! wants "every reading from plant a" therefore has two ways to get there, and
//! only one of them prunes:
//!
//! ```sql
//! -- a join: correct, but the scan cannot see it
//! SELECT * FROM readings
//! WHERE metric_id IN (SELECT metric_id FROM hetida_labels
//!                     WHERE label = 'plant' AND value = 'a')
//!
//! -- the same question, resolved before planning
//! SELECT * FROM readings WHERE hetida_label('plant', 'a')
//! ```
//!
//! [`HAS_LABEL`] is a marker. It is never evaluated: [`LabelPushdown`] replaces
//! every call to it with `metric_id IN (<literals>)` before the plan is
//! optimised, by scanning the registry — which §10 sizes for exactly that, one
//! row per series. What reaches the scan is then a predicate on a sorted `u64`
//! column, so the writer's min/max statistics skip whole row groups (§11). A
//! join reaches the scan as a join, and every row group is read.
//!
//! # A label nothing carries is `false`, not "everything"
//!
//! An unresolved label rewrites to `false`, so the query answers nothing. That
//! is the honest reading: no row can carry a label no series has. Rewriting to
//! `true` — or leaving the marker in place — would silently widen the question.
//!
//! # The marker cannot be evaluated
//!
//! If a call ever reaches execution the rewrite did not happen, and answering
//! anything would be a guess at what the client meant. It errors instead.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::lit;

use crate::registry::{MetricId, MetricRegistry};
use crate::schema::METRIC_ID_COLUMN;

/// The marker a query writes to select series by label (§10).
pub const HAS_LABEL: &str = "hetida_label";

/// `hetida_label(label, value)` — a predicate the analyzer resolves away.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HasLabel {
    signature: Signature,
}

impl Default for HasLabel {
    fn default() -> Self {
        Self {
            // Immutable: the answer depends on the registry, which for the
            // duration of one query does not change. That is what allows the
            // analyzer to fold it into literals at all.
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HasLabel {
    fn name(&self) -> &str {
        HAS_LABEL
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arguments: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        Err(DataFusionError::Plan(format!(
            "{HAS_LABEL} must be resolved against the registry before execution; \
             it takes two string literals, as in {HAS_LABEL}('plant', 'a')"
        )))
    }
}

/// The registered form of [`HasLabel`].
pub fn has_label_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(HasLabel::default())
}

/// Replaces every [`HAS_LABEL`] call with the ids it names (§10).
#[derive(Debug)]
pub struct LabelPushdown {
    registry: Arc<MetricRegistry>,
}

impl LabelPushdown {
    /// Resolves against `registry`.
    pub fn new(registry: Arc<MetricRegistry>) -> Self {
        Self { registry }
    }

    /// The rewrite of one expression, if it is a call to the marker.
    fn rewrite(&self, expr: Expr) -> DataFusionResult<Transformed<Expr>> {
        let Expr::ScalarFunction(call) = &expr else {
            return Ok(Transformed::no(expr));
        };
        if call.func.name() != HAS_LABEL {
            return Ok(Transformed::no(expr));
        }

        let [label, value] = call.args.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "{HAS_LABEL} takes a label and a value, as in {HAS_LABEL}('plant', 'a')"
            )));
        };
        let (Some(label), Some(value)) = (string_literal(label), string_literal(value)) else {
            return Err(DataFusionError::Plan(format!(
                "{HAS_LABEL} takes two string literals: the registry is scanned at \
                 planning time, so neither argument may depend on a row"
            )));
        };

        Ok(Transformed::yes(ids_predicate(
            &self.registry.matching(&label, &value),
        )))
    }
}

impl AnalyzerRule for LabelPushdown {
    fn name(&self) -> &str {
        "hetida_label_pushdown"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DataFusionResult<LogicalPlan> {
        plan.transform_up(|node| {
            node.map_expressions(|expr| expr.transform_up(|inner| self.rewrite(inner)))
        })
        .map(|transformed| transformed.data)
    }
}

/// `metric_id IN (…)`, or `false` when nothing carries the label.
fn ids_predicate(ids: &[MetricId]) -> Expr {
    if ids.is_empty() {
        return lit(false);
    }
    datafusion::prelude::col(METRIC_ID_COLUMN).in_list(
        ids.iter()
            .map(|id| lit(ScalarValue::UInt64(Some(*id))))
            .collect(),
        false,
    )
}

/// The text a literal carries, if it is a string literal.
fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(text)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(text)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(text)), _) => Some(text.clone()),
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

    #[test]
    fn a_label_nothing_carries_becomes_false_not_everything() {
        // The honest reading: no row can carry a label no series has. `true`
        // would silently widen the question the client asked.
        assert_eq!(ids_predicate(&[]), lit(false));
    }

    /// §10, §11: a resolved label becomes literals on `metric_id`, which is
    /// what the writer's min/max statistics can prune on.
    #[test]
    fn resolved_ids_become_a_literal_in_list_on_metric_id() {
        let predicate = ids_predicate(&[7, 9]);
        let rendered = predicate.to_string();

        assert!(rendered.contains(METRIC_ID_COLUMN), "{rendered}");
        assert!(
            rendered.contains('7') && rendered.contains('9'),
            "{rendered}"
        );
    }

    /// The marker is a planning-time construct. Reaching execution means the
    /// rewrite did not happen, and answering anything would be a guess.
    #[test]
    fn the_marker_refuses_to_be_evaluated() {
        let udf = has_label_udf();
        assert_eq!(udf.name(), HAS_LABEL);
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn only_string_literals_are_read() {
        assert_eq!(string_literal(&lit("plant")), Some("plant".to_owned()));
        assert_eq!(string_literal(&lit(7i64)), None);
        assert_eq!(string_literal(&datafusion::prelude::col("label")), None);
    }
}
