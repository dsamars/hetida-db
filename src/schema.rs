// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Table schemas, reserved columns and schema evolution.
//!
//! A table's *logical* schema is what clients send: the identity columns plus
//! payload attributes. Its *storage* schema is that plus the system columns the
//! server owns (§1). Clients never see or supply the latter.
//!
//! Evolution follows §6: nullable additions are accepted, omitted columns read
//! as null, and a type change is refused rather than silently widened.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// Valid time — when the measurement was true (§1).
pub const TIMESTAMP_COLUMN: &str = "timestamp";
/// Series identity, a `u64` surrogate from the registry (§10).
pub const METRIC_ID_COLUMN: &str = "metric_id";
/// System time — when the row was written; the observation axis (§1).
pub const SYSTEM_TIME_COLUMN: &str = "system_time";
/// Total order and version tiebreaker (§1).
pub const SEQ_COLUMN: &str = "seq";

/// The Arrow type of both timestamp columns.
///
/// Microseconds match [`crate::clock::Micros`]; no timezone, because both are
/// absolute instants and a zone would only invite ambiguity.
pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

/// Returns true if `name` is a column the server owns (§1).
pub fn is_reserved(name: &str) -> bool {
    name == SYSTEM_TIME_COLUMN || name == SEQ_COLUMN
}

/// Appends the system columns to `schema`, yielding a storage schema (§1).
///
/// Single source of truth for the storage layout: both [`TableSchema::storage`]
/// and the stamper go through here, so the declared schema and the batches
/// actually produced cannot drift apart.
pub fn append_system_fields(schema: &Schema) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(SYSTEM_TIME_COLUMN, timestamp_type(), false));
    fields.push(Field::new(SEQ_COLUMN, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// Why a schema was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchemaError {
    /// A client supplied a column the server owns (§1).
    #[error("column `{0}` is reserved for the server")]
    ReservedColumn(String),
    /// A required identity column is absent.
    #[error("required column `{0}` is missing")]
    MissingColumn(String),
    /// A column exists but not with the type the table holds.
    #[error("column `{column}` is {found}, expected {expected}")]
    WrongType {
        /// Offending column.
        column: String,
        /// Type the table holds.
        expected: DataType,
        /// Type the incoming schema declared.
        found: DataType,
    },
    /// An identity column was declared nullable.
    #[error("identity column `{0}` may not be nullable")]
    NullableIdentityColumn(String),
    /// A new column was declared non-nullable.
    #[error("new column `{0}` must be nullable")]
    NewColumnNotNullable(String),
}

/// A validated table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    logical: SchemaRef,
}

impl TableSchema {
    /// Validates `schema` as a table's logical schema.
    ///
    /// Rejects reserved columns, absent identity columns, wrong identity types
    /// and nullable identity columns.
    pub fn new(schema: &Schema) -> Result<Self, SchemaError> {
        for field in schema.fields() {
            if is_reserved(field.name()) {
                return Err(SchemaError::ReservedColumn(field.name().clone()));
            }
        }

        Self::check_identity(schema, TIMESTAMP_COLUMN, &timestamp_type())?;
        Self::check_identity(schema, METRIC_ID_COLUMN, &DataType::UInt64)?;

        Ok(Self {
            logical: Arc::new(schema.clone()),
        })
    }

    /// Verifies one identity column is present, correctly typed and not null.
    fn check_identity(schema: &Schema, name: &str, expected: &DataType) -> Result<(), SchemaError> {
        let field = schema
            .field_with_name(name)
            .map_err(|_| SchemaError::MissingColumn(name.to_owned()))?;

        if field.data_type() != expected {
            return Err(SchemaError::WrongType {
                column: name.to_owned(),
                expected: expected.clone(),
                found: field.data_type().clone(),
            });
        }
        if field.is_nullable() {
            return Err(SchemaError::NullableIdentityColumn(name.to_owned()));
        }
        Ok(())
    }

    /// The schema clients send.
    pub fn logical(&self) -> &SchemaRef {
        &self.logical
    }

    /// The logical schema plus the system columns, in storage order (§1).
    ///
    /// The system columns trail the payload so a client batch can be extended
    /// by appending arrays, without reordering what it sent.
    pub fn storage(&self) -> SchemaRef {
        append_system_fields(&self.logical)
    }

    /// Merges an incoming batch schema into this one, per §6.
    ///
    /// Returns the evolved table schema, or refuses the batch. Merging is
    /// idempotent: a schema that adds nothing yields an equal table.
    pub fn merge(&self, incoming: &Schema) -> Result<Self, SchemaError> {
        let mut fields: Vec<Field> = self
            .logical
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();

        for field in incoming.fields() {
            if is_reserved(field.name()) {
                return Err(SchemaError::ReservedColumn(field.name().clone()));
            }

            match self.logical.field_with_name(field.name()) {
                Ok(existing) => {
                    if existing.data_type() != field.data_type() {
                        return Err(SchemaError::WrongType {
                            column: field.name().clone(),
                            expected: existing.data_type().clone(),
                            found: field.data_type().clone(),
                        });
                    }
                }
                Err(_) => {
                    if !field.is_nullable() {
                        return Err(SchemaError::NewColumnNotNullable(field.name().clone()));
                    }
                    fields.push(field.as_ref().clone());
                }
            }
        }

        Ok(Self {
            logical: Arc::new(Schema::new(fields)),
        })
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
    use arrow::datatypes::{DataType, Field, Schema};

    fn identity_fields() -> Vec<Field> {
        vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        ]
    }

    fn minimal() -> Schema {
        Schema::new(identity_fields())
    }

    fn with_payload(name: &str, data_type: DataType, nullable: bool) -> Schema {
        let mut fields = identity_fields();
        fields.push(Field::new(name, data_type, nullable));
        Schema::new(fields)
    }

    #[test]
    fn a_minimal_schema_is_accepted() {
        assert!(TableSchema::new(&minimal()).is_ok());
    }

    /// §6: the server owns `system_time` and `seq`; a client supplying either
    /// would be dictating its own version identity.
    #[test]
    fn supplying_a_reserved_column_is_rejected() {
        for reserved in [SYSTEM_TIME_COLUMN, SEQ_COLUMN] {
            let schema = with_payload(reserved, DataType::UInt64, false);
            let error = TableSchema::new(&schema).expect_err("reserved column must be rejected");
            assert!(matches!(error, SchemaError::ReservedColumn(name) if name == reserved));
        }
    }

    #[test]
    fn a_schema_without_the_identity_columns_is_rejected() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        let error = TableSchema::new(&schema).expect_err("identity columns are required");
        assert!(matches!(error, SchemaError::MissingColumn(_)));
    }

    /// §10: `metric_id` is a `u64` surrogate. A signed or narrower column would
    /// silently change the sort key width.
    #[test]
    fn the_wrong_identity_column_type_is_rejected() {
        let schema = Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::Int64, false),
        ]);
        let error = TableSchema::new(&schema).expect_err("metric_id must be UInt64");
        assert!(matches!(error, SchemaError::WrongType { .. }));
    }

    /// Identity may not be null: a null `metric_id` belongs to no logical row.
    #[test]
    fn a_nullable_identity_column_is_rejected() {
        let schema = Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), true),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        ]);
        let error = TableSchema::new(&schema).expect_err("identity may not be nullable");
        assert!(matches!(error, SchemaError::NullableIdentityColumn(_)));
    }

    /// §1: storage carries the system columns the client never sends.
    #[test]
    fn the_storage_schema_appends_the_system_columns() {
        let table = TableSchema::new(&minimal()).unwrap();
        let storage = table.storage();

        let names: Vec<&str> = storage.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                TIMESTAMP_COLUMN,
                METRIC_ID_COLUMN,
                SYSTEM_TIME_COLUMN,
                SEQ_COLUMN
            ]
        );

        let system_time = storage.field_with_name(SYSTEM_TIME_COLUMN).unwrap();
        assert_eq!(system_time.data_type(), &timestamp_type());
        assert!(!system_time.is_nullable());

        let seq = storage.field_with_name(SEQ_COLUMN).unwrap();
        assert_eq!(seq.data_type(), &DataType::UInt64);
        assert!(!seq.is_nullable());
    }

    /// §6: adding a nullable column is accepted and widens the table.
    #[test]
    fn evolution_accepts_a_new_nullable_column() {
        let table = TableSchema::new(&minimal()).unwrap();
        let evolved = table
            .merge(&with_payload("value", DataType::Float64, true))
            .expect("a nullable addition is allowed");

        assert!(evolved.logical().field_with_name("value").is_ok());
    }

    /// A non-nullable addition cannot be honoured: rows already written have no
    /// value for it, and back-filling would rewrite history.
    #[test]
    fn evolution_rejects_a_new_non_nullable_column() {
        let table = TableSchema::new(&minimal()).unwrap();
        let error = table
            .merge(&with_payload("value", DataType::Float64, false))
            .expect_err("a non-nullable addition must be rejected");
        assert!(matches!(error, SchemaError::NewColumnNotNullable(_)));
    }

    /// §6: a column missing from the batch reads as null, it does not shrink
    /// the table.
    #[test]
    fn evolution_tolerates_a_missing_column() {
        let table = TableSchema::new(&with_payload("value", DataType::Float64, true)).unwrap();
        let evolved = table
            .merge(&minimal())
            .expect("omitting a column is allowed");

        assert!(evolved.logical().field_with_name("value").is_ok());
    }

    /// §6: a type change is rejected, because existing files cannot be reread
    /// under the new type.
    #[test]
    fn evolution_rejects_a_type_change() {
        let table = TableSchema::new(&with_payload("value", DataType::Float64, true)).unwrap();
        let error = table
            .merge(&with_payload("value", DataType::Utf8, true))
            .expect_err("a type change must be rejected");
        assert!(matches!(error, SchemaError::WrongType { .. }));
    }

    #[test]
    fn evolution_rejects_a_reserved_column() {
        let table = TableSchema::new(&minimal()).unwrap();
        let error = table
            .merge(&with_payload(SEQ_COLUMN, DataType::UInt64, true))
            .expect_err("reserved columns stay reserved on evolution");
        assert!(matches!(error, SchemaError::ReservedColumn(_)));
    }

    #[test]
    fn evolution_is_idempotent() {
        let table = TableSchema::new(&with_payload("value", DataType::Float64, true)).unwrap();
        let once = table.merge(table.logical()).unwrap();
        assert_eq!(once.logical(), table.logical());
    }
}
