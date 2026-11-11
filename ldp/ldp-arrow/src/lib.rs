//! `ldp-arrow` — the single place where SQL types become Arrow types.
//!
//! Contract reference: `docs/LDP_Phase1_Implementation_Contract.md` §5.
//! Nothing else in the workspace is allowed to hard-code a type mapping, so
//! that adding a driver (TDS, Snowflake, Oracle) is a change here and nowhere
//! else.
//!
//! Phase 1 mapping (deliberate, documented in PLAN.md):
//!
//! | Postgres            | Arrow                          | note                        |
//! |---------------------|--------------------------------|-----------------------------|
//! | smallint (int2)     | Int32                          | lossless widening           |
//! | integer (int4)      | Int32                          |                             |
//! | bigint (int8)       | Int64                          | partition key type          |
//! | real (float4)       | Float64                        | lossless widening           |
//! | double precision    | Float64                        |                             |
//! | text/varchar/bpchar | Utf8                           | UTF-8 only                  |
//! | boolean             | Boolean                        |                             |
//! | timestamp           | Timestamp(Microsecond, None)   | no tz in Phase 1            |
//! | date                | Date32                         |                             |
//! | numeric             | Decimal128(38, scale)          | scale sampled once per run  |
//! | timestamptz         | **rejected**                   | "no timezone in Phase 1"    |

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// PostgreSQL OIDs (pg_type.oid). Hard-coded on purpose: these are stable,
// well-known numbers and looking them up would need a catalog round-trip.
// ---------------------------------------------------------------------------
pub mod pg_oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
    pub const INT4RANGE: u32 = 3904;
}

/// Human-readable Postgres type name, used in error messages that an operator
/// has to act on.
pub fn pg_type_name(oid: u32) -> &'static str {
    match oid {
        pg_oid::BOOL => "boolean",
        pg_oid::BYTEA => "bytea",
        pg_oid::CHAR => "\"char\"",
        pg_oid::INT8 => "bigint",
        pg_oid::INT2 => "smallint",
        pg_oid::INT4 => "integer",
        pg_oid::TEXT => "text",
        pg_oid::OID => "oid",
        pg_oid::JSON | pg_oid::JSONB => "json",
        pg_oid::FLOAT4 => "real",
        pg_oid::FLOAT8 => "double precision",
        pg_oid::BPCHAR => "character",
        pg_oid::VARCHAR => "character varying",
        pg_oid::DATE => "date",
        pg_oid::TIMESTAMP => "timestamp",
        pg_oid::TIMESTAMPTZ => "timestamp with time zone",
        pg_oid::NUMERIC => "numeric",
        pg_oid::UUID => "uuid",
        pg_oid::INT4RANGE => "int4range",
        _ => "unknown",
    }
}

/// Map a Postgres type OID to the Arrow type LDP will carry it as.
///
/// `None` means "not supported in Phase 1" and the planner must turn that into
/// a configuration error **before** any data is read (contract §5: unknown
/// Postgres type → error at planning time).
pub fn arrow_type_for_pg_oid(oid: u32) -> Option<DataType> {
    match oid {
        pg_oid::BOOL => Some(DataType::Boolean),
        pg_oid::INT2 | pg_oid::INT4 => Some(DataType::Int32),
        pg_oid::INT8 => Some(DataType::Int64),
        pg_oid::FLOAT4 | pg_oid::FLOAT8 => Some(DataType::Float64),
        pg_oid::TEXT | pg_oid::VARCHAR | pg_oid::BPCHAR | pg_oid::CHAR => Some(DataType::Utf8),
        pg_oid::DATE => Some(DataType::Date32),
        pg_oid::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        // Decimal needs a precision/scale decision that only the connector can
        // make (it samples the data), so it is not answered here.
        pg_oid::NUMERIC => None,
        _ => None,
    }
}

/// True when the OID needs a decimal precision/scale decision.
pub fn is_decimal_oid(oid: u32) -> bool {
    oid == pg_oid::NUMERIC
}

// ---------------------------------------------------------------------------
// Decimal helpers
// ---------------------------------------------------------------------------

/// Parse a Postgres `numeric` **as text** into a scaled i128.
///
/// Postgres prints numerics as e.g. `123.4500`, `-0.5` — we accept the plain
/// decimal form and reject anything else rather than guess. Returns `None` if
/// the value has more fractional digits than `scale` can hold, so the caller
/// can raise a loud error: contract §5 forbids silent truncation of decimals.
pub fn decimal_from_str(text: &str, precision: u8, scale: i8) -> Option<i128> {
    let s = text.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("null") {
        return None;
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if body.contains(['e', 'E']) {
        return None; // scientific notation: not produced by pg for numerics
    }
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    let int_digits = if int_part.is_empty() { "0" } else { int_part };
    if !int_digits.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let scale_u = scale.max(0) as usize;
    if frac_part.len() > scale_u {
        // Trailing zeros are fine to drop; real truncation is not.
        let (keep, extra) = frac_part.split_at(scale_u);
        if extra.bytes().any(|b| b != b'0') {
            return None;
        }
        return build_scaled(neg, int_digits, keep, precision);
    }
    let mut frac = String::from(frac_part);
    while frac.len() < scale_u {
        frac.push('0');
    }
    build_scaled(neg, int_digits, &frac, precision)
}

fn build_scaled(neg: bool, int_digits: &str, frac: &str, precision: u8) -> Option<i128> {
    let mut digits = String::with_capacity(int_digits.len() + frac.len());
    digits.push_str(int_digits);
    digits.push_str(frac);
    let trimmed = digits.trim_start_matches('0');
    if trimmed.len() > precision as usize {
        return None;
    }
    let mut acc: i128 = 0;
    for b in digits.bytes() {
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as i128)?;
    }
    Some(if neg { -acc } else { acc })
}

/// Render a scaled i128 back to a decimal string (used by the CSV sink).
pub fn format_decimal(value: i128, scale: i8) -> String {
    let scale = scale.max(0) as usize;
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac_part)
}

// ---------------------------------------------------------------------------
// Batch helpers
// ---------------------------------------------------------------------------

/// Approximate in-memory footprint of a batch, used to enforce
/// `settings.max_batch_bytes` (contract DoD #5).
pub fn batch_memory_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum()
}

/// A schema with the given fields, shared behind an `Arc`.
pub fn schema_of(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// An empty batch that still carries the schema — the shape the empty-result
/// path (DoD #4) hands to sinks.
pub fn empty_batch(schema: SchemaRef) -> RecordBatch {
    let cols = schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, cols).expect("empty batch is always valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_contract_types() {
        assert_eq!(arrow_type_for_pg_oid(pg_oid::INT8), Some(DataType::Int64));
        assert_eq!(arrow_type_for_pg_oid(pg_oid::INT4), Some(DataType::Int32));
        assert_eq!(
            arrow_type_for_pg_oid(pg_oid::FLOAT8),
            Some(DataType::Float64)
        );
        assert_eq!(arrow_type_for_pg_oid(pg_oid::TEXT), Some(DataType::Utf8));
        assert_eq!(arrow_type_for_pg_oid(pg_oid::BOOL), Some(DataType::Boolean));
        assert_eq!(arrow_type_for_pg_oid(pg_oid::DATE), Some(DataType::Date32));
        assert_eq!(
            arrow_type_for_pg_oid(pg_oid::TIMESTAMP),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        // Rejected on purpose: Phase 1 has no timezone semantics.
        assert_eq!(arrow_type_for_pg_oid(pg_oid::TIMESTAMPTZ), None);
        assert_eq!(arrow_type_for_pg_oid(pg_oid::JSONB), None);
        assert_eq!(arrow_type_for_pg_oid(999_999), None);
    }

    #[test]
    fn decimals_round_trip() {
        assert_eq!(decimal_from_str("123.45", 38, 2), Some(12345));
        assert_eq!(decimal_from_str("-0.5", 38, 2), Some(-50));
        assert_eq!(decimal_from_str("7", 38, 2), Some(700));
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(-50, 2), "-0.50");
        assert_eq!(format_decimal(700, 2), "7.00");
    }

    #[test]
    fn decimal_truncation_is_refused_not_silent() {
        // Scale 2 cannot hold 0.005 exactly.
        assert_eq!(decimal_from_str("0.005", 38, 2), None);
        // Trailing zeros beyond the scale are harmless.
        assert_eq!(decimal_from_str("0.5000", 38, 2), Some(50));
    }

    #[test]
    fn empty_batch_keeps_schema() {
        let s = schema_of(vec![Field::new("id", DataType::Int64, false)]);
        let b = empty_batch(s.clone());
        assert_eq!(b.num_rows(), 0);
        assert_eq!(b.schema(), s);
    }
}
