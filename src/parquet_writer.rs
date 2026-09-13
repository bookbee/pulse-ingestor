//! Turning a batch of consumed records into Parquet bytes.
//!
//! # The schema is a cross-repo contract
//!
//! The silver worker reads these files. Adding a nullable column is safe;
//! renaming, retyping or removing one is a breaking change for every reader of
//! the staging bucket, and staging objects are overwritten in place rather than
//! versioned — so a schema change applies retroactively to anything re-uploaded
//! afterwards. Treat [`schema`] the way you would a published API.
//!
//! # Payloads are strings, deliberately
//!
//! `payload` and `event_header` land as JSON *text*, not as Parquet structs.
//! The payload has no fixed schema across product teams, and inferring one per
//! batch would produce files whose schema depends on which events happened to
//! be in them — unreadable as a single table. Text keeps every file identical
//! in shape; the silver worker projects what it needs.
//!
//! # Malformed records are written, not dropped
//!
//! A record that failed to parse still becomes a row, flagged by `malformed`
//! with the parser's complaint in `malformed_reason` and its Kafka coordinates
//! intact. Dropping it would make staging quietly disagree with the topic, and
//! the offsets would be the only evidence — by then long since committed. A
//! visible bad row is worth more than a silent gap.
//!
//! # `received_at` may be null
//!
//! The gateway promises UTC RFC3339. If a value does not parse, the row is kept
//! with a null timestamp rather than discarded: the payload is still the
//! valuable part, and a null is a question a downstream query can ask about.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::envelope::{ParsedRecord, RecordBody};

const UTC: &str = "UTC";

/// The staging schema. See the module docs before changing it.
#[must_use]
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // --- envelope ---
        Field::new("event_id", DataType::Utf8, true),
        Field::new("gateway_id", DataType::Utf8, true),
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            true,
        ),
        Field::new("retry_count", DataType::Int32, true),
        Field::new("stream_name", DataType::Utf8, true),
        Field::new("event_header", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
        // --- provenance: which offset produced this row ---
        Field::new("kafka_topic", DataType::Utf8, false),
        Field::new("kafka_partition", DataType::Int32, false),
        Field::new("kafka_offset", DataType::Int64, false),
        Field::new(
            "kafka_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
            true,
        ),
        // --- write-time facts ---
        Field::new(
            "ingested_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            false,
        ),
        Field::new("malformed", DataType::Boolean, false),
        Field::new("malformed_reason", DataType::Utf8, true),
    ]))
}

/// Serialise records to a self-contained Parquet file in memory.
///
/// In memory because the upload needs the whole object anyway: GCS single-shot
/// uploads want a known length, and a batch is bounded by `BATCH_OFFSET_RANGE`
/// rather than by however long the process has been running.
///
/// # Errors
///
/// Propagates arrow/parquet encoding failures, which in practice mean a bug in
/// this module rather than bad input — malformed input is representable.
pub fn write_batch(
    records: &[ParsedRecord],
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<Vec<u8>> {
    let batch = to_record_batch(records, ingested_at)?;

    let props = WriterProperties::builder()
        // zstd(3): staging objects are written once and read a few times, so
        // size on the wire matters more than the last few percent of write
        // speed. Snappy would be the choice if this were hot-path storage.
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

fn to_record_batch(
    records: &[ParsedRecord],
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<RecordBatch> {
    let n = records.len();

    let mut event_id = StringBuilder::with_capacity(n, n * 16);
    let mut gateway_id = StringBuilder::with_capacity(n, n * 8);
    let mut received_at = TimestampMicrosecondBuilder::with_capacity(n);
    let mut retry_count = Int32Builder::with_capacity(n);
    let mut stream_name = StringBuilder::with_capacity(n, n * 16);
    let mut event_header = StringBuilder::with_capacity(n, n * 32);
    let mut payload = StringBuilder::with_capacity(n, n * 64);
    let mut kafka_topic = StringBuilder::with_capacity(n, n * 16);
    let mut kafka_partition = Int32Builder::with_capacity(n);
    let mut kafka_offset = Int64Builder::with_capacity(n);
    let mut kafka_timestamp = TimestampMillisecondBuilder::with_capacity(n);
    let mut ingested = TimestampMicrosecondBuilder::with_capacity(n);
    let mut malformed = BooleanBuilder::with_capacity(n);
    let mut malformed_reason = StringBuilder::with_capacity(n, n * 16);

    let ingested_us = ingested_at.timestamp_micros();

    for r in records {
        kafka_topic.append_value(&r.meta.topic);
        kafka_partition.append_value(r.meta.partition);
        kafka_offset.append_value(offset_to_i64(r.meta.offset));
        kafka_timestamp.append_option(r.meta.timestamp_ms);
        ingested.append_value(ingested_us);

        match &r.body {
            RecordBody::Parsed(e) => {
                malformed.append_value(false);
                malformed_reason.append_null();

                event_id.append_value(&e.event_id);
                gateway_id.append_value(&e.gateway_id);
                received_at.append_option(parse_rfc3339_micros(&e.received_at));
                retry_count.append_value(e.retry_count);
                stream_name.append_option(e.stream_name.as_deref());
                event_header.append_option(e.event_header.as_ref().map(ToString::to_string));
                payload.append_value(e.payload.to_string());
            }
            RecordBody::Malformed { reason } => {
                malformed.append_value(true);
                malformed_reason.append_value(reason);

                event_id.append_null();
                gateway_id.append_null();
                received_at.append_null();
                retry_count.append_null();
                stream_name.append_null();
                event_header.append_null();
                payload.append_null();
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(event_id.finish()),
        Arc::new(gateway_id.finish()),
        Arc::new(received_at.finish().with_timezone(UTC)),
        Arc::new(retry_count.finish()),
        Arc::new(stream_name.finish()),
        Arc::new(event_header.finish()),
        Arc::new(payload.finish()),
        Arc::new(kafka_topic.finish()),
        Arc::new(kafka_partition.finish()),
        Arc::new(kafka_offset.finish()),
        Arc::new(kafka_timestamp.finish().with_timezone(UTC)),
        Arc::new(ingested.finish().with_timezone(UTC)),
        Arc::new(malformed.finish()),
        Arc::new(malformed_reason.finish()),
    ];

    Ok(RecordBatch::try_new(schema(), columns)?)
}

/// Kafka offsets are `i64` on the wire; ours are `u64` because a negative
/// offset is not a thing. Saturate rather than wrap — an offset above
/// `i64::MAX` is unreachable in practice, and a wrapped negative offset would
/// silently corrupt provenance.
fn offset_to_i64(offset: u64) -> i64 {
    i64::try_from(offset).unwrap_or(i64::MAX)
}

fn parse_rfc3339_micros(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Envelope, RecordMeta};
    // `is_null` and friends live on the Array trait, not the concrete arrays.
    use arrow::array::Array;

    fn parsed(offset: u64, event_id: &str) -> ParsedRecord {
        ParsedRecord {
            meta: RecordMeta {
                topic: "ingestion-events".to_owned(),
                partition: 2,
                offset,
                timestamp_ms: Some(1_760_000_000_000),
            },
            body: RecordBody::Parsed(Box::new(Envelope {
                event_id: event_id.to_owned(),
                gateway_id: "gw-1".to_owned(),
                received_at: "2026-09-14T10:00:00Z".to_owned(),
                retry_count: 1,
                stream_name: Some("ingestion-events".to_owned()),
                event_header: Some(serde_json::json!({"sub": "u1"})),
                payload: serde_json::json!({"kind": "click"}),
            })),
        }
    }

    fn malformed(offset: u64) -> ParsedRecord {
        ParsedRecord {
            meta: RecordMeta {
                topic: "ingestion-events".to_owned(),
                partition: 2,
                offset,
                timestamp_ms: None,
            },
            body: RecordBody::Malformed {
                reason: "expected value at line 1".to_owned(),
            },
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-14T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn writes_a_readable_parquet_file() {
        let bytes = write_batch(&[parsed(0, "a"), parsed(1, "b")], now()).unwrap();
        // Parquet's magic number, both ends. Cheap proof we produced a real
        // file rather than a buffer that happens to be non-empty.
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn schema_has_every_contract_column() {
        let s = schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "event_id",
                "gateway_id",
                "received_at",
                "retry_count",
                "stream_name",
                "event_header",
                "payload",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at",
                "malformed",
                "malformed_reason",
            ]
        );
    }

    #[test]
    fn provenance_columns_are_non_nullable() {
        // A row with no traceable origin is worse than no row.
        let s = schema();
        for name in [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "malformed",
        ] {
            let f = s.field_with_name(name).unwrap();
            assert!(!f.is_nullable(), "{name} must be non-nullable");
        }
    }

    #[test]
    fn a_malformed_record_becomes_a_flagged_row_not_a_gap() {
        let batch =
            to_record_batch(&[parsed(0, "a"), malformed(1), parsed(2, "c")], now()).unwrap();
        assert_eq!(batch.num_rows(), 3, "the bad offset must still be a row");

        let flags = batch
            .column_by_name("malformed")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!flags.value(0));
        assert!(flags.value(1));
        assert!(!flags.value(2));

        let offsets = batch
            .column_by_name("kafka_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(offsets.value(1), 1, "provenance survives a parse failure");
    }

    #[test]
    fn payload_and_header_are_json_text() {
        let batch = to_record_batch(&[parsed(0, "a")], now()).unwrap();
        let payload = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(payload.value(0), r#"{"kind":"click"}"#);

        let header = batch
            .column_by_name("event_header")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(header.value(0), r#"{"sub":"u1"}"#);
    }

    #[test]
    fn an_unparseable_received_at_nulls_the_column_and_keeps_the_row() {
        let mut r = parsed(0, "a");
        if let RecordBody::Parsed(e) = &mut r.body {
            e.received_at = "not a timestamp".to_owned();
        }
        let batch = to_record_batch(&[r], now()).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let ts = batch
            .column_by_name("received_at")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert!(ts.is_null(0));

        // ...and the payload is still there, which is the point.
        let payload = batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert!(!payload.is_null(0));
    }

    #[test]
    fn an_empty_batch_is_still_a_valid_file() {
        let bytes = write_batch(&[], now()).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
    }

    #[test]
    fn offsets_above_i64_max_saturate_rather_than_going_negative() {
        assert_eq!(offset_to_i64(u64::MAX), i64::MAX);
        assert_eq!(offset_to_i64(42), 42);
    }
}
