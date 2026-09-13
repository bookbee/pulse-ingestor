//! The wire envelope, and what a consumed Kafka record becomes in memory.
//!
//! # The envelope is the platform's, not ours
//!
//! `pulse-gateway` defines it (`model.EnrichedPayload`) and
//! `pulse-infra/docs/stack-contract.md` is the authority. Two fields are
//! optional for reasons that are easy to get wrong:
//!
//! - **`event_header` is present only for JWT-authenticated gateway requests.**
//!   API-key requests produce an envelope with no header at all. Treating it as
//!   required would reject every API-key event.
//! - `stream_name` carries the gateway's Redis destination. It is meaningless
//!   on the Kafka path but travels with the envelope, so it is preserved rather
//!   than dropped — staging keeps what it was given.
//!
//! # Payloads stay opaque
//!
//! `payload` and `event_header` are held as raw JSON text and written to
//! Parquet as strings. The payload has no fixed schema — shaping it is the
//! silver worker's job, and staging is a landing pad, not a model. Parsing it
//! here would mean this service needs a migration every time a product team
//! adds a field.
//!
//! # Malformed records do not stop the world
//!
//! A record that is not valid envelope JSON cannot be fixed by retrying, and a
//! consumer that dies on one poison message stops consuming the other 5
//! partitions too. [`ParsedRecord`] therefore models "this offset was
//! unreadable" as a value rather than an error: the offset still counts toward
//! the batch range (so commits stay contiguous) and the failure is counted and
//! logged.

use serde::Deserialize;

/// The JSON envelope as the gateway writes it.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Envelope {
    pub event_id: String,
    pub gateway_id: String,
    /// UTC RFC3339, as a string. Parsed at Parquet-write time, not here — see
    /// [`crate::parquet_writer`].
    pub received_at: String,
    #[serde(default)]
    pub retry_count: i32,
    #[serde(default)]
    pub stream_name: Option<String>,
    /// JWT-authenticated requests only. Kept as raw JSON.
    #[serde(default)]
    pub event_header: Option<serde_json::Value>,
    /// Arbitrary product payload. Kept as raw JSON.
    pub payload: serde_json::Value,
}

/// Where a record came from in Kafka.
///
/// Carried into Parquet so a row in staging can always be traced back to the
/// exact offset that produced it — which is what makes a re-upload auditable
/// rather than merely plausible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMeta {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    /// Broker timestamp in milliseconds, when the broker supplied one.
    pub timestamp_ms: Option<i64>,
}

/// A consumed record: either a parsed envelope, or a note that this offset
/// could not be parsed.
///
/// Both variants carry [`RecordMeta`], because both occupy an offset and an
/// unreadable offset must still advance the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRecord {
    pub meta: RecordMeta,
    pub body: RecordBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordBody {
    Parsed(Box<Envelope>),
    /// The raw bytes were not a valid envelope. Holds the parser's complaint,
    /// truncated — a 4MB unparseable blob should not become a 4MB log line.
    Malformed {
        reason: String,
    },
}

impl ParsedRecord {
    /// Parse a raw Kafka payload into a record.
    ///
    /// Never fails: unparseable input becomes [`RecordBody::Malformed`].
    #[must_use]
    pub fn parse(meta: RecordMeta, payload: Option<&[u8]>) -> Self {
        let body = match payload {
            None => RecordBody::Malformed {
                reason: "record has no payload (tombstone?)".to_owned(),
            },
            Some(bytes) => match serde_json::from_slice::<Envelope>(bytes) {
                Ok(env) => RecordBody::Parsed(Box::new(env)),
                Err(e) => RecordBody::Malformed {
                    reason: truncate(&e.to_string(), 200),
                },
            },
        };
        Self { meta, body }
    }

    #[must_use]
    pub const fn is_malformed(&self) -> bool {
        matches!(self.body, RecordBody::Malformed { .. })
    }

    #[must_use]
    pub const fn envelope(&self) -> Option<&Envelope> {
        match &self.body {
            RecordBody::Parsed(e) => Some(e),
            RecordBody::Malformed { .. } => None,
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut cut = max;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> RecordMeta {
        RecordMeta {
            topic: "ingestion-events".to_owned(),
            partition: 0,
            offset: 7,
            timestamp_ms: Some(1_760_000_000_000),
        }
    }

    const JWT_ENVELOPE: &str = r#"{
        "event_id": "evt-1",
        "gateway_id": "gw-1",
        "received_at": "2026-09-14T10:00:00Z",
        "retry_count": 0,
        "stream_name": "ingestion-events",
        "event_header": {"sub": "user-1"},
        "payload": {"kind": "click", "n": 3}
    }"#;

    // An API-key request produces no event_header at all — not null, absent.
    const API_KEY_ENVELOPE: &str = r#"{
        "event_id": "evt-2",
        "gateway_id": "gw-1",
        "received_at": "2026-09-14T10:00:01Z",
        "retry_count": 0,
        "payload": {"kind": "view"}
    }"#;

    #[test]
    fn parses_a_jwt_envelope_with_its_header() {
        let r = ParsedRecord::parse(meta(), Some(JWT_ENVELOPE.as_bytes()));
        let e = r.envelope().expect("should parse");
        assert_eq!(e.event_id, "evt-1");
        assert!(e.event_header.is_some());
        assert_eq!(e.stream_name.as_deref(), Some("ingestion-events"));
    }

    #[test]
    fn absent_event_header_is_not_an_error() {
        // The API-key path is the majority case; rejecting it would drop most
        // of the traffic on the floor.
        let r = ParsedRecord::parse(meta(), Some(API_KEY_ENVELOPE.as_bytes()));
        let e = r.envelope().expect("should parse");
        assert!(e.event_header.is_none());
        assert!(e.stream_name.is_none());
        assert_eq!(e.retry_count, 0);
    }

    #[test]
    fn payload_is_preserved_verbatim_not_interpreted() {
        let r = ParsedRecord::parse(meta(), Some(JWT_ENVELOPE.as_bytes()));
        let e = r.envelope().unwrap();
        assert_eq!(e.payload["kind"], "click");
        assert_eq!(e.payload["n"], 3);
    }

    #[test]
    fn garbage_becomes_malformed_rather_than_an_error() {
        let r = ParsedRecord::parse(meta(), Some(b"{not json"));
        assert!(r.is_malformed());
        assert!(r.envelope().is_none());
        // The offset survives, so the batch can still advance past it.
        assert_eq!(r.meta.offset, 7);
    }

    #[test]
    fn a_valid_json_document_that_is_not_an_envelope_is_malformed() {
        let r = ParsedRecord::parse(meta(), Some(br#"{"hello":"world"}"#));
        assert!(r.is_malformed());
    }

    #[test]
    fn a_tombstone_is_malformed_not_a_panic() {
        let r = ParsedRecord::parse(meta(), None);
        assert!(r.is_malformed());
    }

    #[test]
    fn malformed_reason_is_truncated_so_logs_stay_readable() {
        let huge = format!(r#"{{"payload": "{}"#, "x".repeat(10_000));
        let r = ParsedRecord::parse(meta(), Some(huge.as_bytes()));
        match r.body {
            RecordBody::Malformed { reason } => assert!(reason.chars().count() <= 201),
            RecordBody::Parsed(_) => panic!("should not parse"),
        }
    }
}
