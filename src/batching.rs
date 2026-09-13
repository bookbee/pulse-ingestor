//! Fixed-boundary batching, and the object names derived from it.
//!
//! # Why fixed boundaries
//!
//! Staging object names embed the offset range they cover:
//!
//! ```text
//! ingestion-events/dt=2026-09-13/0-0-9999.parquet
//! ```
//!
//! The intent is idempotency without a transaction: a retried upload writes the
//! *same* object name and therefore overwrites in place instead of creating a
//! duplicate. That is the whole reason the offsets are in the name.
//!
//! But it only works if the boundaries are **reproducible**. A consumer that
//! batches "whatever happens to be buffered when the timer fires" produces a
//! different range each time it is interrupted: crash mid-batch, or lose the
//! partition to a rebalance, and the consumer that picks it up assembles
//! `[4711, 9993]` where the first had `[4700, 9999]`. Two differently-named
//! objects then cover overlapping events — duplication, not an overwrite, and
//! no amount of retry logic fixes it after the fact.
//!
//! So boundaries are a pure function of the offset, never of timing or
//! buffer state:
//!
//! ```text
//! start = (offset / range_size) * range_size
//! end   = start + range_size - 1
//! ```
//!
//! Any offset in a range maps to that range, on any consumer, at any time. The
//! object name becomes a deterministic function of the data, so retries and
//! reassignments converge on the same object instead of multiplying objects.
//!
//! # The trailing-batch case
//!
//! A low-traffic partition would otherwise wait forever for a full range, so a
//! partial batch is flushed on an idle timer. Crucially it is written to the
//! **same name its full range would use** — `0-0-9999.parquet` even when it
//! holds offsets 0..=12 — so completing the range later overwrites in place
//! rather than leaving `0-0-12.parquet` behind as a permanent duplicate of the
//! first 13 events. See [`ObjectName::for_range`].

use std::num::NonZeroU64;

/// An inclusive, fixed-boundary range of Kafka offsets within one partition.
///
/// Construct with [`BatchRange::containing`] — the boundaries are never chosen
/// by the caller, which is the point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchRange {
    start: u64,
    end: u64,
}

impl BatchRange {
    /// The range that `offset` falls into, given `range_size`.
    ///
    /// This is the only constructor: a range is always aligned to a multiple of
    /// `range_size`, so every consumer derives identical boundaries from the
    /// same offset.
    ///
    /// ```
    /// use std::num::NonZeroU64;
    /// use pulse_ingestor::batching::BatchRange;
    ///
    /// let n = NonZeroU64::new(10_000).unwrap();
    /// let a = BatchRange::containing(0, n);
    /// let b = BatchRange::containing(9_999, n);
    /// assert_eq!(a, b);                 // same range...
    /// assert_eq!(a.start(), 0);
    /// assert_eq!(a.end(), 9_999);
    ///
    /// let c = BatchRange::containing(10_000, n);
    /// assert_eq!(c.start(), 10_000);    // ...next range starts cleanly
    /// ```
    #[must_use]
    pub fn containing(offset: u64, range_size: NonZeroU64) -> Self {
        let size = range_size.get();
        let start = (offset / size) * size;
        // Saturating: a range at the very top of u64 is clamped rather than
        // wrapping to a small number, which would alias a low range's name.
        let end = start.saturating_add(size - 1);
        Self { start, end }
    }

    /// First offset covered (inclusive).
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Last offset covered (inclusive).
    #[must_use]
    pub const fn end(&self) -> u64 {
        self.end
    }

    /// Whether `offset` belongs to this range.
    #[must_use]
    pub const fn contains(&self, offset: u64) -> bool {
        offset >= self.start && offset <= self.end
    }

    /// How many offsets this range spans.
    ///
    /// Note this is the *range width*, not a record count: Kafka offsets are not
    /// guaranteed contiguous (compaction, transaction markers), so a full range
    /// can hold fewer records than its width.
    #[must_use]
    pub const fn width(&self) -> u64 {
        self.end - self.start + 1
    }

    /// The offset a consumer should commit after this range is durably written.
    ///
    /// Kafka commits the *next* offset to read, so this is `end + 1`. Commit
    /// this only after the upload succeeds — an offset committed ahead of a
    /// durable write is silent data loss at the next rebalance.
    #[must_use]
    pub const fn commit_offset(&self) -> u64 {
        self.end + 1
    }

    /// Whether `last_seen` completes this range, i.e. the batch is full.
    #[must_use]
    pub const fn is_complete(&self, last_seen: u64) -> bool {
        last_seen >= self.end
    }
}

/// A staging object name, in the layout the whole platform agrees on.
///
/// Authority for this convention is `pulse-infra/docs/stack-contract.md`:
/// `{topic}/dt=YYYY-MM-DD/{partition}-{startoffset}-{endoffset}.parquet`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(String);

impl ObjectName {
    /// Build the name for a range.
    ///
    /// `date` is the partition date as `YYYY-MM-DD`, in UTC. It is the date the
    /// batch is *written*, not an event timestamp — so a batch spanning
    /// midnight lands wholly under the date it was flushed, and late-arriving
    /// events land under today rather than being backdated. Anything that needs
    /// event-time partitioning is the silver worker's problem, not staging's.
    ///
    /// The name depends only on topic, partition, date and the *aligned* range,
    /// never on how many records were actually written. A partial flush and the
    /// later complete batch therefore share a name, and the complete one
    /// overwrites the partial.
    #[must_use]
    pub fn for_range(topic: &str, partition: i32, date: &str, range: BatchRange) -> Self {
        Self(format!(
            "{topic}/dt={date}/{partition}-{start}-{end}.parquet",
            start = range.start(),
            end = range.end(),
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).expect("test range size must be non-zero")
    }

    #[test]
    fn range_starts_at_zero() {
        let r = BatchRange::containing(0, n(10_000));
        assert_eq!((r.start(), r.end()), (0, 9_999));
        assert_eq!(r.width(), 10_000);
    }

    #[test]
    fn every_offset_in_a_range_maps_to_that_range() {
        let size = n(10_000);
        let expected = BatchRange::containing(0, size);
        for offset in [0, 1, 42, 5_000, 9_998, 9_999] {
            assert_eq!(
                BatchRange::containing(offset, size),
                expected,
                "offset {offset} should map to the first range"
            );
        }
    }

    #[test]
    fn boundary_starts_a_new_range() {
        let size = n(10_000);
        let first = BatchRange::containing(9_999, size);
        let second = BatchRange::containing(10_000, size);
        assert_ne!(first, second);
        assert_eq!(
            first.end() + 1,
            second.start(),
            "ranges must not gap or overlap"
        );
        assert_eq!(second.end(), 19_999);
    }

    #[test]
    fn ranges_tile_the_offset_space_without_gaps_or_overlap() {
        let size = n(256);
        let mut previous: Option<BatchRange> = None;
        for offset in (0..4_096).step_by(7) {
            let r = BatchRange::containing(offset, size);
            assert!(r.contains(offset));
            if let Some(p) = previous {
                if p != r {
                    assert_eq!(
                        p.end() + 1,
                        r.start(),
                        "gap or overlap between {p:?} and {r:?}"
                    );
                }
            }
            previous = Some(r);
        }
    }

    #[test]
    fn range_size_of_one_is_degenerate_but_valid() {
        let r = BatchRange::containing(7, n(1));
        assert_eq!((r.start(), r.end()), (7, 7));
        assert_eq!(r.width(), 1);
        assert_eq!(r.commit_offset(), 8);
    }

    #[test]
    fn commit_offset_is_one_past_the_end() {
        let r = BatchRange::containing(0, n(10_000));
        assert_eq!(
            r.commit_offset(),
            10_000,
            "Kafka commits the next offset to read"
        );
    }

    #[test]
    fn completeness_is_driven_by_the_last_offset_seen() {
        let r = BatchRange::containing(0, n(100));
        assert!(!r.is_complete(0));
        assert!(!r.is_complete(98));
        assert!(r.is_complete(99));
        assert!(r.is_complete(100), "overshoot still counts as complete");
    }

    #[test]
    fn top_of_offset_space_clamps_instead_of_wrapping() {
        // A wrap here would alias a high range onto a low range's object name,
        // which would silently overwrite real data.
        let r = BatchRange::containing(u64::MAX, n(10_000));
        assert_eq!(r.end(), u64::MAX);
        assert!(r.contains(u64::MAX));
    }

    // ─── Object naming ──────────────────────────────────────────────────────

    #[test]
    fn object_name_matches_the_platform_contract() {
        let r = BatchRange::containing(0, n(10_000));
        let name = ObjectName::for_range("ingestion-events", 0, "2026-09-13", r);
        assert_eq!(
            name.as_str(),
            "ingestion-events/dt=2026-09-13/0-0-9999.parquet"
        );
    }

    #[test]
    fn name_is_identical_for_any_offset_in_the_range() {
        // This is the idempotency property: a retry that resumes from a
        // different offset inside the same range still targets one object.
        let size = n(10_000);
        let from_start = ObjectName::for_range(
            "ingestion-events",
            3,
            "2026-09-13",
            BatchRange::containing(20_000, size),
        );
        let from_middle = ObjectName::for_range(
            "ingestion-events",
            3,
            "2026-09-13",
            BatchRange::containing(27_431, size),
        );
        assert_eq!(from_start, from_middle);
    }

    #[test]
    fn partial_batch_uses_the_full_range_name() {
        // A trailing flush holding only offsets 0..=12 must still be named for
        // the whole range, so completing it later overwrites rather than
        // leaving a duplicate object behind.
        let size = n(10_000);
        let partial = ObjectName::for_range(
            "ingestion-logs",
            1,
            "2026-09-13",
            BatchRange::containing(12, size),
        );
        let complete = ObjectName::for_range(
            "ingestion-logs",
            1,
            "2026-09-13",
            BatchRange::containing(9_999, size),
        );
        assert_eq!(partial, complete);
        assert_eq!(
            partial.as_str(),
            "ingestion-logs/dt=2026-09-13/1-0-9999.parquet"
        );
    }

    #[test]
    fn partition_and_date_and_topic_all_separate_objects() {
        let size = n(10_000);
        let r = BatchRange::containing(0, size);
        let base = ObjectName::for_range("ingestion-events", 0, "2026-09-13", r);
        assert_ne!(
            base,
            ObjectName::for_range("ingestion-signals", 0, "2026-09-13", r)
        );
        assert_ne!(
            base,
            ObjectName::for_range("ingestion-events", 1, "2026-09-13", r)
        );
        assert_ne!(
            base,
            ObjectName::for_range("ingestion-events", 0, "2026-09-14", r)
        );
    }
}
