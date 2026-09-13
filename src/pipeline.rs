//! Accumulate → upload → commit, and the rules that keep that order safe.
//!
//! This module is where the service's one correctness property is actually
//! enforced. It is deliberately free of Kafka and HTTP types: it drives a
//! [`Sink`] and a [`Committer`], both of which are fakes in tests, so every
//! rule below is verified without a broker or a bucket.
//!
//! # Rule 1 — commit after upload, never before
//!
//! An offset committed ahead of a durable write is silent data loss at the next
//! rebalance. [`Pipeline`] only ever calls [`Committer::commit`] after
//! [`Sink::put`] has returned `Ok`.
//!
//! # Rule 2 — a partial batch uploads but does not commit
//!
//! This is the subtle one, and getting it wrong loses data in a way no test of
//! the happy path would notice.
//!
//! A low-traffic partition flushes on an idle timer, holding (say) offsets
//! `0..=12` of the range `[0, 9999]`. It writes them to `0-0-9999.parquet` —
//! the *full range's* name, so completing the range later overwrites in place.
//!
//! The trap: `BatchRange::commit_offset()` for that range is `10000`. Committing
//! it after a partial flush would acknowledge 9 987 offsets that were never
//! read. So a partial flush commits **nothing**.
//!
//! The second trap: if a partial flush also *cleared* the buffer, the next
//! upload would contain only `13..=9999` and would overwrite the object that
//! held `0..=12` — destroying them. So a partial flush **keeps** its records,
//! and each subsequent upload of that range is a strict superset of the last.
//!
//! The cost is bounded and deliberate: an idle partition holds at most one
//! range in memory and re-uploads it as it grows, and a restart re-reads from
//! the range start. The alternative — committing early — trades a bounded
//! memory cost for unbounded data loss.
//!
//! # Rule 3 — a revoked partition is dropped, not flushed
//!
//! On revocation the buffered records are uncommitted by construction, so the
//! next owner re-reads them from the last committed offset. Dropping is
//! therefore safe and simple. Flushing on the way out would also be *correct*
//! — same range, same name, same bytes — but it races the new owner for the
//! same object while holding a partition we no longer own, for no gain.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, error, info, warn};

use crate::batching::{BatchRange, ObjectName};
use crate::envelope::ParsedRecord;
use crate::sink::{Sink, UploadError};

/// Commits consumed offsets back to the broker.
#[async_trait]
pub trait Committer: Send + Sync {
    /// Record that everything before `offset` is durably stored.
    ///
    /// `offset` is the *next offset to read*, Kafka's convention.
    async fn commit(&self, topic: &str, partition: i32, offset: u64) -> anyhow::Result<()>;
}

/// How hard to try a failed upload before giving up.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(250),
        }
    }
}

impl RetryPolicy {
    /// No waiting, one attempt — for tests.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::ZERO,
        }
    }
}

/// What a flush did, from the caller's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Range complete: uploaded, then committed this offset.
    Committed(u64),
    /// Partial range: uploaded, nothing committed, records retained.
    UploadedWithoutCommit,
    /// Nothing to do.
    Nothing,
}

/// Counters worth looking at in a log line.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub records: u64,
    pub malformed: u64,
    pub uploads: u64,
    pub commits: u64,
    pub partial_uploads: u64,
    pub revocations: u64,
}

/// Per-partition accumulation state. Pure: no I/O, no clock of its own.
#[derive(Debug)]
struct PartitionBatcher {
    range: BatchRange,
    records: Vec<ParsedRecord>,
    /// Records added since the last successful upload of this range.
    dirty: bool,
    last_append_ms: u64,
}

impl PartitionBatcher {
    fn new(range: BatchRange, now_ms: u64) -> Self {
        Self {
            range,
            records: Vec::new(),
            dirty: false,
            last_append_ms: now_ms,
        }
    }

    fn push(&mut self, record: ParsedRecord, now_ms: u64) {
        self.records.push(record);
        self.dirty = true;
        self.last_append_ms = now_ms;
    }

    fn is_complete(&self) -> bool {
        self.records
            .last()
            .is_some_and(|r| self.range.is_complete(r.meta.offset))
    }

    fn is_idle(&self, now_ms: u64, idle_ms: u64) -> bool {
        self.dirty && now_ms.saturating_sub(self.last_append_ms) >= idle_ms
    }
}

/// Drives records into batches, uploads them, and commits — in that order.
pub struct Pipeline<S: Sink, C: Committer> {
    sink: S,
    committer: C,
    range_size: NonZeroU64,
    idle_ms: u64,
    retry: RetryPolicy,
    batchers: HashMap<(String, i32), PartitionBatcher>,
    stats: Stats,
}

impl<S: Sink, C: Committer> Pipeline<S, C> {
    pub fn new(
        sink: S,
        committer: C,
        range_size: NonZeroU64,
        idle: Duration,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            sink,
            committer,
            range_size,
            idle_ms: u64::try_from(idle.as_millis()).unwrap_or(u64::MAX),
            retry,
            batchers: HashMap::new(),
            stats: Stats::default(),
        }
    }

    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    /// Number of partitions currently holding buffered records.
    #[must_use]
    pub fn active_partitions(&self) -> usize {
        self.batchers.len()
    }

    /// Feed one consumed record in.
    ///
    /// May trigger an upload (and, if the range completed, a commit).
    ///
    /// # Errors
    ///
    /// Propagates a non-retryable upload failure or a commit failure. A
    /// retryable upload failure is retried per [`RetryPolicy`] first.
    pub async fn offer(
        &mut self,
        record: ParsedRecord,
        now_ms: u64,
    ) -> anyhow::Result<FlushOutcome> {
        self.stats.records += 1;
        if record.is_malformed() {
            self.stats.malformed += 1;
            warn!(
                topic = %record.meta.topic,
                partition = record.meta.partition,
                offset = record.meta.offset,
                "record could not be parsed; writing it as a flagged row"
            );
        }

        let key = (record.meta.topic.clone(), record.meta.partition);
        let range = BatchRange::containing(record.meta.offset, self.range_size);

        // A record outside the current range means the range is over — either
        // completed, or skipped past (compaction, transaction markers, a seek).
        // Either way the old batch must go out before the new one starts, or it
        // would be silently replaced and its records lost.
        if let Some(existing) = self.batchers.get(&key) {
            if existing.range != range && existing.dirty {
                debug!(
                    topic = %key.0, partition = key.1,
                    from = existing.range.start(), to = range.start(),
                    "range boundary crossed; flushing the previous range first"
                );
                self.flush_partition(&key, now_ms).await?;
            }
        }

        let batcher = self
            .batchers
            .entry(key.clone())
            .or_insert_with(|| PartitionBatcher::new(range, now_ms));

        // After a boundary flush the entry may still describe the old range.
        if batcher.range != range {
            *batcher = PartitionBatcher::new(range, now_ms);
        }

        batcher.push(record, now_ms);

        if batcher.is_complete() {
            return self.flush_partition(&key, now_ms).await;
        }
        Ok(FlushOutcome::Nothing)
    }

    /// Flush any partition that has gone quiet.
    ///
    /// # Errors
    ///
    /// Propagates the first flush failure.
    pub async fn tick(&mut self, now_ms: u64) -> anyhow::Result<Vec<FlushOutcome>> {
        let idle: Vec<(String, i32)> = self
            .batchers
            .iter()
            .filter(|(_, b)| b.is_idle(now_ms, self.idle_ms))
            .map(|(k, _)| k.clone())
            .collect();

        let mut outcomes = Vec::with_capacity(idle.len());
        for key in idle {
            outcomes.push(self.flush_partition(&key, now_ms).await?);
        }
        Ok(outcomes)
    }

    /// Drop buffered state for partitions we no longer own.
    ///
    /// See rule 3 in the module docs: the records are uncommitted, so the next
    /// owner will re-read them.
    pub fn revoke(&mut self, partitions: &[(String, i32)]) {
        for key in partitions {
            if let Some(b) = self.batchers.remove(key) {
                self.stats.revocations += 1;
                info!(
                    topic = %key.0, partition = key.1,
                    buffered = b.records.len(), range_start = b.range.start(),
                    "partition revoked; dropping uncommitted buffer for the next owner to re-read"
                );
            }
        }
    }

    /// Upload whatever is buffered, committing only complete ranges.
    ///
    /// Used at shutdown: uploading a partial range is free progress, because
    /// the object is overwritten when the range later completes.
    ///
    /// # Errors
    ///
    /// Propagates the first flush failure.
    pub async fn flush_all(&mut self, now_ms: u64) -> anyhow::Result<Vec<FlushOutcome>> {
        let keys: Vec<(String, i32)> = self
            .batchers
            .iter()
            .filter(|(_, b)| b.dirty)
            .map(|(k, _)| k.clone())
            .collect();

        let mut outcomes = Vec::with_capacity(keys.len());
        for key in keys {
            outcomes.push(self.flush_partition(&key, now_ms).await?);
        }
        Ok(outcomes)
    }

    async fn flush_partition(
        &mut self,
        key: &(String, i32),
        now_ms: u64,
    ) -> anyhow::Result<FlushOutcome> {
        let Some(batcher) = self.batchers.get(key) else {
            return Ok(FlushOutcome::Nothing);
        };
        if !batcher.dirty || batcher.records.is_empty() {
            return Ok(FlushOutcome::Nothing);
        }

        let (topic, partition) = (key.0.clone(), key.1);
        let range = batcher.range;
        let complete = batcher.is_complete();
        let count = batcher.records.len();

        let now = Utc::now();
        // The write date, per the stack contract — not an event timestamp.
        let date = now.format("%Y-%m-%d").to_string();
        let name = ObjectName::for_range(&topic, partition, &date, range);

        let bytes = crate::parquet_writer::write_batch(&batcher.records, now)?;
        let size = bytes.len();

        self.upload_with_retry(&name, bytes).await?;
        self.stats.uploads += 1;

        info!(
            object = %name, records = count, bytes = size, complete,
            "uploaded batch"
        );

        // Only now is it safe to touch offsets.
        if complete {
            let commit_at = range.commit_offset();
            self.committer.commit(&topic, partition, commit_at).await?;
            self.stats.commits += 1;
            debug!(topic = %topic, partition, offset = commit_at, "committed after upload");

            self.batchers.remove(key);
            Ok(FlushOutcome::Committed(commit_at))
        } else {
            // Rule 2: no commit, and the records stay so the next upload of
            // this range is a superset rather than a replacement.
            self.stats.partial_uploads += 1;
            if let Some(b) = self.batchers.get_mut(key) {
                b.dirty = false;
                b.last_append_ms = now_ms;
            }
            Ok(FlushOutcome::UploadedWithoutCommit)
        }
    }

    async fn upload_with_retry(&self, name: &ObjectName, bytes: Vec<u8>) -> anyhow::Result<()> {
        let mut attempt = 1;
        loop {
            match self.sink.put(name, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Auth and permanent failures are not retried — retrying a
                    // 401 turns one misconfiguration into a rate-limit event.
                    if !e.is_retryable() || attempt >= self.retry.max_attempts {
                        error!(object = %name, attempt, error = %e, "upload failed");
                        return Err(match e {
                            UploadError::Auth(m) => {
                                anyhow::anyhow!("upload rejected, not retrying: {m}")
                            }
                            other => anyhow::Error::from(other),
                        });
                    }
                    let delay = self.retry.base_delay * 2_u32.saturating_pow(attempt - 1);
                    warn!(object = %name, attempt, error = %e, ?delay, "retrying upload");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Envelope, RecordBody, RecordMeta};
    use crate::sink::UploadError;
    use std::sync::Mutex;

    // --- fakes ----------------------------------------------------------

    #[derive(Default)]
    struct FakeSink {
        puts: Mutex<Vec<(String, usize)>>,
        fail_with: Mutex<Option<UploadError>>,
    }

    impl FakeSink {
        fn names(&self) -> Vec<String> {
            self.puts
                .lock()
                .unwrap()
                .iter()
                .map(|p| p.0.clone())
                .collect()
        }
        fn row_counts(&self) -> Vec<usize> {
            self.puts.lock().unwrap().iter().map(|p| p.1).collect()
        }
    }

    #[async_trait]
    impl Sink for FakeSink {
        async fn put(&self, name: &ObjectName, bytes: Vec<u8>) -> Result<(), UploadError> {
            if let Some(e) = self.fail_with.lock().unwrap().take() {
                return Err(e);
            }
            // Row count is recovered from the parquet footer by counting rows
            // the cheap way: we stash the byte length and trust the writer's
            // own tests for content. What matters here is *which* object and
            // *how many* times.
            self.puts
                .lock()
                .unwrap()
                .push((name.to_string(), bytes.len()));
            Ok(())
        }
        fn describe(&self) -> String {
            "fake".to_owned()
        }
    }

    #[derive(Default)]
    struct FakeCommitter {
        commits: Mutex<Vec<(String, i32, u64)>>,
    }

    impl FakeCommitter {
        fn offsets(&self) -> Vec<u64> {
            self.commits.lock().unwrap().iter().map(|c| c.2).collect()
        }
    }

    #[async_trait]
    impl Committer for FakeCommitter {
        async fn commit(&self, topic: &str, partition: i32, offset: u64) -> anyhow::Result<()> {
            self.commits
                .lock()
                .unwrap()
                .push((topic.to_owned(), partition, offset));
            Ok(())
        }
    }

    // --- helpers --------------------------------------------------------

    fn rec(offset: u64) -> ParsedRecord {
        ParsedRecord {
            meta: RecordMeta {
                topic: "ingestion-events".to_owned(),
                partition: 0,
                offset,
                timestamp_ms: Some(1_760_000_000_000),
            },
            body: RecordBody::Parsed(Box::new(Envelope {
                event_id: format!("evt-{offset}"),
                gateway_id: "gw-1".to_owned(),
                received_at: "2026-09-14T10:00:00Z".to_owned(),
                retry_count: 0,
                stream_name: None,
                event_header: None,
                payload: serde_json::json!({"n": offset}),
            })),
        }
    }

    fn pipeline(range: u64, idle_ms: u64) -> Pipeline<FakeSink, FakeCommitter> {
        Pipeline::new(
            FakeSink::default(),
            FakeCommitter::default(),
            NonZeroU64::new(range).unwrap(),
            Duration::from_millis(idle_ms),
            RetryPolicy::none(),
        )
    }

    // --- the rules ------------------------------------------------------

    #[tokio::test]
    async fn a_complete_range_uploads_once_then_commits_the_next_offset() {
        let mut p = pipeline(4, 1_000);
        for o in 0..4 {
            p.offer(rec(o), 0).await.unwrap();
        }
        assert_eq!(
            p.sink.names(),
            vec![
                "ingestion-events/dt=".to_owned()
                    + &Utc::now().format("%Y-%m-%d").to_string()
                    + "/0-0-3.parquet"
            ]
        );
        // Kafka commits the *next* offset to read.
        assert_eq!(p.committer.offsets(), vec![4]);
        assert_eq!(p.active_partitions(), 0, "completed range is released");
    }

    #[tokio::test]
    async fn nothing_is_committed_before_the_range_completes() {
        let mut p = pipeline(4, 1_000);
        for o in 0..3 {
            p.offer(rec(o), 0).await.unwrap();
        }
        assert!(p.sink.names().is_empty(), "no upload yet");
        assert!(p.committer.offsets().is_empty(), "and certainly no commit");
    }

    #[tokio::test]
    async fn an_idle_partial_batch_uploads_but_commits_nothing() {
        // The rule that stops us acknowledging offsets we never read.
        let mut p = pipeline(10_000, 100);
        for o in 0..13 {
            p.offer(rec(o), 0).await.unwrap();
        }
        let out = p.tick(1_000).await.unwrap();

        assert_eq!(out, vec![FlushOutcome::UploadedWithoutCommit]);
        assert_eq!(p.sink.names().len(), 1);
        assert!(
            p.committer.offsets().is_empty(),
            "committing 10000 here would acknowledge 9987 unread offsets"
        );
    }

    #[tokio::test]
    async fn a_partial_batch_is_written_under_its_full_range_name() {
        let mut p = pipeline(10_000, 100);
        p.offer(rec(0), 0).await.unwrap();
        p.tick(1_000).await.unwrap();

        let name = p.sink.names().into_iter().next().unwrap();
        assert!(
            name.ends_with("/0-0-9999.parquet"),
            "got {name}: a partial batch must use the full range's name so \
             completing it later overwrites in place"
        );
    }

    #[tokio::test]
    async fn completing_a_partially_flushed_range_re_uploads_a_superset() {
        // The trap: if the partial flush dropped its records, this second
        // upload would contain only offsets 2..=3 and would overwrite — and
        // destroy — the object holding 0..=1.
        let mut p = pipeline(4, 100);
        p.offer(rec(0), 0).await.unwrap();
        p.offer(rec(1), 0).await.unwrap();
        p.tick(1_000).await.unwrap();

        let first_size = p.sink.row_counts()[0];

        p.offer(rec(2), 2_000).await.unwrap();
        p.offer(rec(3), 2_000).await.unwrap();

        let names = p.sink.names();
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], names[1], "same object name, overwritten in place");
        assert!(
            p.sink.row_counts()[1] > first_size,
            "the second upload must be a superset, not a replacement"
        );
        assert_eq!(
            p.committer.offsets(),
            vec![4],
            "committed only once complete"
        );
    }

    #[tokio::test]
    async fn an_idle_tick_with_no_new_records_does_not_re_upload() {
        let mut p = pipeline(10_000, 100);
        p.offer(rec(0), 0).await.unwrap();
        p.tick(1_000).await.unwrap();
        p.tick(2_000).await.unwrap();
        assert_eq!(p.sink.names().len(), 1, "nothing changed, nothing to write");
    }

    #[tokio::test]
    async fn a_revoked_partition_is_dropped_without_committing() {
        let mut p = pipeline(10_000, 100);
        for o in 0..5 {
            p.offer(rec(o), 0).await.unwrap();
        }
        p.revoke(&[("ingestion-events".to_owned(), 0)]);

        assert_eq!(p.active_partitions(), 0);
        assert!(
            p.committer.offsets().is_empty(),
            "the next owner re-reads these offsets; committing would lose them"
        );
        assert_eq!(p.stats().revocations, 1);
    }

    #[tokio::test]
    async fn crossing_a_range_boundary_flushes_the_previous_range_first() {
        let mut p = pipeline(4, 100);
        p.offer(rec(0), 0).await.unwrap();
        // Jump past the end of range [0,3] without completing it. Offset 5
        // lands mid-way through [4,7], so the only upload this can produce is
        // the abandoned range's.
        p.offer(rec(5), 0).await.unwrap();

        let names = p.sink.names();
        assert_eq!(
            names.len(),
            1,
            "the abandoned range went out before the new one started"
        );
        assert!(names[0].ends_with("/0-0-3.parquet"));
        assert!(
            p.committer.offsets().is_empty(),
            "an abandoned partial range is uploaded, never committed"
        );
    }

    #[tokio::test]
    async fn partitions_are_batched_independently() {
        let mut p = pipeline(4, 100);
        let mut r = rec(0);
        r.meta.partition = 3;
        p.offer(r, 0).await.unwrap();
        p.offer(rec(0), 0).await.unwrap();
        assert_eq!(p.active_partitions(), 2, "one flood must not flush another");
    }

    #[tokio::test]
    async fn a_malformed_record_still_advances_the_batch() {
        let mut p = pipeline(2, 100);
        let mut bad = rec(0);
        bad.body = RecordBody::Malformed {
            reason: "boom".to_owned(),
        };
        p.offer(bad, 0).await.unwrap();
        p.offer(rec(1), 0).await.unwrap();

        assert_eq!(p.sink.names().len(), 1);
        assert_eq!(
            p.committer.offsets(),
            vec![2],
            "the bad offset is not skipped"
        );
        assert_eq!(p.stats().malformed, 1);
    }

    #[tokio::test]
    async fn an_auth_failure_aborts_without_committing() {
        let mut p = pipeline(2, 100);
        *p.sink.fail_with.lock().unwrap() = Some(UploadError::Auth("401".to_owned()));

        p.offer(rec(0), 0).await.unwrap();
        let err = p.offer(rec(1), 0).await.unwrap_err();

        assert!(err.to_string().contains("not retrying"));
        assert!(
            p.committer.offsets().is_empty(),
            "a failed upload must never be followed by a commit"
        );
    }

    #[tokio::test]
    async fn a_retryable_failure_is_retried_and_then_succeeds() {
        let mut p = Pipeline::new(
            FakeSink::default(),
            FakeCommitter::default(),
            NonZeroU64::new(2).unwrap(),
            Duration::from_millis(100),
            RetryPolicy {
                max_attempts: 3,
                base_delay: Duration::ZERO,
            },
        );
        *p.sink.fail_with.lock().unwrap() = Some(UploadError::Retryable("503".to_owned()));

        p.offer(rec(0), 0).await.unwrap();
        p.offer(rec(1), 0).await.unwrap();

        assert_eq!(p.sink.names().len(), 1, "the retry succeeded");
        assert_eq!(p.committer.offsets(), vec![2]);
    }

    #[tokio::test]
    async fn flush_all_uploads_partials_at_shutdown_without_committing_them() {
        let mut p = pipeline(10_000, 60_000);
        p.offer(rec(0), 0).await.unwrap();
        let out = p.flush_all(1).await.unwrap();

        assert_eq!(out, vec![FlushOutcome::UploadedWithoutCommit]);
        assert_eq!(p.sink.names().len(), 1);
        assert!(p.committer.offsets().is_empty());
    }
}
