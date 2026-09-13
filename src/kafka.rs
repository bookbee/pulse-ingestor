//! The Kafka adapter: consumer construction, rebalance handling, the consume
//! loop, and committing.
//!
//! Everything Kafka-specific lives here so [`crate::pipeline`] can be tested
//! without a broker. The rules this module has to respect are stated there;
//! this is where they meet a real consumer.
//!
//! # Auto-commit is off, and that is load-bearing
//!
//! `enable.auto.commit=true` would commit on a timer, in the background,
//! entirely unaware of whether the batch reached GCS. That is precisely the
//! failure the service is designed to avoid, so the setting is forced here
//! rather than left to configuration — a `.env` typo should not be able to turn
//! the correctness property off.
//!
//! # The rebalance callback runs on librdkafka's thread
//!
//! It cannot touch the pipeline directly, so revocations are sent down a
//! channel and drained by the loop between polls. Revocation is *pre*-rebalance
//! on purpose: the buffer must be dropped before the partition moves.

use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use async_trait::async_trait;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::envelope::{ParsedRecord, RecordMeta};
use crate::pipeline::{Committer, Pipeline};
use crate::sink::Sink;

/// Partitions we are about to lose, as seen by librdkafka's rebalance callback.
pub type Revocation = Vec<(String, i32)>;

/// Forwards revocations out of librdkafka's callback thread.
pub struct RebalanceNotifier {
    tx: Sender<Revocation>,
}

impl ClientContext for RebalanceNotifier {}

impl ConsumerContext for RebalanceNotifier {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                let list: Revocation = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic().to_owned(), e.partition()))
                    .collect();
                info!(count = list.len(), "partitions being revoked");
                // A failed send means the loop is already gone; the buffer dies
                // with it, which is the same outcome.
                let _ = self.tx.send(list);
            }
            Rebalance::Assign(tpl) => {
                info!(count = tpl.count(), "partitions assigned");
            }
            Rebalance::Error(e) => {
                error!(error = %e, "rebalance error");
            }
        }
    }
}

pub type PulseConsumer = StreamConsumer<RebalanceNotifier>;

/// Build a consumer subscribed to the three ingestion topics.
///
/// Returns the consumer and the channel on which revocations arrive.
///
/// # Errors
///
/// Fails if the client cannot be created or the subscription is rejected —
/// which, with auto-topic-creation off upstream, is what a misspelled topic
/// looks like.
pub fn build_consumer(
    config: &Config,
) -> anyhow::Result<(Arc<PulseConsumer>, Receiver<Revocation>)> {
    let (tx, rx) = std::sync::mpsc::channel();

    let consumer: PulseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", &config.consumer_group)
        // Non-negotiable: see the module docs.
        .set("enable.auto.commit", "false")
        // We commit explicit offsets ourselves, so offset storage is ours too.
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        // Long enough that a slow GCS upload does not look like a dead
        // consumer and trigger a rebalance mid-batch.
        .set("max.poll.interval.ms", "600000")
        .set("session.timeout.ms", "45000")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(RebalanceNotifier { tx })
        .context("creating Kafka consumer")?;

    let topics = config.topics.all();
    consumer
        .subscribe(&topics)
        .with_context(|| format!("subscribing to {topics:?}"))?;

    Ok((Arc::new(consumer), rx))
}

/// Commits offsets through the consumer that owns the partition.
pub struct KafkaCommitter {
    consumer: Arc<PulseConsumer>,
}

impl KafkaCommitter {
    #[must_use]
    pub const fn new(consumer: Arc<PulseConsumer>) -> Self {
        Self { consumer }
    }
}

#[async_trait]
impl Committer for KafkaCommitter {
    async fn commit(&self, topic: &str, partition: i32, offset: u64) -> anyhow::Result<()> {
        let consumer = Arc::clone(&self.consumer);
        let topic = topic.to_owned();
        let offset = i64::try_from(offset).context("offset exceeds i64")?;

        // A synchronous commit blocks; commits happen once per completed range
        // (10k records by default), so the cost is negligible and the
        // confirmation is worth more than the microseconds.
        tokio::task::spawn_blocking(move || {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&topic, partition, Offset::Offset(offset))?;
            consumer.commit(&tpl, CommitMode::Sync)
        })
        .await
        .context("commit task panicked")?
        .context("committing offsets")?;

        Ok(())
    }
}

/// Convert a broker message into a record, parsing the envelope.
fn to_record(msg: &rdkafka::message::BorrowedMessage<'_>) -> ParsedRecord {
    let meta = RecordMeta {
        topic: msg.topic().to_owned(),
        partition: msg.partition(),
        // Kafka offsets are non-negative; a negative one would be a broker bug.
        offset: u64::try_from(msg.offset()).unwrap_or(0),
        timestamp_ms: msg.timestamp().to_millis(),
    };
    ParsedRecord::parse(meta, msg.payload())
}

/// Run until shutdown, feeding records into `pipeline`.
///
/// # Errors
///
/// Returns on the first unrecoverable pipeline error (a failed upload that is
/// not retryable, or a failed commit). Broker-level errors are logged and
/// polling continues — a transient metadata failure should not kill the
/// process.
pub async fn run<S: Sink, C: Committer>(
    consumer: Arc<PulseConsumer>,
    revocations: Receiver<Revocation>,
    pipeline: &mut Pipeline<S, C>,
    idle_check: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let now_ms = |t: Instant| -> u64 {
        u64::try_from(t.duration_since(started).as_millis()).unwrap_or(u64::MAX)
    };

    let mut ticker = tokio::time::interval(idle_check);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut shutdown = shutdown;

    info!("consume loop started");
    loop {
        // Drop buffers for partitions we no longer own before doing anything
        // else with them.
        drain_revocations(&revocations, pipeline);

        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("shutdown signalled");
                    break;
                }
            }

            _ = ticker.tick() => {
                pipeline.tick(now_ms(Instant::now())).await?;
            }

            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        let record = to_record(&m);
                        debug!(
                            topic = %record.meta.topic,
                            partition = record.meta.partition,
                            offset = record.meta.offset,
                            "consumed"
                        );
                        pipeline.offer(record, now_ms(Instant::now())).await?;
                    }
                    Err(e) => {
                        // Metadata refreshes, leader elections and transient
                        // broker unavailability all land here. Backing off and
                        // continuing is right; exiting would turn a blip into
                        // an outage.
                        warn!(error = %e, "consumer error; continuing");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    // Partial batches are free progress on the way out: the object is
    // overwritten when the range later completes, and nothing is committed.
    info!("flushing buffered batches before exit");
    pipeline.flush_all(now_ms(Instant::now())).await?;

    let s = pipeline.stats();
    info!(
        records = s.records,
        malformed = s.malformed,
        uploads = s.uploads,
        partial_uploads = s.partial_uploads,
        commits = s.commits,
        revocations = s.revocations,
        "consume loop stopped"
    );
    Ok(())
}

fn drain_revocations<S: Sink, C: Committer>(
    rx: &Receiver<Revocation>,
    pipeline: &mut Pipeline<S, C>,
) {
    while let Ok(list) = rx.try_recv() {
        pipeline.revoke(&list);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Topics;
    use std::num::NonZeroU64;

    fn config() -> Config {
        Config {
            bootstrap_servers: "localhost:19092".to_owned(),
            topics: Topics {
                events: "ingestion-events".to_owned(),
                signals: "ingestion-signals".to_owned(),
                logs: "ingestion-logs".to_owned(),
            },
            consumer_group: "test-group".to_owned(),
            batch_offset_range: NonZeroU64::new(10_000).unwrap(),
            batch_max_idle_ms: 30_000,
            gcs_bucket: "pulse-staging-local".to_owned(),
            storage_emulator_host: Some("localhost:4443".to_owned()),
        }
    }

    // rdkafka's StreamConsumer registers with the Tokio reactor on creation,
    // so this needs a runtime even though it never talks to a broker.
    #[tokio::test]
    async fn a_consumer_can_be_built_and_subscribes_to_all_three_topics() {
        // Construction and subscription are local operations — librdkafka
        // connects lazily — so this runs without a broker.
        let (consumer, _rx) = build_consumer(&config()).expect("should build");
        let subscription = consumer.subscription().expect("should have a subscription");
        let mut topics: Vec<String> = subscription
            .elements()
            .iter()
            .map(|e| e.topic().to_owned())
            .collect();
        topics.sort();
        assert_eq!(
            topics,
            vec!["ingestion-events", "ingestion-logs", "ingestion-signals"]
        );
    }

    #[test]
    fn revocations_reach_the_pipeline_through_the_channel() {
        // The callback thread cannot touch the pipeline, so this hop is the
        // only way a revocation gets there. Worth pinning.
        let (tx, rx) = std::sync::mpsc::channel();
        let notifier = RebalanceNotifier { tx };

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("ingestion-events", 2);
        notifier.pre_rebalance(&Rebalance::Revoke(&tpl));

        let got = rx.try_recv().expect("revocation should have been sent");
        assert_eq!(got, vec![("ingestion-events".to_owned(), 2)]);
    }

    #[test]
    fn an_assignment_is_not_reported_as_a_revocation() {
        let (tx, rx) = std::sync::mpsc::channel();
        let notifier = RebalanceNotifier { tx };

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("ingestion-events", 0);
        notifier.pre_rebalance(&Rebalance::Assign(&tpl));

        assert!(rx.try_recv().is_err(), "assignment must not drop buffers");
    }
}
