//! End-to-end verification against the real `pulse-infra` stack.
//!
//! Run with the stack up (`cd ../pulse-infra && make up PROFILE=core`) and the
//! `integration` feature enabled. Without that feature the file compiles to
//! nothing, so `cargo test` stays fast and offline.
//!
//! # What this proves that the unit tests cannot
//!
//! [`pulse_ingestor::pipeline`] tests the rules against fakes. This drives the
//! same code through a real 3-broker cluster and a real GCS emulator:
//! subscription against a broker with auto-topic-creation off, librdkafka's
//! rebalance callback, a genuine `commit` round-trip, a Parquet file that
//! survives HTTP transport, and object names that survive percent-encoding.
//!
//! # What it still cannot prove
//!
//! Nothing about GCS **auth** — the emulator has no IAM and serves plain HTTP.
//! See `pulse-infra/docs/divergences.md`. A green run here is not a deployable
//! artifact.
//!
//! # Why it creates its own topics
//!
//! The gateway has no Kafka producer yet, so nothing else fills the contract
//! topics — but reusing them would make assertions depend on whatever previous
//! runs left behind. Each run creates three throwaway topics, uses them, and
//! deletes them, so offsets start at 0 and the expected object names are exact.

#![cfg(feature = "integration")]

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;

use pulse_ingestor::config::{Config, Topics};
use pulse_ingestor::kafka::{self, KafkaCommitter};
use pulse_ingestor::pipeline::{Pipeline, RetryPolicy};
use pulse_ingestor::sink::GcsSink;

/// In-network addresses: the test runs as a container on the `pulse-infra`
/// network. Override for a host-side run.
fn brokers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-1:9092,kafka-2:9092,kafka-3:9092".to_owned())
}

fn gcs_host() -> String {
    std::env::var("STORAGE_EMULATOR_HOST").unwrap_or_else(|_| "fake-gcs:4443".to_owned())
}

const BUCKET: &str = "pulse-staging-local";

/// Small on purpose: a range of 5 means two complete batches from ten records,
/// so the test exercises completion twice rather than relying on the idle path.
const RANGE: u64 = 5;
const RECORDS: u64 = 10;

fn run_id() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn envelope(n: u64) -> String {
    serde_json::json!({
        "event_id": format!("evt-{n}"),
        "gateway_id": "it-gateway",
        "received_at": "2026-09-14T10:00:00Z",
        "retry_count": 0,
        "stream_name": "ingestion-events",
        "payload": {"n": n, "kind": "integration"}
    })
    .to_string()
}

async fn create_topics(admin: &AdminClient<DefaultClientContext>, names: &[String]) {
    let topics: Vec<NewTopic<'_>> = names
        .iter()
        // One partition: the point here is offset determinism, not rebalancing.
        // RF=3 matches the cluster the contract describes.
        .map(|n| NewTopic::new(n, 1, TopicReplication::Fixed(3)))
        .collect();
    let res = admin
        .create_topics(&topics, &AdminOptions::new())
        .await
        .expect("create_topics should succeed");
    for r in res {
        r.expect("each topic should be created");
    }
}

async fn object_bytes(name: &str) -> Option<Vec<u8>> {
    let url = format!(
        "http://{}/storage/v1/b/{}/o/{}?alt=media",
        gcs_host(),
        BUCKET,
        urlencode(name)
    );
    let resp = reqwest::get(&url).await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    Some(resp.bytes().await.ok()?.to_vec())
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Poll until `name` exists, or give up.
async fn wait_for_object(name: &str, timeout: Duration) -> Option<Vec<u8>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(b) = object_bytes(name).await {
            return Some(b);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Read a Parquet file back and return `(row_count, event_ids)`.
fn read_parquet(bytes: Vec<u8>) -> (usize, Vec<String>) {
    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .expect("should be a valid parquet file")
        .build()
        .expect("should build a reader");

    let mut rows = 0;
    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch should decode");
        rows += batch.num_rows();
        let col = batch
            .column_by_name("event_id")
            .expect("schema must carry event_id")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("event_id is Utf8");
        for i in 0..col.len() {
            if !col.is_null(i) {
                ids.push(col.value(i).to_owned());
            }
        }
    }
    (rows, ids)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumes_from_kafka_and_writes_parquet_batches_to_gcs() {
    let id = run_id();
    let events = format!("it-{id}-events");
    let signals = format!("it-{id}-signals");
    let logs = format!("it-{id}-logs");
    let group = format!("it-{id}-group");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("admin client");

    create_topics(&admin, &[events.clone(), signals.clone(), logs.clone()]).await;

    // --- produce a known, contiguous run of offsets -----------------------
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        // Wait for all in-sync replicas: the consumer must not race the write.
        .set("acks", "all")
        .create()
        .expect("producer");

    for n in 0..RECORDS {
        let payload = envelope(n);
        producer
            .send(
                FutureRecord::to(&events).payload(&payload).key("k"),
                Duration::from_secs(10),
            )
            .await
            .expect("produce should succeed");
    }

    // --- run the ingestor -------------------------------------------------
    let config = Config {
        bootstrap_servers: brokers(),
        topics: Topics {
            events: events.clone(),
            signals: signals.clone(),
            logs: logs.clone(),
        },
        consumer_group: group.clone(),
        batch_offset_range: NonZeroU64::new(RANGE).unwrap(),
        batch_max_idle_ms: 1_000,
        gcs_bucket: BUCKET.to_owned(),
        storage_emulator_host: Some(gcs_host()),
    };

    let sink = GcsSink::new(&config.gcs_bucket, config.storage_emulator_host.as_deref())
        .expect("sink should build");
    let (consumer, revocations) = kafka::build_consumer(&config).expect("consumer should build");
    let committer = KafkaCommitter::new(Arc::clone(&consumer));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let loop_consumer = Arc::clone(&consumer);

    let handle = tokio::spawn(async move {
        let mut pipeline = Pipeline::new(
            sink,
            committer,
            NonZeroU64::new(RANGE).unwrap(),
            Duration::from_millis(1_000),
            RetryPolicy::default(),
        );
        kafka::run(
            loop_consumer,
            revocations,
            &mut pipeline,
            Duration::from_millis(250),
            shutdown_rx,
        )
        .await
        .map(|()| pipeline.stats())
    });

    // --- verify the objects ----------------------------------------------
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let first = format!("{events}/dt={date}/0-0-4.parquet");
    let second = format!("{events}/dt={date}/0-5-9.parquet");

    let first_bytes = wait_for_object(&first, Duration::from_secs(60))
        .await
        .unwrap_or_else(|| panic!("expected {first} in the staging bucket"));
    let second_bytes = wait_for_object(&second, Duration::from_secs(60))
        .await
        .unwrap_or_else(|| panic!("expected {second} in the staging bucket"));

    let (rows_a, ids_a) = read_parquet(first_bytes);
    let (rows_b, ids_b) = read_parquet(second_bytes);

    assert_eq!(rows_a, 5, "first range holds offsets 0..=4");
    assert_eq!(rows_b, 5, "second range holds offsets 5..=9");
    assert_eq!(
        ids_a,
        (0..5).map(|n| format!("evt-{n}")).collect::<Vec<_>>(),
        "rows arrive in offset order"
    );
    assert_eq!(
        ids_b,
        (5..10).map(|n| format!("evt-{n}")).collect::<Vec<_>>()
    );

    // --- stop, then check what was committed ------------------------------
    shutdown_tx.send(true).expect("shutdown should send");
    let stats = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("loop should stop promptly")
        .expect("loop task should not panic")
        .expect("loop should exit cleanly");

    assert_eq!(stats.records, RECORDS, "every produced record was consumed");
    assert_eq!(stats.malformed, 0);
    assert_eq!(stats.uploads, 2, "two complete ranges, two objects");
    assert_eq!(stats.commits, 2);
    assert_eq!(
        stats.partial_uploads, 0,
        "ten records over a range of five leaves nothing partial"
    );

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&events, 0);
    let committed = consumer
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed offsets should be readable");
    let offset = committed
        .elements()
        .first()
        .expect("one partition")
        .offset()
        .to_raw()
        .expect("a real offset");
    assert_eq!(
        offset, 10,
        "Kafka commits the next offset to read, so ten records commit 10"
    );

    // --- clean up ---------------------------------------------------------
    let _ = admin
        .delete_topics(&[&events, &signals, &logs], &AdminOptions::new())
        .await;
}

/// Drive one ingestor against `config` until `stop` resolves, then return its
/// stats. Factored out so the restart test can run two of them over the same
/// consumer group.
async fn run_ingestor(
    config: &Config,
    until: impl std::future::Future<Output = ()> + Send + 'static,
) -> pulse_ingestor::pipeline::Stats {
    let sink = GcsSink::new(&config.gcs_bucket, config.storage_emulator_host.as_deref())
        .expect("sink should build");
    let (consumer, revocations) = kafka::build_consumer(config).expect("consumer should build");
    let committer = KafkaCommitter::new(Arc::clone(&consumer));
    let range = config.batch_offset_range;
    let idle = Duration::from_millis(config.batch_max_idle_ms);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(async move {
        let mut pipeline = Pipeline::new(sink, committer, range, idle, RetryPolicy::default());
        kafka::run(
            consumer,
            revocations,
            &mut pipeline,
            Duration::from_millis(250),
            shutdown_rx,
        )
        .await
        .map(|()| pipeline.stats())
    });

    until.await;
    shutdown_tx.send(true).expect("shutdown should send");
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("loop should stop promptly")
        .expect("loop task should not panic")
        .expect("loop should exit cleanly")
}

/// The crash-recovery path, for real.
///
/// A partial batch is uploaded but **not** committed. A fresh consumer in the
/// same group therefore re-reads the same offsets from the start of the range,
/// and when the range completes it overwrites the same object with a superset.
///
/// If the partial flush committed — or dropped its buffer — this test is what
/// catches it: the final object would hold 2 rows instead of 5, and the first
/// three events would be gone from staging with their offsets acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_batch_is_not_committed_and_is_overwritten_when_the_range_completes() {
    let id = run_id();
    let events = format!("it-{id}-events");
    let signals = format!("it-{id}-signals");
    let logs = format!("it-{id}-logs");
    let group = format!("it-{id}-group");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("admin client");
    create_topics(&admin, &[events.clone(), signals.clone(), logs.clone()]).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("acks", "all")
        .create()
        .expect("producer");

    let send = |n: u64| {
        let producer = producer.clone();
        let topic = events.clone();
        async move {
            let payload = envelope(n);
            producer
                .send(
                    FutureRecord::to(&topic).payload(&payload).key("k"),
                    Duration::from_secs(10),
                )
                .await
                .expect("produce should succeed");
        }
    };

    let config = Config {
        bootstrap_servers: brokers(),
        topics: Topics {
            events: events.clone(),
            signals: signals.clone(),
            logs: logs.clone(),
        },
        consumer_group: group.clone(),
        batch_offset_range: NonZeroU64::new(RANGE).unwrap(),
        batch_max_idle_ms: 1_000,
        gcs_bucket: BUCKET.to_owned(),
        storage_emulator_host: Some(gcs_host()),
    };

    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    // Three records into a range of five: the batch can never complete on its
    // own, so only the idle timer can flush it.
    let object = format!("{events}/dt={date}/0-0-4.parquet");

    // --- first run: partial, uploaded, uncommitted ------------------------
    for n in 0..3 {
        send(n).await;
    }

    let object_for_wait = object.clone();
    let first = run_ingestor(&config, async move {
        wait_for_object(&object_for_wait, Duration::from_secs(60))
            .await
            .unwrap_or_else(|| panic!("expected the partial batch at {object_for_wait}"));
    })
    .await;

    assert_eq!(first.records, 3);
    assert!(
        first.partial_uploads >= 1,
        "the idle timer should have flushed a partial range"
    );
    assert_eq!(
        first.commits, 0,
        "committing here would acknowledge offsets 3..=4, which were never read"
    );

    let (rows, ids) = read_parquet(
        object_bytes(&object)
            .await
            .expect("the partial object should exist"),
    );
    assert_eq!(rows, 3, "the partial object holds what was read so far");
    assert_eq!(ids, vec!["evt-0", "evt-1", "evt-2"]);

    // Nothing was committed, so a fresh consumer in this group starts at the
    // beginning of the range rather than after it.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&events, 0);
    let probe: rdkafka::consumer::BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", &group)
        .create()
        .expect("probe consumer");
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed offsets should be readable");
    assert!(
        committed
            .elements()
            .first()
            .expect("one partition")
            .offset()
            .to_raw()
            .is_none_or(|o| o <= 0),
        "a partial batch must leave the group offset unset"
    );
    drop(probe);

    // --- second run: the range completes and the object is overwritten ----
    for n in 3..5 {
        send(n).await;
    }

    let second = run_ingestor(&config, async move {
        // Give the new consumer time to join, re-read 0..=2 and consume 3..=4.
        tokio::time::sleep(Duration::from_secs(6)).await;
    })
    .await;

    assert_eq!(
        second.records, 5,
        "the restarted consumer re-read the uncommitted offsets 0..=2"
    );
    assert_eq!(
        second.commits, 1,
        "the completed range commits exactly once"
    );

    let (rows, ids) = read_parquet(
        object_bytes(&object)
            .await
            .expect("the object should still exist"),
    );
    assert_eq!(
        rows, 5,
        "the same object was overwritten with the full range, not replaced by \
         the two records the second run added"
    );
    assert_eq!(
        ids,
        vec!["evt-0", "evt-1", "evt-2", "evt-3", "evt-4"],
        "the first three events survived the restart"
    );

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&events, 0);
    let probe: rdkafka::consumer::BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", &group)
        .create()
        .expect("probe consumer");
    let committed = probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed offsets should be readable");
    assert_eq!(
        committed
            .elements()
            .first()
            .expect("one partition")
            .offset()
            .to_raw()
            .expect("a real offset"),
        5,
        "only now, with the range complete, is the offset advanced"
    );

    let _ = admin
        .delete_topics(&[&events, &signals, &logs], &AdminOptions::new())
        .await;
}
