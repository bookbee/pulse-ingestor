# pulse-ingestor

Consumes telemetry events from Kafka and writes Parquet batches to GCS staging.

> **Status: early.** The batching and configuration layers are built and tested.
> The Kafka consume loop, the Parquet writer, and the GCS upload are not written
> yet — `src/main.rs` validates configuration, prints what it resolved, and exits
> non-zero so nothing mistakes it for a running ingestor.

## What it does

It reads the three `ingestion-*` topics, groups each partition's messages into
fixed offset ranges, writes each range as a Parquet file, and uploads it to a
deterministic object name in the staging bucket.

"Staging" is a transient landing pad with no retention promise — downstream
workers read from it and it is not a system of record. The word is *staging*,
never "bronze".

## Status in detail

| Module | What it does | Tests |
|---|---|---|
| `src/batching.rs` | `BatchRange` + `ObjectName` — fixed offset boundaries and the object names derived from them | 12 |
| `src/config.rs` | Cross-repo contract values from the environment, no silent defaults, every problem reported at once | 7 |
| `src/main.rs` | Loads config, reports it, exits non-zero | — |

`Cargo.toml` has **zero dependencies** on purpose. What is built here is the part
whose correctness cannot be repaired later without a data migration, and that
logic is pure. The dependencies the I/O layers will need (`rdkafka`, `arrow` /
`parquet`, `object_store`, `tokio`, `serde`, `tracing`) are listed as comments in
the manifest; adding one is a deliberate decision, not a reflex.

## The design invariant

**Commit after upload, never before.** An offset committed ahead of a durable
write is silent data loss the next time the partition moves. The sequence is:

1. accumulate a **fixed** offset range
2. write Parquet
3. upload to the deterministic object name
4. *then* commit `BatchRange::commit_offset()` — which is `end + 1`, because
   Kafka commits the next offset to read

Crash anywhere before step 4 and the range is re-read and re-uploaded to the
same object name, overwriting cleanly.

### Why the boundaries are fixed

That guarantee holds only if batch boundaries are reproducible. A consumer that
batches "whatever is buffered when the timer fires" assembles `[4711, 9993]`
where a previous attempt had `[4700, 9999]` — two differently-named objects
covering overlapping events. That is duplication rather than an overwrite, and it
cannot be fixed after the fact.

So `start = (offset / range_size) * range_size`, always. A partial trailing batch
is written to the **same name its full range would use**, so completing it later
overwrites in place instead of stranding a duplicate.

This was chosen explicitly over the alternative — batch freely and deduplicate
downstream in the silver worker. Making batching timing- or size-driven silently
breaks staging as a source of truth, so revisit that decision before changing it.

## Object layout

```
{topic}/dt=YYYY-MM-DD/{partition}-{start_offset}-{end_offset}.parquet
```

For example, the first 10 000 offsets of partition 0:

```
gs://pulse-staging-local/ingestion-events/dt=2026-09-13/0-0-9999.parquet
```

The date is the **write** date in UTC, not an event timestamp.

## Configuration

Copy `.env.example` to `.env`. Every contract value is required and has no
default — a wrong default looks like it worked.

| Variable | Meaning |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker list |
| `KAFKA_TOPIC_EVENTS` / `_SIGNALS` / `_LOGS` | Topics to consume |
| `KAFKA_CONSUMER_GROUP` | This service owns its group; the stack creates none |
| `BATCH_OFFSET_RANGE` | Fixed range width, in offsets |
| `BATCH_MAX_IDLE_MS` | How long to wait before flushing a partial trailing batch |
| `GCS_BUCKET` | Staging bucket |
| `STORAGE_EMULATOR_HOST` | Optional; `host:port` of a GCS emulator. Unset means real GCS |

`.env.example` also carries `KAFKA_ENABLE_AUTO_COMMIT`, `KAFKA_AUTO_OFFSET_RESET`,
`GCS_PATH_TEMPLATE`, and `RUST_LOG`. Those are placeholders for the consume loop
and are **not read by `config.rs` yet** — setting them today changes nothing.

## Build and test

There is no Rust toolchain on the primary development machine, so builds run in a
container. `target/` is gitignored and the container writes it as the host user,
so no root-owned artifacts are left behind.

```bash
# all tests
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; cargo test'

# a single test
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; cargo test partial_batch_uses_the_full_range_name'

# lint and format — both must be clean
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; rustup component add clippy rustfmt >/dev/null;
   cargo clippy --all-targets -- -D warnings && cargo fmt --check'
```

`musl-dev` is required because the Alpine image builds against musl. Nothing in
the crate depends on the container — with a real toolchain installed, plain
`cargo test` works.

## Local development

```bash
cd ../pulse-infra && make up PROFILE=core   # Kafka + fake GCS
```

From the host, brokers are `localhost:19092,19093,19094`; inside the stack's
Docker network they are `kafka-1:9092,kafka-2:9092,kafka-3:9092`. The GCS
emulator is `localhost:4443` / `fake-gcs:4443`, unauthenticated.

The multi-broker default is deliberate: a single broker cannot exercise
consumer-group rebalancing or ISR behaviour, which is exactly what this service's
correctness depends on.

**GCS auth is validated at dev deployment, not locally.** The emulator has no
IAM, serves plain HTTP, and returns different error bodies, so scopes, ADC, token
refresh, 401-vs-403-vs-429 handling, and TLS get their first real exercise in the
dev environment. That is a deliberate decision — see
`../pulse-infra/docs/divergences.md`. A green local run says nothing about auth.

**Topics may be empty locally, and that is expected.** The gateway's Kafka
producer is still in development (`pulse-gateway`, specced as D8–D13), so until
it lands these topics are fed only by this service's own tests and
`pulse-client`.

## Cross-repo contract

`../pulse-infra/docs/stack-contract.md` is the authority for every topic name,
port, bucket, and object path. Anything on that page is a breaking change for
four-plus repositories — read it before renaming a key in `.env.example`.

Sibling services: `pulse-gateway` (HTTP ingest), `pulse-conflux` (Redis/Kafka
aggregator), `pulse-client`, `pulse-infra` (local stack).
