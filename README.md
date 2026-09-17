# pulse-ingestor

Consumes telemetry events from Kafka and writes Parquet batches to GCS staging.

> **Status: working against the local stack.** The full path runs — consume →
> batch → Parquet → GCS → commit — and is verified end to end against a real
> 3-broker Kafka cluster and the GCS emulator. Real GCS **auth** is the
> remaining gap, scheduled for the dev deployment.

## What it does

It reads the three `ingestion-*` topics, groups each partition's messages into
fixed offset ranges, writes each range as a Parquet file, and uploads it to a
deterministic object name in the staging bucket.

"Staging" is a transient landing pad with no retention promise — downstream
workers read from it and it is not a system of record. The word is *staging*,
never "bronze".

## Status in detail

| Module                    | What it does                                                                                      |
| ------------------------- | ------------------------------------------------------------------------------------------------- |
| `src/batching.rs`       | `BatchRange` + `ObjectName` — fixed offset boundaries and the object names derived from them |
| `src/config.rs`         | Contract values from the environment, no silent defaults, every problem reported at once          |
| `src/envelope.rs`       | The gateway's wire envelope; an unparseable record becomes a value, not an error                  |
| `src/parquet_writer.rs` | The staging file schema                                                                           |
| `src/pipeline.rs`       | Accumulate → upload → commit, generic over a sink and a committer                               |
| `src/sink.rs`           | GCS over the JSON API; emulator and real GCS are one code path                                    |
| `src/kafka.rs`          | Consumer, rebalance callback, consume loop, commits                                               |
| `Dockerfile`            | Multi-stage build; 117MB Debian runtime, non-root, no exposed port                                |
| `Makefile`              | Every command below; auto-detects a host toolchain                                                |

**59 unit tests** run with no stack and no network — every correctness rule is
stated against fakes. **2 integration tests** drive the same code through the
real cluster and emulator.

The layering is the point: `batching`, `envelope` and `parquet_writer` are pure,
and `pipeline` is generic over its sink and committer, so the rules that matter
are tested without a broker in the loop.

### Not done yet

- **Real GCS auth.** `sink::ApplicationDefaultCredentials` deliberately returns
  an error rather than a half-written token flow — the emulator cannot validate
  any of it, so code written now would be untested guesswork that looks
  finished. Scheduled for the dev deployment.
- **SASL/TLS Kafka.** Local brokers are PLAINTEXT. The `kafka-tls` feature turns
  on `rdkafka`'s `ssl` and `sasl`; it is off by default so enabling it is a
  visible decision.

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

| Variable                                          | Meaning                                                        |
| ------------------------------------------------- | -------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`                       | Comma-separated broker list                                    |
| `KAFKA_TOPIC_EVENTS` / `_SIGNALS` / `_LOGS` | Topics to consume                                              |
| `KAFKA_CONSUMER_GROUP`                          | This service owns its group; the stack creates none            |
| `BATCH_OFFSET_RANGE`                            | Fixed range width, in offsets                                  |
| `BATCH_MAX_IDLE_MS`                             | How long to wait before flushing a partial trailing batch      |
| `GCS_BUCKET`                                    | Staging bucket                                                 |
| `STORAGE_EMULATOR_HOST`                         | Optional;`host:port` of a GCS emulator. Unset means real GCS |

`RUST_LOG` sets the tracing filter (`pulse_ingestor=debug,rdkafka=warn` to see
every consumed offset).

Three settings are deliberately **not** configurable: `enable.auto.commit` and
`auto.offset.reset` are forced in `src/kafka.rs`, and the object-path template is
hardcoded in `ObjectName::for_range`. Auto-commit runs on a timer that knows
nothing about whether the upload succeeded, and a runtime path template would let
a config edit rename a range that was already written — turning the next retry
into a duplicate instead of an overwrite. `.env.example` explains each omission
where the key would otherwise have been.

## Running it

Everything goes through `make`, and every target works with or without a Rust
toolchain on your machine — `make where` says which one you are getting.

```bash
make infra-up             # shared stack: Kafka + fake GCS (PROFILE=core)
cp .env.example .env      # defaults target the local stack
make run                  # run the service
```

`make help` lists every target. The ones you will use:

| Target                          | What it does                                           |
| ------------------------------- | ------------------------------------------------------ |
| `make run`                    | Run the service against the local stack                |
| `make image`                  | Build the production container image                   |
| `make run-image`              | Run that image against the local stack                 |
| `make test`                   | 59 unit tests — no stack, no network                  |
| `make test-one NAME=…`       | One test by name substring                             |
| `make test-integration`       | 2 integration tests — needs the stack                 |
| `make check`                  | `fmt-check` + `lint` + `test`, what CI would run |
| `make infra-up/-down/-health` | Wrappers over`../pulse-infra`                        |

### Host or container — the addresses differ

This is the one thing that catches people out:

| Running                           | Kafka               | GCS                |
| --------------------------------- | ------------------- | ------------------ |
| On your laptop                    | `localhost:19092` | `localhost:4443` |
| Inside the`pulse-infra` network | `kafka-1:9092`    | `fake-gcs:4443`  |

`.env` holds the **host** addresses, so a native `cargo run` needs no edits. The
container targets inject the in-network addresses as explicit `-e` overrides —
`dotenvy` never overrides a variable that is already set, so those win over the
bind-mounted `.env`. Without that override, a containerised run would dial
`localhost:19092` *inside its own container* and fail looking like a dead broker.

## Build and test

With no toolchain installed, every target runs in a throwaway `rust:1-bookworm`
container against three cached named volumes — registry, rustup toolchain, and
target dir. That cache is why a cold dependency build costs ~90 seconds and a
code change rebuilds in under ten. `CARGO_TARGET_DIR` points away from the bind
mount so container artifacts never collide with the host.

Install a toolchain (see below) and the same `make` targets shell out to your
local `cargo` instead — nothing in the crate depends on the container.

Debian rather than Alpine, unlike `pulse-gateway`: `rdkafka` compiles librdkafka
from source, and musl makes that more fragile for no gain here.

### Optional: a native toolchain

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install cmake        # librdkafka's build needs it
```

Then `make where` reports the host toolchain and `make run` uses it directly,
reading `.env` as-is.

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
`pulse-client`. The integration tests create and delete their own throwaway
topics rather than using the contract ones, so their assertions do not depend on
what previous runs left behind.

## Cross-repo contract

`../pulse-infra/docs/stack-contract.md` is the authority for every topic name,
port, bucket, and object path. Anything on that page is a breaking change for
four-plus repositories — read it before renaming a key in `.env.example`.

Sibling services: `pulse-gateway` (HTTP ingest), `pulse-conflux` (Redis/Kafka
aggregator), `pulse-client`, `pulse-infra` (local stack).
