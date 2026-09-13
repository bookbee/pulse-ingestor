# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Early Rust crate. **Language decided 2026-09-13: Rust** (not Java).

What exists is deliberately the part that cannot be changed later without a data
migration:

- `src/batching.rs` — fixed offset-range boundaries and the object names derived
  from them. This is where the idempotency guarantee lives or dies. 12 tests.
- `src/config.rs` — cross-repo contract values, loaded with no silent defaults,
  reporting every problem at once. 7 tests.
- `src/main.rs` — loads config, prints what it resolved, exits **non-zero**. It
  does not idle, so nothing mistakes it for a running ingestor.

**Not written yet**: the Kafka consume loop, the Parquet writer, the GCS upload.
`Cargo.toml` has zero dependencies and lists the ones those will need, with a
note that adding one is a real decision.

## Commands

**There is no Rust toolchain on this machine.** Build and test in a container:

```bash
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; cargo test'

# a single test
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; cargo test partial_batch_uses_the_full_range_name'

# lint and format, both of which must be clean
docker run --rm -v "$PWD":/src -w /src rust:1-alpine sh -c \
  'apk add --no-cache musl-dev >/dev/null; rustup component add clippy rustfmt >/dev/null;
   cargo clippy --all-targets -- -D warnings && cargo fmt --check'
```

`musl-dev` is needed because the Alpine image builds against musl. `target/` is
gitignored and the container writes it as the host user, so no root-owned
artifacts. If a real toolchain gets installed, plain `cargo test` works —
nothing here depends on the container.

## The one invariant to protect

**Commit after upload, never before.** An offset committed ahead of a durable
write is silent data loss the next time the partition moves. The sequence is:

1. accumulate a **fixed** offset range
2. write Parquet
3. upload to the deterministic object name
4. *then* commit `BatchRange::commit_offset()` (which is `end + 1` — Kafka
   commits the next offset to read)

Crash anywhere before step 4 and the range is re-read and re-uploaded to the
same object name, overwriting. That is the whole design.

### Why boundaries are fixed, and why it is not negotiable

Object names embed their offset range, so a retry overwrites instead of
duplicating. **That only holds if the boundaries are reproducible.** A consumer
that batches "whatever is buffered when the timer fires" assembles `[4711, 9993]`
where a previous attempt had `[4700, 9999]` — two differently-named objects
covering overlapping events. Duplication, not an overwrite, and unfixable after
the fact.

So `start = (offset / range_size) * range_size`, always. A partial trailing
batch is written to the **same name its full range would use**, so completing it
later overwrites in place rather than stranding a duplicate.

This was decided explicitly over the alternative (batch freely, dedup downstream
in the silver worker). Do not "optimise" batching to be timing- or size-driven
without revisiting that decision — it silently breaks staging as a source of
truth.

## Upstream contract

**This service reads Kafka, not Redis.** (An earlier version of this file said
Redis — that is `pulse-conflux`'s job.) The authority is
`../pulse-infra/docs/stack-contract.md`; anything there is a breaking change for
four-plus repos.

- **Topics**: `ingestion-events`, `ingestion-signals`, `ingestion-logs` — 6
  partitions each, RF=3 with `min.insync.replicas=2` on `core`/`full`, RF=1 on
  `lite`. 6 divides by 1/2/3/6 so rebalancing is observable at any group size.
- **Auto-topic-creation is OFF upstream**: a misspelled topic is an error, not
  an empty stream.
- **This service owns its consumer group.** The stack creates none, so group
  creation, the rebalance listener and partition-revocation handling are all
  yours — and the rebalance listener is exactly where the commit-after-upload
  invariant gets violated if you are careless.
- **Sink**: bucket `pulse-staging-local`, path
  `{topic}/dt=YYYY-MM-DD/{partition}-{startoffset}-{endoffset}.parquet`. The
  date is the **write** date in UTC, not an event timestamp.
- The word is **staging**, never "bronze" — a transient landing pad with no
  retention promise.
- **The gateway does not write Kafka at this commit.** Its only producer is
  Redis. Kafka is provisioned for this service and fed by its own tests and
  `pulse-client`; the `gateway → Kafka` edge in the platform diagram is unbuilt.

## Local development

```bash
cd ../pulse-infra && make up PROFILE=core    # Kafka + fake GCS: this service's world
```

From the host, brokers are `localhost:19092,19093,19094`; inside the stack's
Docker network they are `kafka-1:9092,kafka-2:9092,kafka-3:9092`. GCS is
`localhost:4443` / `fake-gcs:4443`, unauthenticated.

**The GCS write path is unverified for auth locally.** The emulator has no IAM,
serves plain HTTP, and returns different error bodies, so scopes, ADC, token
refresh, 401-vs-403-vs-429 handling and TLS are all untested. A green local run
is not a deployable artifact — see `../pulse-infra/docs/divergences.md`.

## Conventions

- Copy `.env.example` to `.env`; both `.env` and `.env.*` are gitignored, with
  `!.env.example` keeping the template committable. Every contract value is
  required with no default — a wrong default looks like it worked.
- `clippy -D warnings` and `cargo fmt --check` are the bar; both are clean now.
- Config parsing lives in free functions rather than methods because the process
  environment is global: tests that mutate it concurrently corrupt each other,
  and the free functions are what make it testable at all.
- `BatchRange` has exactly one constructor (`containing`) and private fields.
  Boundaries are never caller-chosen — that is the point of the type.
