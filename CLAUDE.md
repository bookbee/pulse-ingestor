# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Feature-complete for the local stack. **Language decided 2026-09-13: Rust**
(not Java). The full path runs: consume → batch → Parquet → GCS → commit.

| Module | Role |
|---|---|
| `batching.rs` | Fixed offset-range boundaries and the object names from them. Pure. |
| `config.rs` | Contract values, no silent defaults, all problems reported at once. Pure. |
| `envelope.rs` | The gateway's wire format; unparseable records become values, not errors. Pure. |
| `parquet_writer.rs` | The staging file schema. Pure. |
| `pipeline.rs` | Accumulate → upload → commit, generic over a `Sink` and a `Committer`. |
| `sink.rs` | GCS over the JSON API; emulator and real GCS differ only in endpoint and token. |
| `kafka.rs` | Consumer, rebalance callback, consume loop, commits. |

59 unit tests plus 2 integration tests that run against the real stack. Verified
end to end on 2026-09-14: 12 records consumed from `ingestion-events`, two
complete ranges uploaded and committed, one partial range uploaded and *not*
committed.

**Still open**: real GCS auth (`sink::ApplicationDefaultCredentials` is a
deliberate `unimplemented`-by-error, scheduled for the dev deployment), and the
`kafka-tls` feature for the SASL/TLS brokers production uses.

## Commands

Everything is a `make` target. **There is no Rust toolchain on this machine**, so
by default each one runs in a throwaway `rust:1-bookworm` container; if `cargo`
appears on PATH the same targets use it directly instead. `make where` reports
which, and `make help` lists them all.

```bash
make infra-up                      # shared stack (PROFILE=core|full|lite)
cp .env.example .env               # required by the run targets
make run                           # run the service
make test                          # 59 unit tests; no stack, no network
make test-one NAME=a_partial_batch # one test by substring
make test-integration              # 2 tests; REQUIRES the stack
make check                         # fmt-check + lint + test
make image && make run-image       # production image, run against the stack
```

Three things about the container plumbing are deliberate:

- **Three cached volumes** — registry, rustup toolchain, target dir. The rustup
  one exists because the rust image ships *no* clippy or rustfmt; without it,
  `make lint` re-downloads them every run. `CARGO_TARGET_DIR` points away from
  the bind mount so container artifacts never collide with the host.
- **`--network pulse-infra` only where it is needed.** `make test` must work
  with the stack down, so only the run and integration targets join the network.
- **In-network addresses are injected as `-e` overrides.** `.env` holds *host*
  addresses (`localhost:19092`); inside the network they must be `kafka-1:9092`
  / `fake-gcs:4443`. `dotenvy` does not override already-set variables, so the
  overrides win over the bind-mounted `.env`. A containerised run without them
  dials `localhost:19092` inside its own container and fails looking exactly
  like a dead broker.

The `Dockerfile` is multi-stage: deps build against dummy sources first so that
layer survives ordinary source edits, then a 117MB `debian:bookworm-slim`
runtime, non-root, no `EXPOSE` and no `HEALTHCHECK` — this service serves no HTTP
surface, and liveness here is consumer lag, which belongs in monitoring rather
than a container probe.

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

### The partial-batch rule, which is where this actually gets broken

A partial flush **uploads but commits nothing, and keeps its records.** Both
halves are load-bearing, and both look like dead weight to anyone tidying up:

- **Why no commit.** A range of 10 000 holding offsets `0..=12` still has
  `commit_offset() == 10000`. Committing that acknowledges 9 987 offsets nobody
  read. So a partial flush commits nothing, and the range's offsets stay
  uncommitted until it completes.
- **Why keep the records.** If the partial flush cleared its buffer, the next
  upload of that range would contain only `13..=9999` and would overwrite — and
  destroy — the object holding `0..=12`. Every upload of a range must be a
  superset of the last.

The cost is bounded on purpose: an idle partition holds at most one range in
memory and re-uploads it as it grows, and a restart re-reads from the range
start. That trades a bounded memory cost for the elimination of an unbounded
data-loss one.

`pipeline.rs` states these as rules 1–3 and tests each; the integration test
`a_partial_batch_is_not_committed_and_is_overwritten_when_the_range_completes`
proves the whole cycle against a real broker. If you change flush behaviour and
that test still passes, look again — it is the only test that would catch this.

### Settings that are deliberately not configurable

Three things a `.env` could plausibly expose and must not:
`enable.auto.commit`, `auto.offset.reset` (both forced in `kafka.rs`) and the
object-path template (hardcoded in `ObjectName::for_range`). Auto-commit runs on
a timer that knows nothing about the upload; a runtime path template would let a
config edit rename a range that was already written, turning the next retry into
a duplicate instead of an overwrite. `.env.example` documents each omission.

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
- **The gateway's Kafka producer is in development** (`pulse-gateway`, specced
  as D8–D13; a `queue.Producer` interface exists but there is no Kafka client
  dependency yet). Until it lands, its only live producer is Redis and these
  topics are fed by this service's own tests and `pulse-client` — so an empty
  topic locally is expected, not a bug. Re-check the gateway before assuming a
  consume-loop problem.

## Local development

```bash
cd ../pulse-infra && make up PROFILE=core    # Kafka + fake GCS: this service's world
```

From the host, brokers are `localhost:19092,19093,19094`; inside the stack's
Docker network they are `kafka-1:9092,kafka-2:9092,kafka-3:9092`. GCS is
`localhost:4443` / `fake-gcs:4443`, unauthenticated.

**Develop against the emulator; GCS auth is validated at dev deployment.** That
is a deliberate decision, not an oversight. The emulator has no IAM, serves
plain HTTP, and returns different error bodies, so scopes, ADC, token refresh,
401-vs-403-vs-429 handling and TLS get their first real exercise in the dev
environment — see `../pulse-infra/docs/divergences.md`. Build the upload path so
the endpoint and credentials are configuration (`STORAGE_EMULATOR_HOST` already
is), and keep auth/retry handling separable from batching so dev-deploy findings
land in one place. A green local run still says nothing about auth.

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
