//! pulse-ingestor: consumes events from Kafka and writes Parquet batches to GCS
//! staging.
//!
//! # The correctness property this crate exists to protect
//!
//! **Commit after upload, never before.** An offset committed ahead of a
//! durable write is silent data loss the next time the partition moves. The
//! sequence is: accumulate a fixed offset range → write Parquet → upload to the
//! deterministic object name → *then* commit
//! [`batching::BatchRange::commit_offset`].
//!
//! Crash anywhere before the commit and the range is re-read and re-uploaded to
//! the same object name, which overwrites. That is why boundaries are fixed
//! rather than timing-dependent — see the [`batching`] module docs — and why a
//! *partial* batch uploads without committing, which [`pipeline`] explains.
//!
//! # Shape
//!
//! The layers are split so the rules can be tested without a broker or a
//! bucket:
//!
//! - [`batching`] — fixed offset ranges and the object names derived from them.
//!   Pure.
//! - [`envelope`] — the gateway's wire format, and what an unparseable record
//!   becomes. Pure.
//! - [`parquet_writer`] — the staging file schema. Pure.
//! - [`pipeline`] — accumulate → upload → commit, generic over a [`sink::Sink`]
//!   and a [`pipeline::Committer`]. Drives fakes in its own tests.
//! - [`sink`] — GCS over the JSON API, with the emulator and real GCS as the
//!   same code path plus a different endpoint and token.
//! - [`kafka`] — the consumer, the rebalance callback, and the consume loop.
//!
//! # Local development
//!
//! The stack in `pulse-infra` provisions everything this needs:
//!
//! ```text
//! cd ../pulse-infra && make up PROFILE=core
//! ```
//!
//! That gives a 3-broker Kafka cluster (6 partitions per topic, RF=3,
//! `min.insync.replicas=2`) and an unauthenticated GCS emulator. The multi-broker
//! default is deliberate: a single broker cannot exercise consumer-group
//! rebalancing or ISR behaviour, which is exactly what this service's
//! correctness depends on.
//!
//! **The GCS write path is unverified for auth locally** — the emulator has no
//! IAM and serves plain HTTP, and real credentials are a dev-deployment task
//! (see [`sink::ApplicationDefaultCredentials`]). Consult
//! `pulse-infra/docs/divergences.md` before trusting a green local run.

pub mod batching;
pub mod config;
pub mod envelope;
pub mod kafka;
pub mod parquet_writer;
pub mod pipeline;
pub mod sink;
