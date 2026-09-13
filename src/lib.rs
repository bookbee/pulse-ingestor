//! pulse-ingestor: consumes events from Kafka and writes Parquet batches to GCS
//! staging.
//!
//! # Status
//!
//! Early. What exists is the part that cannot be changed later without a data
//! migration:
//!
//! - [`batching`] — fixed offset-range boundaries and the object names derived
//!   from them. This is where the idempotency guarantee lives or dies.
//! - [`config`] — the cross-repo contract values, loaded with no silent
//!   defaults.
//!
//! Not yet written: the Kafka consume loop, the Parquet writer, and the GCS
//! upload. See `Cargo.toml` for the dependencies those will pull in.
//!
//! # The correctness property this crate exists to protect
//!
//! **Commit after upload, never before.** An offset committed ahead of a
//! durable write is silent data loss the next time the partition moves. The
//! sequence is: accumulate a fixed offset range → write Parquet → upload to the
//! deterministic object name → *then* commit [`batching::BatchRange::commit_offset`].
//!
//! Crash anywhere before the commit and the range is re-read and re-uploaded to
//! the same object name, which overwrites. That is why boundaries are fixed
//! rather than timing-dependent — see the [`batching`] module docs.
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
//! IAM and serves plain HTTP. See `pulse-infra/docs/divergences.md` before
//! trusting a green local run.

pub mod batching;
pub mod config;
