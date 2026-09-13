//! Configuration, read from the environment.
//!
//! Mirrors `.env.example`. Following `pulse-gateway`'s convention: **no silent
//! defaults for anything that is part of the cross-repo contract**, and every
//! missing or malformed variable is collected so startup reports all of them at
//! once instead of one per restart.
//!
//! Tunables that are nobody else's business (batch sizing, log level) do have
//! defaults. Topic names, bucket and broker list do not — a wrong value there
//! is a silent integration failure, and a wrong *default* is worse, because it
//! looks like it worked.

use std::env;
use std::num::NonZeroU64;

/// Everything the ingestor needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Kafka bootstrap servers, comma-separated.
    pub bootstrap_servers: String,
    /// Topics to consume, in contract order: events, signals, logs.
    pub topics: Topics,
    /// Consumer group. This service owns group creation; the stack creates none.
    pub consumer_group: String,
    /// Fixed offset-range width. See [`crate::batching`] for why it is fixed.
    pub batch_offset_range: NonZeroU64,
    /// How long to wait before flushing a partial trailing batch.
    pub batch_max_idle_ms: u64,
    /// GCS staging bucket.
    pub gcs_bucket: String,
    /// `host:port` of a GCS emulator, when running against the local stack.
    /// `None` means talk to real GCS — which is unverified locally, see
    /// `pulse-infra/docs/divergences.md`.
    pub storage_emulator_host: Option<String>,
}

/// The three ingestion topics.
#[derive(Debug, Clone)]
pub struct Topics {
    pub events: String,
    pub signals: String,
    pub logs: String,
}

impl Topics {
    /// All three, for subscribing in one call.
    #[must_use]
    pub fn all(&self) -> [&str; 3] {
        [&self.events, &self.signals, &self.logs]
    }
}

/// Every problem found while loading configuration.
///
/// Deliberately a list: reporting one missing variable per startup attempt
/// wastes an afternoon on a service with a dozen required settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub problems: Vec<String>,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "invalid configuration ({} problem(s)):",
            self.problems.len()
        )?;
        for p in &self.problems {
            writeln!(f, "  - {p}")?;
        }
        write!(f, "see .env.example for the full set")
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Load from the process environment.
    ///
    /// Does not read a `.env` file: that is the binary's job, before calling
    /// this, so tests and containers can supply the environment directly.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut problems = Vec::new();

        let bootstrap_servers = required("KAFKA_BOOTSTRAP_SERVERS", &mut problems);
        let events = required("KAFKA_TOPIC_EVENTS", &mut problems);
        let signals = required("KAFKA_TOPIC_SIGNALS", &mut problems);
        let logs = required("KAFKA_TOPIC_LOGS", &mut problems);
        let consumer_group = required("KAFKA_CONSUMER_GROUP", &mut problems);
        let gcs_bucket = required("GCS_BUCKET", &mut problems);

        let batch_offset_range = parse_non_zero("BATCH_OFFSET_RANGE", 10_000, &mut problems);
        let batch_max_idle_ms = parse_or("BATCH_MAX_IDLE_MS", 30_000, &mut problems);

        let storage_emulator_host = env::var("STORAGE_EMULATOR_HOST")
            .ok()
            .filter(|v| !v.trim().is_empty());

        if !problems.is_empty() {
            return Err(ConfigError { problems });
        }

        Ok(Self {
            bootstrap_servers: bootstrap_servers.unwrap_or_default(),
            topics: Topics {
                events: events.unwrap_or_default(),
                signals: signals.unwrap_or_default(),
                logs: logs.unwrap_or_default(),
            },
            consumer_group: consumer_group.unwrap_or_default(),
            batch_offset_range,
            batch_max_idle_ms,
            gcs_bucket: gcs_bucket.unwrap_or_default(),
            storage_emulator_host,
        })
    }
}

fn required(key: &str, problems: &mut Vec<String>) -> Option<String> {
    match env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        Ok(_) => {
            problems.push(format!("{key} is set but empty"));
            None
        }
        Err(_) => {
            problems.push(format!("{key} is required and not set"));
            None
        }
    }
}

fn parse_or<T>(key: &str, default: T, problems: &mut Vec<String>) -> T
where
    T: std::str::FromStr + Copy,
{
    match env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.trim().parse::<T>() {
            Ok(v) => v,
            Err(_) => {
                problems.push(format!("{key}={raw:?} is not a valid number"));
                default
            }
        },
    }
}

fn parse_non_zero(key: &str, default: u64, problems: &mut Vec<String>) -> NonZeroU64 {
    let fallback = NonZeroU64::new(default).expect("caller-supplied default must be non-zero");
    match env::var(key) {
        Err(_) => fallback,
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => {
                problems.push(format!(
                    "{key} must be greater than 0 (offset ranges cannot be empty)"
                ));
                fallback
            }
            Ok(v) => NonZeroU64::new(v).unwrap_or(fallback),
            Err(_) => {
                problems.push(format!("{key}={raw:?} is not a valid number"));
                fallback
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests exercise the pure helpers rather than Config::from_env,
    // because the process environment is global: two tests mutating it run
    // concurrently and corrupt each other. Keeping the parsing logic in free
    // functions is what makes it testable at all.

    #[test]
    fn missing_required_key_is_reported_by_name() {
        let mut problems = Vec::new();
        let got = required("PULSE_TEST_DEFINITELY_UNSET_KEY", &mut problems);
        assert!(got.is_none());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("PULSE_TEST_DEFINITELY_UNSET_KEY"));
        assert!(problems[0].contains("required"));
    }

    #[test]
    fn unparsable_number_is_reported_and_falls_back() {
        let mut problems = Vec::new();
        // SAFETY: single-threaded test setting a key no other test touches.
        env::set_var("PULSE_TEST_BAD_NUMBER", "not-a-number");
        let got: u64 = parse_or("PULSE_TEST_BAD_NUMBER", 42, &mut problems);
        env::remove_var("PULSE_TEST_BAD_NUMBER");

        assert_eq!(
            got, 42,
            "falls back so the error list stays the failure path"
        );
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("not a valid number"));
    }

    #[test]
    fn absent_optional_number_uses_its_default_silently() {
        let mut problems = Vec::new();
        let got: u64 = parse_or("PULSE_TEST_ANOTHER_UNSET_KEY", 7, &mut problems);
        assert_eq!(got, 7);
        assert!(problems.is_empty(), "a default is not a problem");
    }

    #[test]
    fn zero_offset_range_is_rejected() {
        let mut problems = Vec::new();
        env::set_var("PULSE_TEST_ZERO_RANGE", "0");
        let got = parse_non_zero("PULSE_TEST_ZERO_RANGE", 10_000, &mut problems);
        env::remove_var("PULSE_TEST_ZERO_RANGE");

        assert_eq!(got.get(), 10_000);
        assert_eq!(problems.len(), 1);
        assert!(
            problems[0].contains("greater than 0"),
            "a zero range would divide by zero and name every object identically"
        );
    }

    #[test]
    fn all_problems_are_collected_not_just_the_first() {
        let mut problems = Vec::new();
        let _ = required("PULSE_TEST_UNSET_ONE", &mut problems);
        let _ = required("PULSE_TEST_UNSET_TWO", &mut problems);
        let _ = required("PULSE_TEST_UNSET_THREE", &mut problems);
        assert_eq!(
            problems.len(),
            3,
            "one restart per missing var is not a workflow"
        );
    }

    #[test]
    fn error_display_lists_every_problem() {
        let err = ConfigError {
            problems: vec!["A is required".into(), "B is required".into()],
        };
        let text = err.to_string();
        assert!(text.contains("2 problem(s)"));
        assert!(text.contains("A is required"));
        assert!(text.contains("B is required"));
    }

    #[test]
    fn topics_subscribe_in_contract_order() {
        let t = Topics {
            events: "ingestion-events".into(),
            signals: "ingestion-signals".into(),
            logs: "ingestion-logs".into(),
        };
        assert_eq!(
            t.all(),
            ["ingestion-events", "ingestion-signals", "ingestion-logs"]
        );
    }
}
