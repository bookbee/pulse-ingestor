//! Binary entry point: wire the layers together and run until told to stop.

use std::time::Duration;

use anyhow::Context as _;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use pulse_ingestor::config::Config;
use pulse_ingestor::kafka::{self, KafkaCommitter};
use pulse_ingestor::pipeline::{Pipeline, RetryPolicy};
use pulse_ingestor::sink::{GcsSink, Sink as _};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `.env` is the binary's business, not the config module's, so tests and
    // containers can supply the environment directly.
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            // Every problem at once, not one per restart.
            error!("{e}");
            std::process::exit(1);
        }
    };

    let sink = GcsSink::new(&config.gcs_bucket, config.storage_emulator_host.as_deref())
        .context("building the GCS sink")?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        kafka = %config.bootstrap_servers,
        group = %config.consumer_group,
        topics = ?config.topics.all(),
        sink = %sink.describe(),
        batch_offset_range = config.batch_offset_range.get(),
        idle_flush_ms = config.batch_max_idle_ms,
        "starting pulse-ingestor"
    );
    if config.storage_emulator_host.is_some() {
        info!(
            "GCS emulator in use — no IAM, plain HTTP; auth is unverified until the dev deployment"
        );
    }

    let (consumer, revocations) = kafka::build_consumer(&config).context("connecting to Kafka")?;
    let committer = KafkaCommitter::new(std::sync::Arc::clone(&consumer));

    let mut pipeline = Pipeline::new(
        sink,
        committer,
        config.batch_offset_range,
        Duration::from_millis(config.batch_max_idle_ms),
        RetryPolicy::default(),
    );

    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        wait_for_signal().await;
        let _ = tx.send(true);
    });

    // The idle timer is checked more often than it fires, so a partition that
    // goes quiet is noticed promptly without spinning.
    let idle_check = Duration::from_millis((config.batch_max_idle_ms / 4).max(250));

    kafka::run(consumer, revocations, &mut pipeline, idle_check, rx).await?;

    info!("exited cleanly");
    Ok(())
}

/// Ctrl-C, or SIGTERM from a container runtime.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "cannot listen for SIGTERM; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
