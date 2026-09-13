//! Binary entry point.
//!
//! Loads and validates configuration, reports what it resolved, and stops —
//! the consume loop is not written yet. It deliberately exits non-zero rather
//! than idling, so nothing mistakes this for a running ingestor.

use std::process::ExitCode;

use pulse_ingestor::batching::{BatchRange, ObjectName};
use pulse_ingestor::config::Config;

fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    println!("pulse-ingestor {}", env!("CARGO_PKG_VERSION"));
    println!("  kafka:          {}", config.bootstrap_servers);
    println!("  topics:         {}", config.topics.all().join(", "));
    println!("  consumer group: {}", config.consumer_group);
    println!("  bucket:         {}", config.gcs_bucket);
    match &config.storage_emulator_host {
        Some(host) => println!("  gcs endpoint:   {host} (emulator — no IAM, plain HTTP)"),
        None => println!("  gcs endpoint:   real GCS"),
    }
    println!("  batch range:    {} offsets", config.batch_offset_range);
    println!("  idle flush:     {} ms", config.batch_max_idle_ms);

    // Show the naming scheme concretely: this is the contract other repos read.
    let example = BatchRange::containing(0, config.batch_offset_range);
    println!(
        "\n  first object for partition 0 would be:\n    gs://{}/{}",
        config.gcs_bucket,
        ObjectName::for_range(&config.topics.events, 0, "YYYY-MM-DD", example)
    );
    println!(
        "    covering offsets {}..={}, committing {} after upload",
        example.start(),
        example.end(),
        example.commit_offset()
    );

    eprintln!("\nconsume loop not implemented yet — exiting");
    ExitCode::FAILURE
}
