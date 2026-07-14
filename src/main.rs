//! Reproduction of *"Load is not what you should balance: Introducing
//! Prequal"* (Wydrowski et al., NSDI 2024) — a load balancer that picks
//! replicas by asynchronously probed requests-in-flight (RIF) and latency
//! instead of averaged CPU utilization.
//!
//! # Entry point and flow
//!
//! [`main`] parses the CLI ([`cli::Cli`]) and dispatches to one of two roles:
//!
//! - **`server`** → [`servers::replica::run`]: an axum replica exposing
//!   `POST /work` (CPU-bound hashing) and `GET /probe` (load signals: RIF,
//!   latency estimate, CPU utilization). It can host an in-process
//!   [`servers::antagonist`] that burns CPU to create the paper's
//!   time-varying interference.
//! - **`client`** → [`client::run`]: an open-loop Poisson load generator.
//!   Each query asks a [`client::policy::Balancer`] to pick a replica; the
//!   `prequal` policy keeps a [`client::pool::ProbePool`] of async probe
//!   responses and applies the Hot-Cold Lexicographic (HCL) selection rule.
//!
//! Query outcomes are aggregated by [`metrics::collector::MetricsCollector`]
//! and printed as a JSON summary at the end of the run. Wire types shared by
//! both roles and the Prequal tuning parameters live in [`config`].

use crate::cli::{Cli, Command};
use clap::Parser;

mod cli;
mod client;
mod config;
mod metrics;
mod servers;

/// Process entry point: initializes stderr logging (filtered by `RUST_LOG`,
/// default `info`), parses the CLI, and runs the chosen role to completion.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Server(args) => servers::replica::run(args).await,
        Command::Client(args) => client::run(args).await,
    }
}
