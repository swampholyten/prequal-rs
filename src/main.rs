use crate::cli::{Cli, Command};
use clap::Parser;

mod cli;
mod client;
mod config;
mod metrics;
mod servers;

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
