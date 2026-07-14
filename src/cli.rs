//! Command-line interface: one binary, two roles (`server` and `client`),
//! parsed in [`crate::main`] and dispatched to the matching `run` function.

use clap::{Parser, Subcommand};

use crate::{client, servers};

/// Top-level argument parser.
#[derive(Parser)]
pub struct Cli {
    /// The role this process runs.
    #[command(subcommand)]
    pub command: Command,
}

/// The two process roles.
#[derive(Subcommand)]
pub enum Command {
    /// Run a backend replica (work + probe endpoints, optional antagonist).
    Server(servers::replica::ServerArgs),
    /// Run the load-generating client with a chosen balancing policy.
    Client(client::ClientArgs),
}
