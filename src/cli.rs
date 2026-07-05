use clap::{Parser, Subcommand};

use crate::{client, servers};

#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Run a backend replica (work + probe endpoints, optional antagonist).
    Server(servers::replica::ServerArgs),
    /// Run the load-generating client with a chosen balancing policy.
    Client(client::ClientArgs),
}
