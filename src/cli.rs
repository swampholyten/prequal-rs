use clap::{Parser, Subcommand};

use crate::{client, servers};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server(servers::replica::ServerArgs),
    Client(client::ClientArgs),
}
