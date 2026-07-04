mod config;
mod metrics;
mod servers;
mod client;
mod cli;


#[tokio::main]
async fn main() {

    tracing_subscriber::fmt::init();
    println!("Hello, world!");
}
