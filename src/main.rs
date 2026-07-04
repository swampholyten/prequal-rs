mod config;
mod metrics;
mod servers;

#[tokio::main]
async fn main() {

    tracing_subscriber::fmt::init();
    println!("Hello, world!");
}
