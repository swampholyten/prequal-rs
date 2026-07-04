pub mod policy;
pub mod pool;

use clap::Args;

#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Comma-separated replica base URLs, e.g. http://r1:8000,http://r2:8000
    #[arg(long, value_delimiter = ',')]
    pub servers: Vec<String>,
    /// Balancing policy: prequal | random | round-robin | po2 | wrr
    #[arg(long, default_value = "prequal")]
    pub policy: String,
    /// Offered load: open-loop Poisson arrivals per second.
    #[arg(long, default_value_t = 100.0)]
    pub qps: f64,
    #[arg(long, default_value_t = 60)]
    pub duration_s: u64,
    /// Mean hash iterations per query; per-query cost is Normal with
    /// std = mean, truncated at zero (the paper's testbed workload).
    #[arg(long, default_value_t = 2_000_000)]
    pub mean_iterations: u64,
    /// Per-query timeout; timeouts count as errors.
    #[arg(long, default_value_t = 2000)]
    pub timeout_ms: u64,
    /// Number of independent balancer instances (separate probe pools).
    /// The paper's testbed has 100 client replicas; a single shared pool
    /// makes every query herd onto the same "best" replica.
    #[arg(long, default_value_t = 6)]
    pub balancers: usize,
    #[arg(long, default_value_t = 3)]
    pub r_probe: usize,
    /// Paper default is 16 with n=100 replicas; the pool must stay well
    /// below the replica count so each balancer sees a random subset —
    /// that is what decorrelates clients and prevents herding. With our
    /// 6-replica testbed, 4 is the equivalent setting.
    #[arg(long, default_value_t = 4)]
    pub pool_capacity: usize,
    #[arg(long, default_value_t = 1000)]
    pub probe_ttl_ms: u64,
    /// Hot/cold RIF quantile; 0 = RIF-only control (§5.2).
    #[arg(long, default_value_t = 2_f64.powf(-0.25))]
    pub q_rif: f64,
}
