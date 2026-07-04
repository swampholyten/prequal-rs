use std::time::{Duration, Instant};

/// One probe response held in the pool
#[derive(Debug, Clone, Copy)]
pub struct Probe {
    pub replica: usize,
    /// RIF as reported, plus one per query we dispatched there since.
    pub rif: u32,
    /// RIF as reported by the probe, for latency rescaling.
    pub rif_at_probe: u32,
    pub latency_us: u64,
    pub received_at: Instant,
    pub uses: u32,
}

/// Bounded pool of probe responses with the paper's removal machinery:
/// TTL expiry, reuse budget, oldest-eviction on overflow, and per-query
/// removal alternating between olderst and worst.
pub struct ProbePool {
    probes: Vec<Probe>,
    capacity: usize,
    ttl: Duration,
    reuse_budget: u32,
    q_rif: f64,
    remove_oldest_next: bool,
}
