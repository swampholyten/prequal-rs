use clap::Args;
use parking_lot::Mutex;
use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Instant,
};

#[derive(Args, Debug)]
pub struct ServerArgs {
    #[arg(long, default_value_t = 8000)]
    pub port: u16,

    /// CPU allocation for this replica in cores.
    #[arg(long, default_value_t = 1.0)]
    pub cpu_alloc: f64,

    /// Antagonist burn target in % of one core (0 = no antagonist).
    #[arg(long, default_value_t = 0)]
    pub antagonist_cpu: u8,

    /// Antagonist square-wave period in seconds (0 = constant burn).
    #[arg(long, default_value_t = 0)]
    pub antagonist_period_s: u64,

    /// Phase offset of the square wave, so replicas spike at different times.
    #[arg(long, default_value_t = 0)]
    pub antagonist_phase_s: u64,
}

#[derive(Clone)]
struct ServerState {
    rif: Arc<AtomicU32>,
    latency_ring: Arc<Mutex<LatencyRing>>,
    cpu: Arc<Mutex<CpuTracker>>,
}

/// Ring buffer of recently completed queries for probe latency estimation.
struct LatencyRing {
    entries: Vec<RingEntry>,
    head: usize,
    capacity: usize,

    /// Estimates use only samples this recently. Older samples predate antagonist swings.
    max_age: std::time::Duration,
}

#[derive(Clone, Copy)]
struct RingEntry {
    rif_at_arrival: u32,
    duration_us: u64,
    finished_at: Instant,
}

// Process CPU time (user+sys) via getrusage.
// Includes the in-process antagonist, like machine CPU utilization in the paper's setting.
struct CpuTracker {
    last_cpu_us: u64,
    last_wall: Instant,
    alloc: f64,
    util_ema: f64,
}
