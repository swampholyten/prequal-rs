use std::sync::{Arc, atomic::AtomicU32};

use parking_lot::Mutex;

use crate::config::WorkloadDist;

#[derive(Clone)]
struct ServerState {
    rif: Arc<AtomicU32>,
    latency_ring: Arc<Mutex<LatencyRing>>,
    dist: WorkloadDist,
}

/// Ring buffer of (rif_at_arrival, duration_us) for probe latency estimation.
struct LatencyRing {
    entries: Vec<(u32, u64)>,
    head: usize,
    capacity: usize,
}
