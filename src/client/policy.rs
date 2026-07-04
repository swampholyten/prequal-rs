use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use parking_lot::Mutex;

use crate::{client::pool::ProbePool, config::PrequalConfig};

pub enum Policy {
    Random,
    RoundRobin(AtomicUsize),
    /// Classic power-of-d-choices with synchronous probes, choosing by RIF.
    Po2,
    /// CPU-based weighted random: weights refreshed in the background from
    /// each relica's smoothed CPU utilization (the paper's WRR incumbent).
    Wrr {
        weights: Arc<Mutex<Vec<f64>>>,
    },
    Prequal {
        pool: Arc<Mutex<ProbePool>>,
        cfg: PrequalConfig,
    },
}

pub struct Balancer {
    pub policy: Policy,
    pub urls: Arc<Vec<String>>,
    pub http: reqwest::Client,
    pub probe_timeout: Duration,
}
