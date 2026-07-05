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

impl Probe {
    /// Latency estimate including our own dispatches since the probe:
    /// processor-sharing scaling, mirroring the server-side estimator.
    pub fn adjusted_latency(&self) -> u64 {
        self.latency_us * (self.rif as u64 + 1) / (self.rif_at_probe as u64 + 1)
    }
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

impl ProbePool {
    pub fn new(capacity: usize, ttl: Duration, reuse_budget: u32, q_rif: f64) -> Self {
        Self {
            probes: Vec::with_capacity(capacity),
            capacity,
            ttl,
            reuse_budget,
            q_rif,
            remove_oldest_next: false,
        }
    }

    pub fn len(&self) -> usize {
        self.probes.len()
    }

    /// Debug view: (replica, rif, latency_us, age_ms) for every probe.
    pub fn snapshot(&self, now: Instant) -> Vec<(usize, u32, u64, u64)> {
        self.probes
            .iter()
            .map(|p| {
                (
                    p.replica,
                    p.rif,
                    p.latency_us,
                    now.duration_since(p.received_at).as_millis() as u64,
                )
            })
            .collect()
    }

    pub fn insert(&mut self, probe: Probe) {
        if self.probes.len() >= self.capacity {
            self.remove_oldest();
        }
        self.probes.push(probe);
    }

    /// Drop probes past their TTL or reuse budget.
    pub fn expire(&mut self, now: Instant) {
        let ttl = self.ttl;
        let budget = self.reuse_budget;
        self.probes
            .retain(|p| now.duration_since(p.received_at) < ttl && p.uses < budget);
    }

    /// RIF value at the Q_RIF quantile of the estimated RIF distribution
    /// *across replicas* (latest probe per replica); probes strictly above
    /// it are "hot". Deduplication matters when the pool holds several
    /// probes of the same replica: an overloaded replica's own copies would
    /// otherwise drag the quantile up to its own RIF and it could never
    /// classify as hot. Returns None on an empty pool.
    fn hot_threshold(&self) -> Option<u32> {
        let mut latest: std::collections::HashMap<usize, (Instant, u32)> = Default::default();
        for p in &self.probes {
            let e = latest.entry(p.replica).or_insert((p.received_at, p.rif));
            if p.received_at >= e.0 {
                *e = (p.received_at, p.rif);
            }
        }
        if latest.is_empty() {
            return None;
        }
        let mut rifs: Vec<u32> = latest.into_values().map(|(_, rif)| rif).collect();
        rifs.sort_unstable();
        let idx = (self.q_rif * (rifs.len() - 1) as f64).floor() as usize;
        Some(rifs[idx])
    }

    /// Hot-Cold Lexicographic rule: if every probe is hot, take the lowest RIF;
    /// otherwise take the cold probe with the
    /// lowest latency. Bumps the chosen probe's use count and RIF (the
    /// client compensates for its own query).
    pub fn hcl_select(&mut self) -> Option<usize> {
        let threshold = self.hot_threshold()?;
        let cold_best = self
            .probes
            .iter()
            .enumerate()
            .filter(|(_, p)| p.rif <= threshold)
            .min_by_key(|(_, p)| p.adjusted_latency())
            .map(|(i, _)| i);
        let idx = match cold_best {
            Some(i) => i,
            None => self
                .probes
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.rif)
                .map(|(i, _)| i)?,
        };
        self.probes[idx].uses += 1;
        let replica = self.probes[idx].replica;
        // Compensate for our own dispatch on every copy of this replica,
        // not just the chosen probe, or the stale copies stay attractive.
        for p in self.probes.iter_mut().filter(|p| p.replica == replica) {
            p.rif += 1;
        }
        Some(replica)
    }

    /// Per-query removal (rate r_remove), alternating between the oldest
    /// probe and the worst one — worst being the highest-RIF probe if any
    /// probe is hot, else the highest-latency cold probe.
    pub fn remove_one(&mut self) {
        if self.probes.is_empty() {
            return;
        }
        self.remove_oldest_next = !self.remove_oldest_next;
        if self.remove_oldest_next {
            self.remove_oldest();
        } else {
            let threshold = self.hot_threshold().unwrap();
            let any_hot = self.probes.iter().any(|p| p.rif > threshold);
            let idx = if any_hot {
                self.probes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, p)| p.rif)
                    .map(|(i, _)| i)
            } else {
                self.probes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, p)| p.adjusted_latency())
                    .map(|(i, _)| i)
            };
            if let Some(i) = idx {
                self.probes.swap_remove(i);
            }
        }
    }

    fn remove_oldest(&mut self) {
        if let Some(i) = self
            .probes
            .iter()
            .enumerate()
            .min_by_key(|(_, p)| p.received_at)
            .map(|(i, _)| i)
        {
            self.probes.swap_remove(i);
        }
    }
}
