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
/// removal alternating between oldest and worst.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(replica: usize, rif: u32, latency_us: u64) -> Probe {
        Probe {
            replica,
            rif,
            rif_at_probe: rif,
            latency_us,
            received_at: Instant::now(),
            uses: 0,
        }
    }

    fn pool_with(probes: Vec<Probe>, q_rif: f64) -> ProbePool {
        let mut pool = ProbePool::new(16, Duration::from_secs(10), 100, q_rif);
        for p in probes {
            pool.insert(p);
        }
        pool
    }

    #[test]
    fn hcl_picks_lowest_latency_cold_replica() {
        // rifs [1,2,3,10], q=0.84 -> threshold = 3rd smallest = 3.
        // Replica 3 (rif 10) is hot; among cold, replica 1 has lowest latency.
        let mut pool = pool_with(
            vec![
                probe(0, 1, 9_000),
                probe(1, 2, 4_000),
                probe(2, 3, 8_000),
                probe(3, 10, 1_000),
            ],
            0.84,
        );
        assert_eq!(pool.hcl_select(), Some(1));
    }

    #[test]
    fn q_rif_zero_is_rif_only_control() {
        // threshold = min RIF, so only min-RIF probes are cold; latency of
        // hotter replicas must not matter.
        let mut pool = pool_with(
            vec![probe(0, 3, 1_000), probe(1, 5, 10), probe(2, 7, 10)],
            0.0,
        );
        assert_eq!(pool.hcl_select(), Some(0));
    }

    #[test]
    fn q_rif_one_is_latency_only_control() {
        // threshold = max RIF, everything cold: pure latency choice.
        let mut pool = pool_with(
            vec![probe(0, 30, 5_000), probe(1, 1, 9_000), probe(2, 2, 7_000)],
            1.0,
        );
        assert_eq!(pool.hcl_select(), Some(0));
    }

    #[test]
    fn selection_compensates_rif_on_all_copies() {
        let mut pool = pool_with(vec![probe(0, 0, 1_000), probe(0, 0, 2_000)], 0.84);
        pool.hcl_select();
        assert!(pool.snapshot(Instant::now()).iter().all(|&(_, rif, _, _)| rif == 1));
    }

    #[test]
    fn expire_enforces_reuse_budget() {
        let mut pool = ProbePool::new(16, Duration::from_secs(10), 2, 0.84);
        pool.insert(probe(0, 0, 1_000));
        pool.insert(probe(1, 5, 1_000));
        pool.hcl_select(); // replica 0, uses -> 1
        pool.hcl_select(); // replica 0 again (still coldest), uses -> 2
        pool.expire(Instant::now());
        assert_eq!(pool.len(), 1); // replica 0's probe hit the budget
    }

    #[test]
    fn insert_evicts_oldest_at_capacity() {
        let mut pool = ProbePool::new(2, Duration::from_secs(10), 100, 0.84);
        let old = probe(0, 0, 1_000);
        std::thread::sleep(Duration::from_millis(2));
        pool.insert(probe(1, 1, 1_000));
        pool.insert(old); // oldest by received_at even though inserted last
        std::thread::sleep(Duration::from_millis(2));
        pool.insert(probe(2, 2, 1_000));
        let replicas: Vec<usize> = pool.snapshot(Instant::now()).iter().map(|s| s.0).collect();
        assert_eq!(pool.len(), 2);
        assert!(!replicas.contains(&0));
    }

    #[test]
    fn removal_alternates_oldest_then_worst() {
        let mut pool = pool_with(
            vec![probe(0, 1, 1_000), probe(1, 2, 2_000), probe(2, 9, 3_000)],
            0.84,
        );
        // First removal: oldest (replica 0, inserted first).
        pool.remove_one();
        let replicas: Vec<usize> = pool.snapshot(Instant::now()).iter().map(|s| s.0).collect();
        assert!(!replicas.contains(&0));
        // Second removal: worst. Threshold over [2,9] at q=0.84 is 2, so
        // replica 2 (rif 9) is hot and must go as highest-RIF.
        pool.remove_one();
        let replicas: Vec<usize> = pool.snapshot(Instant::now()).iter().map(|s| s.0).collect();
        assert!(!replicas.contains(&2));
    }
}
