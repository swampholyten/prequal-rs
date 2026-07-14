//! Wire types shared between client and server, and the Prequal tuning
//! parameters with the paper's derived reuse budget.

use serde::{Deserialize, Serialize};

/// Reply to `GET /probe`, produced by the `probe` handler in
/// [`crate::servers::replica`] and consumed by every probing policy in
/// [`crate::client::policy`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProbeResponse {
    /// Requests in flight: queries accepted but not yet finished, including
    /// those still queued for a worker slot. Prequal's primary load signal.
    pub rif: u32,
    /// Estimated latency (µs) a query arriving at the current RIF would see,
    /// from the replica's RIF-indexed median of recent completions.
    pub latency_us: u64,
    /// Smoothed CPU utilization relative to the replica's allocation.
    /// Used only by the WRR incumbent policy.
    pub cpu_util: f64,
}

/// Body of `POST /work`: one query, sent by [`crate::client::run`] and
/// executed by the `work` handler in [`crate::servers::replica`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkRequest {
    /// Hash iterations to spin; drawn per query from a truncated normal
    /// distribution on the client, scaled by the replica's `work_factor`.
    pub iterations: u64,
}

/// Reply to `POST /work`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkResponse {
    /// Server-side wall time (µs) from arrival to completion, queueing included.
    pub duration_us: u64,
}

/// Prequal parameters (§4 of the paper). Built from [`crate::client::ClientArgs`]
/// and passed to [`crate::client::policy::Balancer::new`], which sizes the
/// [`crate::client::pool::ProbePool`] from it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PrequalConfig {
    /// Maximum probes held in the pool; the oldest is evicted on overflow.
    pub pool_capacity: usize,
    /// Probe lifetime (ms): older probes are dropped on every selection.
    pub probe_ttl_ms: u64,
    /// Probes fired (asynchronously, to distinct random replicas) per query.
    pub r_probe: usize,
    /// Probes removed from the pool per query, alternating oldest/worst.
    pub r_remove: usize,
    /// Safety margin in the reuse-budget formula (Eq. 1); paper default 1.
    pub delta: f64,
    /// Hot/cold RIF quantile: probes above the pool's `q_rif` RIF quantile
    /// are "hot". 0 degenerates to RIF-only, 1 to latency-only (§5.2).
    pub q_rif: f64,
}

impl Default for PrequalConfig {
    fn default() -> Self {
        Self {
            pool_capacity: 16,
            probe_ttl_ms: 1000,
            r_probe: 3,
            r_remove: 1,
            delta: 1.0,
            q_rif: 2_f64.powf(-0.25),
        }
    }
}

impl PrequalConfig {
    /// Maximum times one probe may be reused before
    /// [`crate::client::pool::ProbePool::expire`] drops it.
    ///
    /// Paper Eq. (1): b_reuse = max(1, (1+delta) / ((1 - m/n) * r_probe - r_remove)).
    /// (1 - m/n) * r_probe is the rate probes grow the pool (a probe of a
    /// replica already present replaces it), r_remove the per-query removals.
    pub fn reuse_budget(&self, n_replicas: usize) -> u32 {
        let m = self.pool_capacity as f64;
        let n = n_replicas as f64;
        let net = (1.0 - m / n) * self.r_probe as f64 - self.r_remove as f64;
        if net <= 0.0 {
            return u32::MAX; // pool never drains from reuse alone
        }
        ((1.0 + self.delta) / net).max(1.0).ceil() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_budget_matches_paper_defaults() {
        // Paper testbed: m=16, n=100, r_probe=3, r_remove=1, delta=1.
        // net = (1 - 0.16)*3 - 1 = 1.52; b = ceil(2/1.52) = 2.
        let cfg = PrequalConfig::default();
        assert_eq!(cfg.reuse_budget(100), 2);
    }

    #[test]
    fn reuse_budget_unbounded_when_pool_cannot_accumulate() {
        // m = n: every probe hits a replica already pooled, net <= 0.
        let cfg = PrequalConfig {
            pool_capacity: 100,
            ..Default::default()
        };
        assert_eq!(cfg.reuse_budget(100), u32::MAX);
    }

    #[test]
    fn reuse_budget_small_testbed() {
        // m=3, n=6: net = 0.5*3 - 1 = 0.5; b = ceil(2/0.5) = 4.
        let cfg = PrequalConfig {
            pool_capacity: 3,
            ..Default::default()
        };
        assert_eq!(cfg.reuse_budget(6), 4);
    }
}
