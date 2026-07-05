use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProbeResponse {
    pub rif: u32,
    pub latency_us: u64,
    pub cpu_util: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkRequest {
    pub iterations: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkResponse {
    pub duration_us: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PrequalConfig {
    pub pool_capacity: usize,
    pub probe_ttl_ms: u64,
    pub r_probe: usize,
    pub r_remove: usize,
    pub delta: f64,
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
    /// Eq.: b_reuse = max(1, (1+delta) / ((1 - m/n) * (r_probe - r_remove)))
    pub fn reuse_budget(&self, n_replicas: usize) -> u32 {
        let m = self.pool_capacity as f64;
        let n = n_replicas as f64;
        let net = (1.0 - m / n) * (self.r_probe as f64 - self.r_remove as f64);
        if net <= 0.0 {
            return u32::MAX; // pool ever drains from reuse alone
        }
        ((1.0 + self.delta) / net).max(1.0).ceil() as u32
    }
}
