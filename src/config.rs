use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub enum WorkloadDist {
    #[default]
    Normal,
    Pareto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub server_addresses: Vec<String>,
    pub policy: String,
    pub probe_pool_size: usize,
    pub probe_ttl_ms: u64,
    pub r_probe: usize,
    pub r_remove: usize,
    pub q_rif: f64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addresses: vec![],
            policy: "prequal".into(),
            probe_pool_size: 16,
            probe_ttl_ms: 1000,
            r_probe: 3,
            r_remove: 1,
            q_rif: 2_f64.powf(-0.25),
        }
    }
}
