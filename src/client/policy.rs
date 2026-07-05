use std::sync::atomic::Ordering;
use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rand::RngExt;
use tracing::debug;

use crate::{
    client::pool::ProbePool,
    config::{PrequalConfig, ProbeResponse},
};

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

impl Balancer {
    pub fn new(policy_name: &str, urls: Vec<String>, cfg: PrequalConfig) -> Self {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .build()
            .expect("http client");
        let n = urls.len();
        let policy = match policy_name {
            "random" => Policy::Random,
            "round-robin" => Policy::RoundRobin(AtomicUsize::new(0)),
            "po2" => Policy::Po2,
            "wrr" => Policy::Wrr {
                weights: Arc::new(Mutex::new(vec![1.0; n])),
            },
            "prequal" => Policy::Prequal {
                pool: Arc::new(Mutex::new(ProbePool::new(
                    cfg.pool_capacity,
                    Duration::from_millis(cfg.probe_ttl_ms),
                    cfg.reuse_budget(n),
                    cfg.q_rif,
                ))),
                cfg,
            },
            other => panic!("unknown policy: {other}"),
        };
        Self {
            policy,
            urls: Arc::new(urls),
            http,
            probe_timeout: Duration::from_millis(100),
        }
    }

    /// Debug: dump pool contents periodically.
    pub fn start_pool_dump(&self) {
        if let Policy::Prequal { pool, .. } = &self.policy {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                loop {
                    tick.tick().await;
                    let snap = pool.lock().snapshot(Instant::now());
                    let view: Vec<String> = snap
                        .iter()
                        .map(|(r, rif, lat, age)| {
                            format!("r{r}:rif={rif},lat={}ms,age={age}ms", lat / 1000)
                        })
                        .collect();
                    tracing::info!("pool: {}", view.join(" "));
                }
            });
        }
    }

    /// Spawn background machinery a policy needs (WRR weight refresher).
    pub fn start(&self) {
        if let Policy::Wrr { weights } = &self.policy {
            let weights = weights.clone();
            let urls = self.urls.clone();
            let http = self.http.clone();
            let timeout = self.probe_timeout;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    for (i, url) in urls.iter().enumerate() {
                        if let Some(p) = fetch_probe(&http, url, timeout).await {
                            weights.lock()[i] = 1.0 / p.cpu_util.max(0.05);
                        }
                    }
                }
            });
        }
    }

    /// Pick a replica index for the next query.
    pub async fn select(&self) -> usize {
        let n = self.urls.len();
        match &self.policy {
            Policy::Random => rand::rng().random_range(0..n),
            Policy::RoundRobin(counter) => counter.fetch_add(1, Ordering::Relaxed) % n,
            Policy::Po2 => {
                let (a, b) = {
                    let mut rng = rand::rng();
                    let a = rng.random_range(0..n);
                    let mut b = rng.random_range(0..n - 1);
                    if b >= a {
                        b += 1;
                    }
                    (a, b)
                };
                let (pa, pb) = tokio::join!(
                    fetch_probe(&self.http, &self.urls[a], self.probe_timeout),
                    fetch_probe(&self.http, &self.urls[b], self.probe_timeout),
                );
                match (pa, pb) {
                    (Some(pa), Some(pb)) => {
                        if pa.rif <= pb.rif {
                            a
                        } else {
                            b
                        }
                    }
                    (Some(_), None) => a,
                    (None, Some(_)) => b,
                    (None, None) => a,
                }
            }
            Policy::Wrr { weights } => {
                let weights = weights.lock();
                let total: f64 = weights.iter().sum();
                let mut x = rand::rng().random_range(0.0..total);
                for (i, w) in weights.iter().enumerate() {
                    x -= w;
                    if x <= 0.0 {
                        return i;
                    }
                }
                n - 1
            }
            Policy::Prequal { pool, cfg } => {
                self.trigger_probes(pool, cfg.r_probe);
                let mut pool = pool.lock();
                pool.expire(Instant::now());
                // Fall back to uniform random when occupancy drops below 2 (§4).
                let choice = if pool.len() < 2 {
                    None
                } else {
                    pool.hcl_select()
                };
                for _ in 0..cfg.r_remove {
                    pool.remove_one();
                }
                drop(pool);
                choice.unwrap_or_else(|| rand::rng().random_range(0..n))
            }
        }
    }

    /// Fire r_probe asynchronous probes to distinct random replicas; the
    /// responses land in the pool off the critical path (§4 "Probing rate").
    fn trigger_probes(&self, pool: &Arc<Mutex<ProbePool>>, r_probe: usize) {
        let n = self.urls.len();
        let targets = rand::seq::index::sample(&mut rand::rng(), n, r_probe.min(n)).into_vec();
        for replica in targets {
            let http = self.http.clone();
            let url = self.urls[replica].clone();
            let pool = pool.clone();
            let timeout = self.probe_timeout;
            tokio::spawn(async move {
                if let Some(resp) = fetch_probe(&http, &url, timeout).await {
                    pool.lock().insert(super::pool::Probe {
                        replica,
                        rif: resp.rif,
                        rif_at_probe: resp.rif,
                        latency_us: resp.latency_us,
                        received_at: Instant::now(),
                        uses: 0,
                    });
                } else {
                    debug!("probe to {url} failed");
                }
            });
        }
    }
}

async fn fetch_probe(
    http: &reqwest::Client,
    url: &str,
    timeout: Duration,
) -> Option<ProbeResponse> {
    http.get(format!("{url}/probe"))
        .timeout(timeout)
        .send()
        .await
        .ok()?
        .json::<ProbeResponse>()
        .await
        .ok()
}
