//! Aggregates per-query outcomes into HDR histograms for the run summary.

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde_json::json;

/// Thread-safe collector shared by all query tasks in [`crate::client::run`].
/// Records one latency sample (or error) per query and renders the summary
/// printed at the end of the run and in the periodic progress line.
pub struct MetricsCollector {
    inner: Mutex<Inner>,
}

/// Mutable state behind the collector's lock.
struct Inner {
    /// End-to-end latency (µs) across all replicas.
    histogram: Histogram<u64>,
    /// Same latencies split by serving replica, to show load distribution.
    per_replica: Vec<Histogram<u64>>,
    /// Failed queries: HTTP errors, timeouts, bad response bodies.
    error_count: u64,
    /// All queries, successful or not.
    query_count: u64,
    /// Collector creation time, for the elapsed-seconds field.
    window_start: std::time::Instant,
}

impl MetricsCollector {
    /// Empty collector with one per-replica histogram per backend.
    pub fn new(n_replicas: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                histogram: Histogram::new(3).expect("histogram"),
                per_replica: (0..n_replicas)
                    .map(|_| Histogram::new(3).expect("histogram"))
                    .collect(),
                error_count: 0,
                query_count: 0,
                window_start: std::time::Instant::now(),
            }),
        }
    }

    /// Record a successful query: its end-to-end latency (µs) and which
    /// replica served it.
    pub fn record(&self, replica: usize, latency_us: u64) {
        let mut inner = self.inner.lock();
        inner.query_count += 1;
        inner.histogram.record(latency_us).ok();
        inner.per_replica[replica].record(latency_us).ok();
    }

    /// Record a failed query (timeout, HTTP error, or undecodable body).
    pub fn record_error(&self) {
        let mut inner = self.inner.lock();
        inner.query_count += 1;
        inner.error_count += 1;
    }

    /// JSON summary: policy, offered qps, counts, overall latency
    /// percentiles (ms), and per-replica query counts and percentiles.
    pub fn summary(&self, policy: &str, qps: f64) -> serde_json::Value {
        let inner = self.inner.lock();
        let h = &inner.histogram;
        let per_replica: Vec<serde_json::Value> = inner
            .per_replica
            .iter()
            .map(|h| {
                json!({
                    "queries": h.len(),
                    "p50_ms": h.value_at_quantile(0.50) as f64 / 1000.0,
                    "p99_ms": h.value_at_quantile(0.99) as f64 / 1000.0,
                })
            })
            .collect();
        json!({
            "policy": policy,
            "offered_qps": qps,
            "per_replica": per_replica,
            "elapsed_s": inner.window_start.elapsed().as_secs_f64(),
            "queries": inner.query_count,
            "errors": inner.error_count,
            "latency_ms": {
                "mean": h.mean() / 1000.0,
                "p50": h.value_at_quantile(0.50) as f64 / 1000.0,
                "p90": h.value_at_quantile(0.90) as f64 / 1000.0,
                "p99": h.value_at_quantile(0.99) as f64 / 1000.0,
                "p999": h.value_at_quantile(0.999) as f64 / 1000.0,
                "max": h.max() as f64 / 1000.0,
            },
        })
    }
}
