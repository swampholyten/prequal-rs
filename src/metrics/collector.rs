use hdrhistogram::Histogram;
use parking_lot::Mutex;

pub struct MetricsCollector {
    inner: Mutex<Inner>,
}

struct Inner {
    histogram: Histogram<u64>,     // latency in microseconds
    rif_histogram: Histogram<u64>, // RIF values
    error_count: u64,
    query_count: u64,
    window_start: std::time::Instant,
}
