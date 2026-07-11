pub mod policy;
pub mod pool;

use std::{sync::Arc, time::{Duration, Instant}};

use clap::Args;
use rand_distr::{Distribution, Exp, Normal};
use tracing::info;

use crate::{client::policy::Balancer, config::{PrequalConfig, WorkRequest, WorkResponse}, metrics::collector::MetricsCollector};

#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Comma-separated replica base URLs, e.g. http://r1:8000,http://r2:8000
    #[arg(long, value_delimiter = ',')]
    pub servers: Vec<String>,
    /// Balancing policy: prequal | random | round-robin | po2 | wrr
    #[arg(long, default_value = "prequal")]
    pub policy: String,
    /// Offered load: open-loop Poisson arrivals per second.
    #[arg(long, default_value_t = 100.0)]
    pub qps: f64,
    #[arg(long, default_value_t = 60)]
    pub duration_s: u64,
    /// Mean hash iterations per query; per-query cost is Normal with
    /// std = mean, truncated at zero (the paper's testbed workload).
    #[arg(long, default_value_t = 2_000_000)]
    pub mean_iterations: u64,
    /// Per-query timeout; timeouts count as errors.
    #[arg(long, default_value_t = 2000)]
    pub timeout_ms: u64,
    /// Number of independent balancer instances (separate probe pools).
    /// The paper's testbed has 100 client replicas; a single shared pool
    /// makes every query herd onto the same "best" replica.
    #[arg(long, default_value_t = 6)]
    pub balancers: usize,
    #[arg(long, default_value_t = 3)]
    pub r_probe: usize,
    /// Paper default is 16 with n=100 replicas; the pool must stay well
    /// below the replica count so each balancer sees a random subset —
    /// that is what decorrelates clients and prevents herding. With our
    /// 6-replica testbed, 4 is the equivalent setting.
    #[arg(long, default_value_t = 4)]
    pub pool_capacity: usize,
    #[arg(long, default_value_t = 1000)]
    pub probe_ttl_ms: u64,
    /// Hot/cold RIF quantile; 0 = RIF-only control (§5.2).
    #[arg(long, default_value_t = 2_f64.powf(-0.25))]
    pub q_rif: f64,
}

pub async fn run(args: ClientArgs) {
    assert!(
        args.servers.len() >= 2,
        "need at least 2 replica URLs (--servers)"
    );
    let cfg = PrequalConfig {
        r_probe: args.r_probe,
        pool_capacity: args.pool_capacity,
        probe_ttl_ms: args.probe_ttl_ms,
        q_rif: args.q_rif,
        ..Default::default()
    };
    let balancers: Vec<Arc<Balancer>> = (0..args.balancers.max(1))
        .map(|_| {
            Arc::new(Balancer::new(&args.policy, args.servers.clone(), cfg))
        })
        .collect();
    for b in &balancers {
        b.start();
    }
    if std::env::var("DEBUG_POOL").is_ok() {
        balancers[0].start_pool_dump();
    }

    let metrics = Arc::new(MetricsCollector::new(args.servers.len()));
    let normal = Normal::new(args.mean_iterations as f64, args.mean_iterations as f64)
        .expect("normal distribution");
    let inter_arrival = Exp::new(args.qps).expect("exp distribution");
    let timeout = Duration::from_millis(args.timeout_ms);

    info!(
        "client starting: policy={} qps={} duration={}s replicas={}",
        args.policy,
        args.qps,
        args.duration_s,
        args.servers.len()
    );

    // Periodic progress line.
    {
        let metrics = metrics.clone();
        let policy = args.policy.clone();
        let qps = args.qps;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.tick().await;
            loop {
                tick.tick().await;
                info!("progress: {}", metrics.summary(&policy, qps));
            }
        });
    }

    let deadline = Instant::now() + Duration::from_secs(args.duration_s);
    // Absolute arrival schedule with catch-up: per-arrival sleep() has a
    // ~1ms floor, which silently caps the generator near ~350 qps.
    let mut next_arrival = Instant::now();
    while next_arrival < deadline {
        let (wait_s, iterations) = {
            let mut rng = rand::rng();
            let wait_s = inter_arrival.sample(&mut rng);
            let iterations = normal.sample(&mut rng).max(0.0) as u64;
            (wait_s, iterations)
        };
        next_arrival += Duration::from_secs_f64(wait_s);
        tokio::time::sleep_until(next_arrival.into()).await;

        let balancer = balancers[rand::random_range(0..balancers.len())].clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let replica = balancer.select().await;
            let result = balancer
                .http
                .post(format!("{}/work", balancer.urls[replica]))
                .timeout(timeout)
                .json(&WorkRequest { iterations })
                .send()
                .await
                .and_then(|r| r.error_for_status());
            match result {
                Ok(resp) => match resp.json::<WorkResponse>().await {
                    Ok(_) => metrics.record(replica, start.elapsed().as_micros() as u64),
                    Err(_) => metrics.record_error(),
                },
                Err(_) => metrics.record_error(),
            }
        });
    }

    // Let in-flight queries drain before reporting.
    tokio::time::sleep(timeout + Duration::from_millis(500)).await;
    println!("{}", metrics.summary(&args.policy, args.qps));
}
