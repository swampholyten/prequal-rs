use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use clap::Args;
use parking_lot::Mutex;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Instant,
};
use tokio::sync::Semaphore;
use tracing::info;

use crate::{
    config::{ProbeResponse, WorkRequest, WorkResponse},
    servers::antagonist,
};

#[derive(Args, Debug)]
pub struct ServerArgs {
    #[arg(long, default_value_t = 8000)]
    pub port: u16,

    /// CPU allocation of this replica in cores (the paper's guaranteed
    /// per-VM allocation). Normalizes reported CPU utilization and bounds
    /// in-process concurrency: at most round(cpu_alloc) queries execute at
    /// once, the rest queue. Should match the kernel-enforced limit of the
    /// container/VM the replica runs in (e.g. docker --cpus).
    #[arg(long, default_value_t = 1.0)]
    pub cpu_alloc: f64,

    /// Antagonist burn target in % of one core (0 = no antagonist).
    #[arg(long, default_value_t = 0)]
    pub antagonist_cpu: u8,

    /// Antagonist square-wave period in seconds (0 = constant burn).
    #[arg(long, default_value_t = 0)]
    pub antagonist_period_s: u64,

    /// Phase offset of the square wave, so replicas spike at different times.
    #[arg(long, default_value_t = 0)]
    pub antagonist_phase_s: u64,

    /// Multiplier on per-query work; 2.0 makes this a "slow" replica standing
    /// in for older hardware (§5.2/§5.3 of the paper).
    #[arg(long, default_value_t = 1.0)]
    pub work_factor: f64,
}

#[derive(Clone)]
struct ServerState {
    rif: Arc<AtomicU32>,
    latency_ring: Arc<Mutex<LatencyRing>>,
    cpu: Arc<Mutex<CpuTracker>>,
    work_factor: f64,
    /// Worker slots bounding concurrent query execution to the CPU
    /// allocation; shared with the antagonist so its bursts steal this
    /// replica's serving capacity specifically.
    work_slots: Arc<Semaphore>,
}

/// Decrements RIF on drop, so a query cancelled mid-flight (client timeout
/// closes the connection and axum drops the handler future) still gets
/// counted out. Without this, RIF leaks upward forever under overload,
/// corrupting the primary load signal.
struct RifGuard(Arc<AtomicU32>);

impl RifGuard {
    /// Increment and return the RIF including this query.
    fn arm(rif: &Arc<AtomicU32>) -> (Self, u32) {
        let at_arrival = rif.fetch_add(1, Ordering::SeqCst) + 1;
        (Self(rif.clone()), at_arrival)
    }
}

impl Drop for RifGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Ring buffer of recently completed queries for probe latency estimation.
struct LatencyRing {
    entries: Vec<RingEntry>,
    head: usize,
    capacity: usize,

    /// Estimates use only samples this recently. Older samples predate antagonist swings.
    max_age: std::time::Duration,
}

#[derive(Clone, Copy)]
struct RingEntry {
    rif_at_arrival: u32,
    duration_us: u64,
    finished_at: Instant,
}

// Process CPU time (user+sys) via getrusage.
// Includes the in-process antagonist, like machine CPU utilization in the paper's setting.
struct CpuTracker {
    last_cpu_us: u64,
    last_wall: Instant,
    alloc: f64,
    util_ema: f64,
}

impl LatencyRing {
    fn new(capacity: usize, max_age: std::time::Duration) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            head: 0,
            capacity,
            max_age,
        }
    }

    fn push(&mut self, rif_at_arrival: u32, duration_us: u64) {
        let entry = RingEntry {
            rif_at_arrival,
            duration_us,
            finished_at: Instant::now(),
        };
        if self.entries.len() < self.capacity {
            self.entries.push(entry);
        } else {
            self.entries[self.head] = entry;
            self.head = (self.head + 1) % self.capacity;
        }
    }

    /// Latency estimate for a query arriving at the current `rif`, from the
    /// median of the samples nearest that RIF. The estimate must be
    /// RIF-indexed to stay a *leading* indicator: when RIF spikes, the
    /// advertised latency must rise immediately — before any slow completion
    /// lands — or clients keep herding onto the replica for a full
    /// completion round-trip. When the nearest samples were taken at a
    /// different occupancy, scale by (rif+1)/(tag+1), the processor-sharing
    /// relation our CPU-capped replicas actually follow. Fresh samples are
    /// preferred (they reflect the current antagonist state); a drained
    /// replica falls back to the whole ring.
    fn median_near(&self, rif: u32, now: Instant) -> u64 {
        let fresh: Vec<&RingEntry> = self
            .entries
            .iter()
            .filter(|e| now.duration_since(e.finished_at) <= self.max_age)
            .collect();
        let mut candidates: Vec<&RingEntry> = if fresh.is_empty() {
            self.entries.iter().collect()
        } else {
            fresh
        };
        if candidates.is_empty() {
            return 0;
        }
        candidates.sort_unstable_by_key(|e| e.rif_at_arrival.abs_diff(rif));
        let nearest = &candidates[..candidates.len().min(16)];
        let median_of = |mut v: Vec<u64>| -> u64 {
            v.sort_unstable();
            v[v.len() / 2]
        };
        let latency = median_of(nearest.iter().map(|e| e.duration_us).collect());
        let tag = median_of(nearest.iter().map(|e| e.rif_at_arrival as u64).collect());
        latency * (rif as u64 + 1) / (tag + 1)
    }
}

fn process_cpu_us() -> u64 {
    let mut ru = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let ru = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr());
        ru.assume_init()
    };
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

impl CpuTracker {
    fn new(alloc: f64) -> Self {
        Self {
            last_cpu_us: process_cpu_us(),
            last_wall: Instant::now(),
            alloc,
            util_ema: 0.0,
        }
    }

    fn sample(&mut self) {
        let cpu = process_cpu_us();
        let wall = Instant::now();
        let dw = wall.duration_since(self.last_wall).as_micros() as f64;
        if dw > 0.0 {
            let util = (cpu - self.last_cpu_us) as f64 / (dw * self.alloc);
            // WRR uses smoothed historical statistics; EMA models that.
            self.util_ema = 0.7 * self.util_ema + 0.3 * util;
        }
        self.last_cpu_us = cpu;
        self.last_wall = wall;
    }
}

/// The CPU-bound unit of work: an iterated hash, as in the paper's testbed.
fn spin_hash(iterations: u64) -> u64 {
    let mut x: u64 = 0xcbf29ce484222325;
    for i in 0..iterations {
        x ^= i;
        x = x.wrapping_mul(0x100000001b3);
        x ^= x >> 33;
    }
    std::hint::black_box(x)
}

async fn work(
    State(state): State<ServerState>,
    Json(req): Json<WorkRequest>,
) -> Json<WorkResponse> {
    // RIF spans arrival to finish, so it counts queued queries too — that is
    // what makes it the leading indicator the paper wants (§4 "Load signals":
    // latency "includes the sojourn time in the queue").
    let (_rif, rif_at_arrival) = RifGuard::arm(&state.rif);
    let start = Instant::now();

    let iterations = (req.iterations as f64 * state.work_factor) as u64;
    let permit = state
        .work_slots
        .clone()
        .acquire_owned()
        .await
        .expect("semaphore closed");
    // The permit moves into the closure: even if this handler is cancelled,
    // the slot is held until the spin actually ends.
    tokio::task::spawn_blocking(move || {
        let out = spin_hash(iterations);
        drop(permit);
        out
    })
    .await
    .expect("worker panicked");

    let duration_us = start.elapsed().as_micros() as u64;
    state.latency_ring.lock().push(rif_at_arrival, duration_us);
    Json(WorkResponse { duration_us })
}

async fn probe(State(state): State<ServerState>) -> Json<ProbeResponse> {
    let rif = state.rif.load(Ordering::SeqCst);
    let latency_us = state.latency_ring.lock().median_near(rif, Instant::now());
    let cpu_util = state.cpu.lock().util_ema;
    Json(ProbeResponse {
        rif,
        latency_us,
        cpu_util,
    })
}

pub async fn run(args: ServerArgs) {
    let slots = (args.cpu_alloc.round() as usize).max(1);
    let state = ServerState {
        rif: Arc::new(AtomicU32::new(0)),
        latency_ring: Arc::new(Mutex::new(LatencyRing::new(
            512,
            std::time::Duration::from_millis(500),
        ))),
        cpu: Arc::new(Mutex::new(CpuTracker::new(args.cpu_alloc))),
        work_factor: args.work_factor,
        work_slots: Arc::new(Semaphore::new(slots)),
    };

    let cpu = state.cpu.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            tick.tick().await;
            cpu.lock().sample();
        }
    });

    if args.antagonist_cpu > 0 {
        tokio::spawn(antagonist::run(
            args.antagonist_cpu,
            args.antagonist_period_s,
            args.antagonist_phase_s,
            state.work_slots.clone(),
        ));
    }

    let app = Router::new()
        .route("/work", post(work))
        .route("/probe", get(probe))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", args.port);
    info!("replica listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app).await.expect("server failed");
}
