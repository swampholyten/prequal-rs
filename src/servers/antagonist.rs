use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::Semaphore;
use tracing::info;

/// Burn a fraction of this replica's CPU allocation. With `period_s > 0` the
/// burn follows a square wave — `cpu_pct` for the first half of each period,
/// idle for the second half — offset by `phase_s`, so replicas experience
/// interference spikes at different times (the paper's time-varying
/// antagonist load).
///
/// The burner competes for the replica's own worker slots, so a spike
/// steals serving capacity from *this* replica specifically — modeling an
/// antagonist process saturating the machine/VM that hosts it, while other
/// replicas keep their own allocations. A deficit counter keeps the average
/// burn at the configured rate even when bursts queue behind in-flight
/// queries.
pub async fn run(cpu_pct: u8, period_s: u64, phase_s: u64, slots: Arc<Semaphore>) {
    let target_rate = cpu_pct.min(100) as f64 / 100.0;
    info!("antagonist starting: {cpu_pct}% of allocation, period {period_s}s, phase {phase_s}s");
    let origin = Instant::now();
    let mut last = Instant::now();
    let mut deficit_us = 0.0_f64;

    loop {
        let now = Instant::now();
        let active = if period_s == 0 {
            true
        } else {
            (now.duration_since(origin).as_secs() + phase_s) % period_s < period_s / 2
        };
        if active {
            deficit_us += target_rate * now.duration_since(last).as_micros() as f64;
        }
        last = now;
        // Cap the backlog so a long wait doesn't trigger a catch-up train.
        deficit_us = deficit_us.min(300_000.0);

        if deficit_us >= 20_000.0 {
            let burst = Duration::from_micros(deficit_us.min(100_000.0) as u64);
            let permit = slots.clone().acquire_owned().await.expect("semaphore closed");
            tokio::task::spawn_blocking(move || {
                let deadline = Instant::now() + burst;
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
                drop(permit);
            })
            .await
            .ok();
            deficit_us -= burst.as_micros() as f64;
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
