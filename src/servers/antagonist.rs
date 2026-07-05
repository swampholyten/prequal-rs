use std::time::{Duration, Instant};

use tracing::info;

/// Burn CPU in 100ms duty cycles. With `period_s > 0` the burn follows a
/// square wave — `cpu_pct` for the first half of each period, idle for the
/// second half — offset by `phase_s`, so replicas experience interference
/// spikes at different times (the paper's time-varying antagonist load).
/// Uses spawn_blocking so the spin does not starve the async runtime.
pub async fn run(cpu_pct: u8, period_s: u64, phase_s: u64) {
    let cpu_pct = cpu_pct.min(100) as u64;
    info!("antagonist starting: {cpu_pct}% CPU, period {period_s}s, phase {phase_s}s");
    let origin = Instant::now();

    loop {
        let active = if period_s == 0 {
            true
        } else {
            let t = (origin.elapsed().as_secs() + phase_s) % period_s;
            t < period_s / 2
        };
        let work_ms = if active { cpu_pct } else { 0 };
        let sleep_ms = 100 - work_ms;

        if work_ms > 0 {
            tokio::task::spawn_blocking(move || {
                let deadline = Instant::now() + Duration::from_millis(work_ms);
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            })
            .await
            .ok();
        }
        if sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }
}
