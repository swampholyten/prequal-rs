use tracing::info;

// Burn CPU at `cpu_pct`% of one core in a duty-cycle loop.
// Uses spawn_blocking so the spin does not starve the async runtime.
pub async fn run(cpu_pct: u8) {
    let cpu_pct = cpu_pct.min(99) as u64;
    info!("antagonist starting: {cpu_pct}% CPU target");

    loop {
        let work_ms = cpu_pct;
        let sleep_ms = 100 - cpu_pct;
        tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(work_ms);
            while std::time::Instant::now() < deadline {
                std::hint::spin_loop();
            }
        })
        .await
        .ok();
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
    }
}
