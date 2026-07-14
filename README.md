# Prequal Rust

Reproduction of **"Load is not what you should balance: Introducing Prequal"**
Wydrowski, Kleinberg, Rumble, Archer — NSDI 2024

---

## What this project is

Prequal is a load balancing policy from Google (deployed on YouTube) that replaces
CPU-utilisation-based policies (like Weighted Round Robin) with two real-time signals:

- **RIF** — Requests-In-Flight (instantaneous, no averaging required)
- **Latency** — median of recently completed queries at a given RIF level

Replicas are classified as **hot** or **cold** based on their RIF quantile.
The **HCL (Hot-Cold Lexicographic) rule** then picks:
- among cold replicas → lowest latency
- among hot replicas  → lowest RIF

Probing is **asynchronous**: probe responses are stored in a bounded pool and reused
across queries, taking probing off the critical serving path.

## Running

One binary, two subcommands: `server` runs a backend replica, `client` runs
the load generator. Build and test first:

```sh
cargo build --release
cargo test --release            # 10 unit tests on pool/HCL/config
```

### Manual run

Start two or more replicas (each on its own port), then point a client at them:

```sh
# terminal 1 and 2: replicas
./target/release/prequal-rs server --port 8001
./target/release/prequal-rs server --port 8002

# terminal 3: client, 60 s of 100 qps with the prequal policy
./target/release/prequal-rs client \
  --servers http://localhost:8001,http://localhost:8002 \
  --policy prequal --qps 100 --duration-s 60
```

The client prints a JSON summary (latency percentiles, per-replica counts,
errors) to stdout when the run ends. Useful knobs:

- `--policy` — `prequal | random | round-robin | po2 | wrr`
- server: `--cpu-alloc` (cores this replica may use), `--work-factor`
  (2.0 = a "slow" replica), `--antagonist-cpu / --antagonist-period-s /
  --antagonist-phase-s` (background CPU burner)
- client: `--qps`, `--mean-iterations` (work per query), `--balancers`,
  `--r-probe`, `--pool-capacity`, `--q-rif`
- `--help` on either subcommand lists everything

### Full experiment suite

Reproduces the paper's experiments (load ramp, policy comparison, `Q_RIF`
sweep, probing rate) with self-calibrating load levels. Docker is required
for per-replica CPU isolation; takes about 35 minutes:

```sh
experiments/run.sh              # env knobs: N_REPLICAS, REPLICA_CPUS, MEAN_ITER, DURATION_S
uv run experiments/analyze.py   # tables on stdout + experiments/figures/*.png
```

Set `USE_DOCKER=0` to fall back to bare processes (no CPU isolation, results
less meaningful). On GitHub, the `experiments` workflow runs the same suite
and uploads results as the `experiment-results` artifact.

See `REPORT.md` for the correctness review and the results of the CI run.

## Documentation

Every module, function, and parameter is documented with rustdoc, including the
crate-level overview of the entry point and query flow. Build and open the API
documentation page with:

```sh
cargo doc --no-deps --document-private-items --open
```
