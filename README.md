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
