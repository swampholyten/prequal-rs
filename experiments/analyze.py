# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Aggregate the experiment JSON summaries into tables + figures.

Run with:  uv run experiments/analyze.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = Path(__file__).parent / "results"
FIG = Path(__file__).parent / "figures"
FIG.mkdir(exist_ok=True)


def load(pattern: str) -> list[dict]:
    out = []
    for f in sorted(RES.glob(pattern)):
        d = json.loads(f.read_text())
        d["_file"] = f.stem
        out.append(d)
    return out


def capacity(tag: str) -> float | None:
    f = RES / f"capacity_{tag}.txt"
    return float(f.read_text().strip()) if f.exists() else None


def cov(per_replica: list[dict]) -> float:
    """Coefficient of variation of per-replica query counts (imbalance)."""
    counts = [r["queries"] for r in per_replica]
    mean = sum(counts) / len(counts)
    if mean == 0:
        return 0.0
    var = sum((c - mean) ** 2 for c in counts) / len(counts)
    return (var**0.5) / mean


# ---------------------------------------------------------------- Exp A: ramp
ramp = load("rampA_*.json")
CAP = capacity("antagonist")
POL_COLORS = {"wrr": "tab:red", "prequal": "tab:blue"}

fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
for policy in ["wrr", "prequal"]:
    rows = sorted(
        (d for d in ramp if d["policy"] == policy), key=lambda d: d["offered_qps"]
    )
    x = [d["offered_qps"] for d in rows]
    for ax, q, style in zip(axes[:2], ["p50", "p99"], ["-o", "-o"]):
        ax.plot(
            x,
            [d["latency_ms"][q] for d in rows],
            style,
            color=POL_COLORS[policy],
            label=policy,
        )
    axes[2].plot(
        x,
        [100.0 * d["errors"] / max(d["queries"], 1) for d in rows],
        "-o",
        color=POL_COLORS[policy],
        label=policy,
    )
for ax, title in zip(axes, ["p50 latency", "p99 latency", "errors"]):
    ax.set_title(title)
    ax.set_xlabel("offered qps")
    if CAP:
        ax.axvline(CAP, color="gray", ls="--", lw=1, alpha=0.7)
    ax.legend()
    ax.grid(alpha=0.3)
axes[0].set_ylabel("ms")
axes[1].set_ylabel("ms")
axes[1].set_yscale("log")
axes[2].set_ylabel("% of queries")
fig.suptitle("Experiment A — load ramp, WRR vs Prequal (dashed line ≈ 1.0x capacity)")
fig.tight_layout()
fig.savefig(FIG / "expA_ramp.png", dpi=130)

print("\n== Experiment A: load ramp (WRR vs Prequal) ==")
print(f"{'qps':>5} {'policy':>9} {'p50':>8} {'p90':>8} {'p99':>9} {'p999':>9} {'err%':>6} {'cov':>5}")
for d in sorted(ramp, key=lambda d: (d["offered_qps"], d["policy"])):
    l = d["latency_ms"]
    err = 100.0 * d["errors"] / max(d["queries"], 1)
    print(
        f"{int(d['offered_qps']):>5} {d['policy']:>9} {l['p50']:>8.1f} {l['p90']:>8.1f}"
        f" {l['p99']:>9.1f} {l['p999']:>9.1f} {err:>6.2f} {cov(d['per_replica']):>5.2f}"
    )

# ------------------------------------------------------- Exp B: policy compare
pol = load("polB_*.json")
policies = ["random", "round-robin", "wrr", "po2", "prequal"]
pol_levels = sorted({int(d["offered_qps"]) for d in pol})
fig, axes = plt.subplots(1, len(pol_levels), figsize=(13, 4.5), sharey=True)
for ax, qps in zip(axes, pol_levels):
    rows = {d["policy"]: d for d in pol if int(d["offered_qps"]) == qps}
    y = range(len(policies))
    p90 = [rows[p]["latency_ms"]["p90"] for p in policies]
    p99 = [rows[p]["latency_ms"]["p99"] for p in policies]
    ax.barh(y, p99, color="lightsteelblue", label="p99")
    ax.barh(y, p90, color="tab:blue", label="p90")
    ax.set_yticks(y, policies)
    ax.invert_yaxis()
    pct = f" (~{int(100 * qps / CAP)}% of capacity)" if CAP else ""
    ax.set_title(f"{qps} qps{pct}")
    ax.set_xlabel("latency (ms)")
    ax.legend()
    ax.grid(alpha=0.3, axis="x")
fig.suptitle("Experiment B — replica selection rules (cf. paper Fig. 7)")
fig.tight_layout()
fig.savefig(FIG / "expB_policies.png", dpi=130)

print("\n== Experiment B: replica selection rules ==")
print(f"{'qps':>5} {'policy':>12} {'p50':>8} {'p90':>8} {'p99':>9} {'p999':>9} {'err%':>6} {'cov':>5}")
for d in sorted(pol, key=lambda d: (d["offered_qps"], policies.index(d["policy"]))):
    l = d["latency_ms"]
    err = 100.0 * d["errors"] / max(d["queries"], 1)
    print(
        f"{int(d['offered_qps']):>5} {d['policy']:>12} {l['p50']:>8.1f} {l['p90']:>8.1f}"
        f" {l['p99']:>9.1f} {l['p999']:>9.1f} {err:>6.2f} {cov(d['per_replica']):>5.2f}"
    )

# ------------------------------------------------------------ Exp D: probe rate
probe = load("probeD_*.json")
if probe:
    dqps = int(probe[0]["offered_qps"])
    print(f"\n== Experiment D: probing rate (prequal @{dqps} qps, ~1.15x) ==")
    print(f"{'r_probe':>8} {'p50':>8} {'p90':>8} {'p99':>9} {'p999':>9} {'err%':>6}")
    rows = sorted(probe, key=lambda d: int(d["_file"].split("rp")[1].split("_")[0]))
    xs, p99s, p999s = [], [], []
    for d in rows:
        rp = int(d["_file"].split("rp")[1].split("_")[0])
        l = d["latency_ms"]
        err = 100.0 * d["errors"] / max(d["queries"], 1)
        xs.append(rp)
        p99s.append(l["p99"])
        p999s.append(l["p999"])
        print(f"{rp:>8} {l['p50']:>8.1f} {l['p90']:>8.1f} {l['p99']:>9.1f} {l['p999']:>9.1f} {err:>6.2f}")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(xs, p99s, "-o", label="p99")
    ax.plot(xs, p999s, "-o", label="p99.9")
    ax.set_xlabel("r_probe (probes per query)")
    ax.set_ylabel("latency (ms)")
    ax.set_title("Experiment D — probing rate (cf. paper Fig. 8)")
    ax.set_xticks(xs)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / "expD_proberate.png", dpi=130)

# ------------------------------------------------------------ Exp C: Q_RIF sweep
qrif = load("qrifC_*.json")
if qrif:
    cqps = int(qrif[0]["offered_qps"])
    cap2 = capacity("fastslow")
    pct2 = f" (~{int(100 * cqps / cap2)}% of capacity)" if cap2 else ""
    print(f"\n== Experiment C: Q_RIF sweep, half fast / half slow @{cqps} qps{pct2} ==")
    print(f"{'q_rif':>6} {'p50':>8} {'p90':>8} {'p99':>9} {'p999':>9} {'err%':>6} {'fast:slow':>10}")
    rows = sorted(qrif, key=lambda d: float(d["_file"].split("_q")[1].split("_")[0]))
    xs, p50s, p90s, p99s = [], [], [], []
    for d in rows:
        q = float(d["_file"].split("_q")[1].split("_")[0])
        l = d["latency_ms"]
        err = 100.0 * d["errors"] / max(d["queries"], 1)
        # even index (ports 8001/8003/8005) = fast, odd index = slow (2x work)
        fast = sum(r["queries"] for i, r in enumerate(d["per_replica"]) if i % 2 == 0)
        slow = sum(r["queries"] for i, r in enumerate(d["per_replica"]) if i % 2 == 1)
        ratio = fast / max(slow, 1)
        xs.append(q)
        p50s.append(l["p50"])
        p90s.append(l["p90"])
        p99s.append(l["p99"])
        print(
            f"{q:>6.2f} {l['p50']:>8.1f} {l['p90']:>8.1f} {l['p99']:>9.1f}"
            f" {l['p999']:>9.1f} {err:>6.2f} {ratio:>10.2f}"
        )
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(xs, p50s, "-o", label="p50")
    ax.plot(xs, p90s, "-o", label="p90")
    ax.plot(xs, p99s, "-o", label="p99")
    ax.set_xlabel("Q_RIF   (0 = RIF-only  →  1 = latency-only)")
    ax.set_ylabel("latency (ms)")
    ax.set_title("Experiment C — hot/cold threshold Q_RIF (cf. paper Fig. 9)")
    ax.set_yscale("log")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / "expC_qrif.png", dpi=130)

print(f"\nfigures written to {FIG}")
