#!/bin/bash
# Testbed experiments mirroring §5 of the Prequal paper (NSDI'24): N replica
# entities + 1 client process on one machine, CPU-bound hash work.
#
# Each replica runs in its own Docker container with a hard CPU limit
# (--cpus $REPLICA_CPUS) — the analogue of the paper's per-VM guaranteed CPU
# allocation. Set USE_DOCKER=0 to fall back to bare host processes (the
# in-process worker-slot cap still bounds concurrency, but there is no
# kernel-enforced CPU isolation).
#
# The suite is self-calibrating: it first walks the random policy up a load
# ladder to find this testbed's saturation knee, then expresses every
# experiment's load level as a fraction of that measured capacity. This keeps
# the suite meaningful across machines (laptop, CI runner, ...).
#
# Env knobs: N_REPLICAS (default 6), REPLICA_CPUS (default 1.0), MEAN_ITER
# (default 6M ~ 13ms/core), DURATION_S per data point (default 40).
set -u
cd "$(dirname "$0")/.."
BIN=./target/release/prequal-rs
RES=experiments/results
mkdir -p "$RES"

N=${N_REPLICAS:-6}
REPLICA_CPUS=${REPLICA_CPUS:-1.0}
MEAN_ITER=${MEAN_ITER:-6000000}
DUR=${DURATION_S:-40}
USE_DOCKER=${USE_DOCKER:-1}
IMAGE=${IMAGE:-prequal-rs:exp}

SERVERS=$(for i in $(seq 1 "$N"); do printf "http://localhost:%d," $((8000+i)); done | sed 's/,$//')

if [ "$USE_DOCKER" = 1 ] && ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "building $IMAGE"
    docker build -q -t "$IMAGE" -f experiments/Dockerfile.runtime target/release
fi

start_replica() { # $1 = index, rest = extra server args
    local i="$1"; shift
    if [ "$USE_DOCKER" = 1 ]; then
        docker run -d --rm --name "prequal-r$i" --cpus "$REPLICA_CPUS" \
            -p "$((8000+i)):8000" "$IMAGE" \
            server --port 8000 --cpu-alloc "$REPLICA_CPUS" "$@" >/dev/null
    else
        $BIN server --port "$((8000+i))" --cpu-alloc "$REPLICA_CPUS" "$@" 2>/dev/null &
    fi
}

stop_servers() {
    if [ "$USE_DOCKER" = 1 ]; then
        docker ps -q --filter name='prequal-r' | xargs -r docker rm -f >/dev/null 2>&1
    else
        pkill -f 'prequal-rs server' 2>/dev/null
    fi
    sleep 1.5
}

# N replicas with staggered square-wave antagonists (60% of the allocation,
# 20s period): the paper's unpredictable time-varying antagonist load (§5.1).
start_antagonist_testbed() {
    for i in $(seq 1 "$N"); do
        start_replica "$i" --antagonist-cpu 60 --antagonist-period-s 20 \
            --antagonist-phase-s $(( (i-1)*20/N ))
    done
    sleep 3
}

# Half fast, half slow replicas (work inflated 2x on slow), no antagonists:
# the paper's fast/slow hardware-generation split for the Q_RIF experiment.
start_fastslow_testbed() {
    for i in $(seq 1 "$N"); do
        wf=1.0; [ $((i % 2)) -eq 0 ] && wf=2.0
        start_replica "$i" --work-factor $wf
    done
    sleep 3
}

client() { # writes summary JSON to $1; rest = extra client args
    local out="$1"; shift
    $BIN client --servers "$SERVERS" --mean-iterations "$MEAN_ITER" \
        --timeout-ms 5000 "$@" 2>/dev/null > "$RES/$out"
}

run_client() {
    echo ">>> $1"
    client "$@" --duration-s "$DUR"
    sleep 12   # drain + thermal cooldown; back-to-back runs contaminate each other
}

p50_of() { python3 -c "import json;print(json.load(open('$RES/$1'))['latency_ms']['p50'])"; }
err_of() { python3 -c "import json;d=json.load(open('$RES/$1'));print(100*d['errors']/max(d['queries'],1))"; }

# Walk the random policy up a x1.2 ladder until p50 exceeds 4x the unloaded
# p50 (or errors appear); capacity = the previous level. Writes it to
# $RES/capacity_<tag>.txt and returns it on stdout.
calibrate() {
    local tag=$1
    client "cal_${tag}_base.json" --policy random --qps 20 --duration-s 10
    local base; base=$(p50_of "cal_${tag}_base.json")
    local q=$(( N * 20 )) prev=$(( N * 20 ))
    for _ in $(seq 1 10); do
        sleep 6
        client "cal_${tag}_${q}.json" --policy random --qps "$q" --duration-s 15
        local p50 err
        p50=$(p50_of "cal_${tag}_${q}.json"); err=$(err_of "cal_${tag}_${q}.json")
        echo "calibrate[$tag]: qps=$q p50=${p50}ms (base ${base}ms) err=${err}%" >&2
        if python3 -c "exit(0 if $p50 > 4*$base or $err > 0 else 1)"; then break; fi
        prev=$q
        q=$(python3 -c "print(int($q*1.2))")
    done
    echo "$prev" > "$RES/capacity_${tag}.txt"
    echo "$prev"
}

frac() { python3 -c "print(int($1*$2))"; }

# ---- Calibration + Experiment A: load ramp, WRR vs Prequal (§5.1, Fig. 6) --
stop_servers; start_antagonist_testbed
C=$(calibrate antagonist)
echo "=== capacity (antagonist testbed): $C qps ==="
# The paper's multiplicative 10/9 load steps, 0.75x .. 1.27x of allocation.
for f in 0.75 0.83 0.93 1.03 1.14 1.27; do
    qps=$(frac "$C" "$f")
    for policy in wrr prequal; do
        run_client "rampA_${policy}_${qps}.json" --policy $policy --qps "$qps"
    done
done

# ---- Experiment B: replica selection rules at 70% / 90% (§5.2, Fig. 7) -----
for f in 0.70 0.90; do
    qps=$(frac "$C" "$f")
    for policy in random round-robin wrr po2 prequal; do
        run_client "polB_${policy}_${qps}.json" --policy $policy --qps "$qps"
    done
done

# ---- Experiment D: probing rate r_probe (§5.3, Fig. 8) ---------------------
# Run hot (~1.15x) so probe-pool freshness matters.
qps=$(frac "$C" 1.15)
for rp in 1 2 3 4; do
    run_client "probeD_rp${rp}_${qps}.json" --policy prequal --qps "$qps" --r-probe "$rp"
done
stop_servers

# ---- Experiment C: Q_RIF sweep on fast/slow replicas (§5.3, Fig. 9) --------
start_fastslow_testbed
C2=$(calibrate fastslow)
echo "=== capacity (fast/slow testbed): $C2 qps ==="
qps=$(frac "$C2" 0.90)
for q in 0.0 0.35 0.59 0.84 0.97 1.0; do
    run_client "qrifC_q${q}_${qps}.json" --policy prequal --qps "$qps" --q-rif "$q"
done
stop_servers
echo "all experiments done"
