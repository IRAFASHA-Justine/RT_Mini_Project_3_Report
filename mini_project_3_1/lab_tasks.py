"""
Mini Project 3.1 — Lab Tasks driver.
Runs every experiment and writes results/mp31.json.
"""
import json
import statistics
import io
import contextlib
import os
import sys

# Make sibling imports work regardless of where this is run from
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import race_demo # type: ignore
import queue_sim
import semaphore_pool
import timing_overhead
import livelock_demo


res = {}


def race(n, runs=3):
    out = []
    for _ in range(runs):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            race_demo.run(num_threads=n)
        out.append(race_demo.shared_counter)
    return out


# Race demo: 3 runs x (5 and 10 threads)
res["race5"] = race(5)
res["race10"] = race(10)

# Queue (fast + slow consumer, then a maxsize sweep)
res["queue_fast"] = queue_sim.run(5, 0.0)
res["queue_slow"] = queue_sim.run(5, 0.005)
res["queue_sweep"] = [queue_sim.run(m, 0.002) for m in (1, 2, 5, 10, 20)]

# Semaphore counts 1, 3, 6
res["sem"] = {str(c): semaphore_pool.run(c) for c in (1, 3, 6)}

# Dispatch latency
x = timing_overhead.measure(200)
s = sorted(x)
res["timing"] = {
    "samples": x,
    "min": s[0],
    "mean": statistics.mean(x),
    "p95": s[int(0.95 * len(s)) - 1],
    "max": s[-1],
}

# Livelock
r, p = livelock_demo.run(1.0)
res["livelock"] = {"retries": r, "progress": p}

os.makedirs("results", exist_ok=True)
with open("results/mp31.json", "w") as f:
    json.dump(res, f, indent=2)

print("done — wrote results/mp31.json")