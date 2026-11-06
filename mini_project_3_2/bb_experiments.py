"""
Mini Project 3.2 — experiment driver.

Reproduces the deadlock 5 times, verifies the fix completes, and sweeps
buffer capacity from 1 to 50. Writes results/mp32.json.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bounded_buffer as bb


res = {
    "flawed": [bb.simulate(True, seed=s) for s in range(5)],
    "fixed": bb.simulate(False),
    "sweep": [],
}

for cap in (1, 2, 5, 10, 25, 50):
    runs = [bb.simulate(False, capacity=cap, seed=s) for s in range(3)]
    res["sweep"].append({
        "capacity": cap,
        **{k: sum(r[k] for r in runs) / 3
           for k in ("throughput", "avg_prod_block_ms", "avg_cons_block_ms")},
    })

os.makedirs("results", exist_ok=True)
with open("results/mp32.json", "w") as f:
    json.dump(res, f, indent=2, default=str)

print("ok — wrote results/mp32.json")