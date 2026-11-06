# Mini Project 3.3 — Priority-Based Interrupt Latency Simulator

Three periodic interrupt sources, priority 1 (highest) to 3 (lowest):

| Source | Priority | Period | Top half | Bottom half |
|---|---|---|---|---|
| A (hi) | 1 | 20 ms | 0.5 ms | 3 ms |
| B (mid) | 2 | 50 ms | 0.5 ms | 8 ms |
| C (lo) | 3 | 100 ms | 0.5 ms | 15 ms |

Top halves occupy the CPU (via `irq_lock`) and preempt everything. Bottom
halves are queued by priority and executed in 1 ms slices by a single CPU
worker, so higher-priority arrivals preempt lower-priority running work mid-
slice. Writes `results/mp33.json`.

## Run

```bash
python irq_sim.py