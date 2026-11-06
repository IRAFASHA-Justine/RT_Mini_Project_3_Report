# Mini Project 3.1 — Concurrency, Synchronization, Race Conditions

| Script | Demonstrates | Report figure / section |
|---|---|---|
| `race_demo.py` | Lost-update race with 5 and 10 threads | Fig 1, §3.1 |
| `race_fixed.py` | Mutex fix (exact 5000) | §3.1 |
| `queue_sim.py` | Bounded queue as RTOS mailbox | Fig 4, §3.1 |
| `semaphore_pool.py` | Counting semaphore pool (1, 3, 6) | Fig 2, §Lab Task 2 |
| `timing_overhead.py` | Dispatch latency histogram | Fig 3, §3.1 |
| `livelock_demo.py` | Livelock with opposite lock order | §Lab Task 3 |
| `lab_tasks.py` | Runs all of the above → `results/mp31.json` | — |

## Run

```bash
python lab_tasks.py