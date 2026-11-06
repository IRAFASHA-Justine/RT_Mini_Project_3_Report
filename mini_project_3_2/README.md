# Mini Project 3.2 — Bounded-Buffer Producer–Consumer (Deadlock & Fix)

`bounded_buffer.py` implements the classic producer–consumer with explicit
`threading.Lock` objects (no `queue.Queue`) so acquisition order is fully
controllable.

- **Flawed mode** (`--flawed`): producers take `buffer_lock → stats_lock`,
  consumers take `stats_lock → buffer_lock` → circular wait → deadlock.
- **Fixed mode** (default): every task takes `buffer_lock → stats_lock`.
  Circular wait is impossible, so deadlock is impossible.

`bb_experiments.py` reproduces the deadlock 5 times, verifies the fix, and
sweeps buffer capacity 1 → 50. Writes `results/mp32.json`.

## Run

```bash
python bounded_buffer.py --flawed   # demonstrate the deadlock
python bounded_buffer.py            # verify the fix
python bb_experiments.py            # full sweep