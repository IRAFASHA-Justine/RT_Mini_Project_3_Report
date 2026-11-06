"""
Mini Project 3.1 — Step 9: thread-dispatch latency.

A new thread sets an Event; the main thread measures the time until
event.wait() returns. That gap is the observed dispatch latency.
"""
import threading
import time
import statistics
import json


def measure(n=200):
    out = []
    for _ in range(n):
        ev = threading.Event()
        t = threading.Thread(target=ev.set)
        t0 = time.perf_counter()
        t.start()
        ev.wait()
        out.append((time.perf_counter() - t0) * 1e6)   # microseconds
        t.join()
    return out


if __name__ == "__main__":
    x = measure()
    s = sorted(x)
    print(f"n={len(x)} min={s[0]:.1f} us mean={statistics.mean(x):.1f} us "
          f"p95={s[int(0.95 * len(s)) - 1]:.1f} us max={s[-1]:.1f} us "
          f"(worst-case observed dispatch latency)")