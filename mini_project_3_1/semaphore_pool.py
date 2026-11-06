"""
Mini Project 3.1 — Steps 7-8: counting semaphore as a pool of fixed-size blocks.

Eight task threads compete for Semaphore(3), each holding the resource for a
random 50-150 ms. Peak simultaneous holders must equal the semaphore count.
"""
import threading
import time
import random


def run(count=3, tasks=8, seed=1):
    random.seed(seed)
    sem = threading.Semaphore(count)
    state = threading.Lock()
    holding = 0
    peak = 0
    waits = [0.0] * tasks
    holds = [random.uniform(0.05, 0.15) for _ in range(tasks)]

    def task(tid):
        nonlocal holding, peak
        t0 = time.perf_counter()
        sem.acquire()
        waits[tid] = time.perf_counter() - t0
        with state:
            holding += 1
            peak = max(peak, holding)
        time.sleep(holds[tid])
        with state:
            holding -= 1
        sem.release()

    ts = [threading.Thread(target=task, args=(i,)) for i in range(tasks)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    w = [x * 1e3 for x in waits]
    return {
        "count": count,
        "waits_ms": w,
        "peak_holders": peak,
        "immediate": sum(1 for x in w if x < 5),
        "avg_wait_ms": sum(w) / len(w),
        "max_wait_ms": max(w),
    }


if __name__ == "__main__":
    r = run(3)
    for i, w in enumerate(r["waits_ms"]):
        print(f"task {i}: wait = {w:7.2f} ms")
    print(f"peak simultaneous holders = {r['peak_holders']}, "
          f"immediate = {r['immediate']}, "
          f"avg wait = {r['avg_wait_ms']:.1f} ms")