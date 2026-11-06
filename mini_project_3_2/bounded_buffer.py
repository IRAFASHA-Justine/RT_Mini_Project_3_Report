"""
Mini Project 3.2 — bounded-buffer producer/consumer with deadlock reproduction
and lock-ordering fix.

Flawed:  producers take buffer_lock -> stats_lock,
         consumers take stats_lock  -> buffer_lock  (circular wait).
Fixed:   every task takes buffer_lock -> stats_lock (global lock order).
"""
import threading
import time
import random
import sys
import json


def simulate(flawed, capacity=5, n_prod=3, n_cons=3, total=600,
             watchdog_s=1.0, seed=7, verbose=False):
    random.seed(seed)
    buffer = []
    buffer_lock = threading.Lock()
    stats_lock = threading.Lock()
    stats = {"produced": 0, "consumed": 0,
             "prod_wait": 0.0, "cons_wait": 0.0}
    state = {}          # thread name -> "holds X / waiting for Y"
    done = threading.Event()
    deadlock = {}
    per = total // n_prod

    def acquire(name, lock, lock_name, held):
        state[name] = f"holds {held or 'nothing'}, WAITING for {lock_name}"
        lock.acquire()
        state[name] = f"holds {(held + ', ' if held else '') + lock_name}"

    def producer(name):
        for _ in range(per):
            time.sleep(random.uniform(0.0005, 0.005))    # produce the item
            t0 = time.perf_counter()
            while True:
                if flawed:
                    acquire(name, buffer_lock, "buffer_lock", "")
                    if len(buffer) < capacity:
                        buffer.append(1)
                        time.sleep(0.001)                # widen the window
                        acquire(name, stats_lock, "stats_lock", "buffer_lock")
                        stats["produced"] += 1
                        stats["prod_wait"] += time.perf_counter() - t0
                        stats_lock.release()
                        buffer_lock.release()
                        break
                    buffer_lock.release()
                else:
                    with buffer_lock:
                        if len(buffer) < capacity:
                            buffer.append(1)
                            with stats_lock:
                                stats["produced"] += 1
                                stats["prod_wait"] += time.perf_counter() - t0
                            break
                    time.sleep(0.0003)                   # full: back off
        state[name] = "finished"

    def consumer(name):
        for _ in range(total // n_cons):
            t0 = time.perf_counter()
            while True:
                if flawed:
                    acquire(name, stats_lock, "stats_lock", "")
                    time.sleep(0.001)
                    acquire(name, buffer_lock, "buffer_lock", "stats_lock")
                    if buffer:
                        buffer.pop()
                        stats["consumed"] += 1
                        stats["cons_wait"] += time.perf_counter() - t0
                        buffer_lock.release()
                        stats_lock.release()
                        break
                    buffer_lock.release()
                    stats_lock.release()
                else:
                    with buffer_lock:
                        if buffer:
                            buffer.pop()
                            with stats_lock:
                                stats["consumed"] += 1
                                stats["cons_wait"] += time.perf_counter() - t0
                            break
                    time.sleep(0.0003)
            time.sleep(random.uniform(0.0005, 0.005))    # consume the item
        state[name] = "finished"

    def watchdog():
        last, last_t = -1, time.perf_counter()
        while not done.is_set():
            time.sleep(0.1)
            if stats["produced"] != last:
                last, last_t = stats["produced"], time.perf_counter()
            elif time.perf_counter() - last_t > watchdog_s:
                deadlock.update(
                    produced=stats["produced"],
                    consumed=stats["consumed"],
                    snapshot=dict(state),
                    stalled_for=time.perf_counter() - last_t,
                )
                done.set()
                return

    threads = ([threading.Thread(target=producer, args=(f"P{i+1}",), daemon=True)
                for i in range(n_prod)] +
               [threading.Thread(target=consumer, args=(f"C{i+1}",), daemon=True)
                for i in range(n_cons)])

    wd = threading.Thread(target=watchdog, daemon=True)
    t_start = time.perf_counter()
    wd.start()
    for t in threads:
        t.start()
    while not done.is_set() and any(t.is_alive() for t in threads):
        time.sleep(0.02)
    elapsed = time.perf_counter() - t_start

    if deadlock:
        return {"deadlock": True, **deadlock, "elapsed": elapsed}

    done.set()
    return {
        "deadlock": False,
        "elapsed": elapsed,
        "capacity": capacity,
        "throughput": total / elapsed,
        "avg_prod_block_ms": stats["prod_wait"] / total * 1e3,
        "avg_cons_block_ms": stats["cons_wait"] / total * 1e3,
    }


if __name__ == "__main__":
    flawed = "--flawed" in sys.argv
    r = simulate(flawed)
    if r["deadlock"]:
        print(f"[WATCHDOG] DEADLOCK: produced count frozen at "
              f"{r['produced']} for {r['stalled_for']:.1f} s "
              f"(consumed={r['consumed']})")
        for k, v in sorted(r["snapshot"].items()):
            print(f"  {k}: {v}")
    else:
        print(f"completed in {r['elapsed']:.2f} s, "
              f"throughput = {r['throughput']:.0f} items/s")