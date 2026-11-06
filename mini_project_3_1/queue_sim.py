"""
Mini Project 3.1 — Steps 5-6: bounded queue as an RTOS message queue (Section 3.4).

A producer puts 20 timestamped items into queue.Queue(maxsize=5) and a consumer
computes per-item latency from send to receipt.
"""
import queue
import threading
import time
import json
import sys

ITEMS = 20


def run(maxsize=5, consumer_delay=0.0):
    q = queue.Queue(maxsize=maxsize)
    latencies, block_times = [], []

    def producer_task():
        for i in range(ITEMS):
            t0 = time.perf_counter()
            q.put((i, time.perf_counter()))    # blocks when full
            block_times.append(time.perf_counter() - t0)

    def consumer_task():
        for _ in range(ITEMS):
            i, sent = q.get()
            latencies.append(time.perf_counter() - sent)
            if consumer_delay:
                time.sleep(consumer_delay)

    p = threading.Thread(target=producer_task)
    c = threading.Thread(target=consumer_task)
    p.start(); c.start(); p.join(); c.join()

    blocked = sum(1 for b in block_times if b > 0.001)
    return {
        "maxsize": maxsize,
        "consumer_delay_ms": consumer_delay * 1e3,
        "min_ms": min(latencies) * 1e3,
        "max_ms": max(latencies) * 1e3,
        "avg_ms": sum(latencies) / len(latencies) * 1e3,
        "producer_blocked_puts": blocked,
        "producer_block_total_ms": sum(block_times) * 1e3,
    }


if __name__ == "__main__":
    for d in (0.0, 0.005):
        r = run(5, d)
        print(f"consumer_delay={r['consumer_delay_ms']:.0f} ms "
              f"min={r['min_ms']:.3f} max={r['max_ms']:.3f} "
              f"avg={r['avg_ms']:.3f} ms "
              f"blocked_puts={r['producer_blocked_puts']} "
              f"total_block={r['producer_block_total_ms']:.1f} ms")