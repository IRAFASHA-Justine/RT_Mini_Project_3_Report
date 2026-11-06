
"""
Mini Project 3.1 — Steps 1-3: lost-update race condition (Section 3.8).
Demonstrates that an unprotected read-modify-write loses updates under concurrency.

Expected: 5000 (5 threads x 1000 increments)
Actual:   ~1000 (all threads fall into lock-step)
"""
import threading
import time
import sys

shared_counter = 0   # shared state, deliberately unprotected


def increment_unsafe(iterations):
    global shared_counter
    for _ in range(iterations):
        value = shared_counter       # 1. read
        time.sleep(0.0001)           # widen the interleaving window
        shared_counter = value + 1   # 2. write back (stale value possible)


def run(num_threads=5, iterations=1000):
    global shared_counter
    shared_counter = 0
    threads = [threading.Thread(target=increment_unsafe, args=(iterations,))
               for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    expected = num_threads * iterations
    lost = expected - shared_counter
    print(f"threads={num_threads} expected={expected} "
          f"actual={shared_counter} lost={lost}")
    return shared_counter


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    run(num_threads=n)