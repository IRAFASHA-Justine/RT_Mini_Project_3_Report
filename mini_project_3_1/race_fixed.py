"""
Mini Project 3.1 — Step 4: race fixed with a mutex (Section 3.8).

Wrapping the read-modify-write in `with counter_lock:` serialises the critical
section. No thread can read a stale value -> result is exactly 5000.
"""
import threading
import time
import sys

shared_counter = 0
counter_lock = threading.Lock()   # mutex guarding the critical section


def increment_safe(iterations):
    global shared_counter
    for _ in range(iterations):
        with counter_lock:            # critical section: atomic
            value = shared_counter
            time.sleep(0.0001)
            shared_counter = value + 1


def run(num_threads=5, iterations=1000):
    global shared_counter
    shared_counter = 0
    threads = [threading.Thread(target=increment_safe, args=(iterations,))
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