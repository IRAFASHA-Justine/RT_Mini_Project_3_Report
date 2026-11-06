"""
Mini Project 3.1 — Lab Task 3: livelock.

Two threads each take one of two locks in opposite orders, try for the second
lock without blocking, and on failure release the first and retry in lock-step
(a barrier keeps them synchronised). Result: many retries, zero completed work.
"""
import threading
import time


def run(duration=1.0):
    A, B = threading.Lock(), threading.Lock()
    bar = threading.Barrier(2)
    retries = [0, 0]
    progress = [0, 0]
    stop = time.perf_counter() + duration

    def worker(idx, first, second):
        try:
            while time.perf_counter() < stop:
                first.acquire()
                bar.wait(timeout=1)                 # both hold first lock
                got = second.acquire(blocking=False)   # fails: peer holds it
                bar.wait(timeout=1)                 # both attempted
                if got:
                    progress[idx] += 1
                    second.release(); first.release(); return
                first.release()                     # back off ...
                retries[idx] += 1                   # ... and retry in lock-step
                bar.wait(timeout=1)
        except threading.BrokenBarrierError:
            pass

    t1 = threading.Thread(target=worker, args=(0, A, B))
    t2 = threading.Thread(target=worker, args=(1, B, A))
    t1.start(); t2.start(); t1.join(); t2.join()
    return retries, progress


if __name__ == "__main__":
    r, p = run()
    print(f"retries T1={r[0]} T2={r[1]} "
          f"completed work T1={p[0]} T2={p[1]}")