

## Step 6 — `mini_project_3_3/` — Priority IRQ simulator

### `mini_project_3_3/irq_sim.py`


"""
Mini Project 3.3 — priority-based interrupt latency simulator.

Top half:  runs above every task priority; occupies the CPU via irq_lock.
Bottom half: queued in a priority heap; the CPU worker runs the highest-priority
             ready bottom half in 1 ms slices, so a newly-arrived higher-priority
             bottom half preempts a running one mid-execution.

Wall-clock simulation, times in ms. Writes results/mp33.json.
"""
import threading
import time
import heapq
import itertools
import statistics
import json
import os

SLICE = 0.001  # 1 ms

BASE = [
    dict(name="A (hi)",  prio=1, period_ms=20,  top_ms=0.5, bottom_ms=3),
    dict(name="B (mid)", prio=2, period_ms=50,  top_ms=0.5, bottom_ms=8),
    dict(name="C (lo)",  prio=3, period_ms=100, top_ms=0.5, bottom_ms=15),
]


def cfg(**over):
    return [dict(s, **over.get(s["name"][0], {})) for s in BASE]


def run(sources, duration=6.0, record_window=False):
    irq_lock = threading.Lock()
    cv = threading.Condition()
    heap = []
    cnt = itertools.count()
    stats = {s["name"]: {"irq_lat": [], "top": [], "resp": []} for s in sources}
    segs = []
    preempts = [0]
    t0 = time.perf_counter()
    stop = t0 + duration
    irq_active = [0]

    def source(s):
        k = 1
        while True:
            assertion = t0 + 0.05 + k * s["period_ms"] / 1e3
            k += 1
            if assertion > stop:
                return
            d = assertion - time.perf_counter()
            if d > 0:
                time.sleep(d)
            with irq_lock:                      # top half starts
                start = time.perf_counter()
                irq_active[0] = 1
                time.sleep(s["top_ms"] / 1e3)   # top-half work
                end = time.perf_counter()
                irq_active[0] = 0
            stats[s["name"]]["irq_lat"].append((start - assertion) * 1e3)
            stats[s["name"]]["top"].append((end - start) * 1e3)
            if record_window:
                segs.append((start - t0, end - t0, s["name"], "top"))
            with cv:                            # defer to a bottom half
                heapq.heappush(heap, [s["prio"], next(cnt), assertion,
                                      s["bottom_ms"] / 1e3, s["name"]])
                cv.notify()

    def cpu():
        last = None
        while True:
            with cv:
                while not heap:
                    if time.perf_counter() > stop + 0.5:
                        return
                    cv.wait(0.05)
                task = heapq.heappop(heap)
            while irq_active[0]:
                time.sleep(0.0002)              # CPU busy with a top half
            if last is not None and last is not task and last[3] > 0:
                preempts[0] += 1
            st = time.perf_counter()
            time.sleep(min(SLICE, task[3]))
            en = time.perf_counter()
            task[3] -= min(SLICE, task[3])
            if record_window:
                segs.append((st - t0, en - t0, task[4], "bottom"))
            if task[3] <= 1e-9:
                stats[task[4]]["resp"].append((en - task[2]) * 1e3)
                last = None
            else:
                with cv:
                    heapq.heappush(heap, task)  # requeue remainder
                last = task

    ts = [threading.Thread(target=source, args=(s,)) for s in sources]
    w = threading.Thread(target=cpu)
    for t in ts:
        t.start()
    w.start()
    for t in ts:
        t.join()
    w.join()

    out = {}
    for n, d in stats.items():
        r = sorted(d["resp"])
        if not r:
            continue
        out[n] = {
            "n": len(r),
            "min": r[0],
            "mean": statistics.mean(r),
            "max": r[-1],
            "p95": r[max(0, int(0.95 * len(r)) - 1)],
            "irq_lat_mean": statistics.mean(d["irq_lat"]),
            "irq_lat_max": max(d["irq_lat"]),
            "top_mean": statistics.mean(d["top"]),
        }
    return out, segs


if __name__ == "__main__":
    res = {}
    configs = {
        "1 baseline (short top halves)": cfg(),
        "2 long top half on B": cfg(B=dict(top_ms=12)),
        "3 heavy load (A period 10 ms)": cfg(A=dict(period_ms=10)),
    }
    for name, c in configs.items():
        res[name], _ = run(c, 6.0)
        print(name, {k: round(v["max"], 1) for k, v in res[name].items()})

    sweep = {}
    for p in (40, 20, 10, 7):
        r, _ = run(cfg(A=dict(period_ms=p)), 4.0)
        sweep[str(p)] = r
        print(f"sweep A period {p} -> C max {r['C (lo)']['max']:.1f}")

    _, segs = run(BASE, 1.0, record_window=True)
    segs = [s for s in segs if 0.25 < s[0] < 0.45]

    os.makedirs("results", exist_ok=True)
    with open("results/mp33.json", "w") as f:
        json.dump({"configs": res, "sweep": sweep, "segs": segs}, f, indent=2)
    print("ok — wrote results/mp33.json")