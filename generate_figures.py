#!/usr/bin/env python3
"""
Generates ALL seven figures of "RT_Mini_Project_3_Report" (Real-Time Systems, Unit 3).

  Figure 1  race_demo      : final counter, 5 vs 10 threads, no lock      (Mini Project 3.1)
  Figure 2  semaphore      : per-task wait for Semaphore(1), (3), (6)     (Mini Project 3.1, Lab Task 2)
  Figure 3  timing         : histogram of 200 dispatch latencies          (Mini Project 3.1, Step 9)
  Figure 4  queue sweep    : avg latency vs queue maxsize                 (Mini Project 3.1, Lab Task 4)
  Figure 5  buffer sweep   : throughput + blocking time vs capacity       (Mini Project 3.2)
  Figure 6  timeline       : preemption timeline, 200 ms window           (Mini Project 3.3)
  Figure 7  arrival sweep  : response latency of C vs period of A         (Mini Project 3.3)

The experiments are the same ones as in the report appendix (race_demo, queue_sim,
semaphore_pool, timing_overhead, bounded_buffer, irq_sim), included here so this ONE file
is complete and needs no other script.  Python 3.9+, matplotlib required:  pip install matplotlib

USAGE
  python generate_figures.py                    # run every experiment, save fig1..fig7 PNGs in ./figures
  python generate_figures.py --fig 5            # only Figure 5 (runs only the experiment it needs)
  python generate_figures.py --fig 6 7          # several figures
  python generate_figures.py --quick            # shorter runs (about 15 s total), same figures, slightly noisier
  python generate_figures.py --save-json results  # also store the raw measurements (mp31/mp32/mp33.json)
  python generate_figures.py --from-json results  # redraw the figures from saved JSON, no experiments
                                                  # (works with the results/*.json written by your lab_tasks.py,
                                                  #  bb_experiments.py and irq_sim.py drivers)
  python generate_figures.py --outdir pics --dpi 200

Timing-based results depend on your machine/OS, so numbers will differ a little from the report
(the figures' shapes, which are the point, are reproduced).  On Windows time.sleep() has ~15 ms
granularity, so use --quick there or expect long runs.
"""
import argparse
import heapq
import itertools
import json
import os
import random
import statistics
import threading
import time

import matplotlib
matplotlib.use("Agg")                      # write PNG files, no window needed
import matplotlib.pyplot as plt


# =====================================================================================
#  MINI PROJECT 3.1 experiments
# =====================================================================================
def race_unsafe(num_threads=5, iterations=1000):
    """Lost-update race: read, 0.1 ms sleep, write back (no lock). Returns final counter."""
    counter = [0]

    def inc():
        for _ in range(iterations):
            value = counter[0]                 # 1. read
            time.sleep(0.0001)                 # widen the interleaving window
            counter[0] = value + 1             # 2. write back (stale value possible)

    ts = [threading.Thread(target=inc) for _ in range(num_threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return counter[0]


def queue_run(maxsize=5, consumer_delay=0.0, items=20):
    import queue
    q = queue.Queue(maxsize=maxsize)
    latencies, block_times = [], []

    def producer():
        for i in range(items):
            t0 = time.perf_counter()
            q.put((i, time.perf_counter()))    # blocks while the queue is full
            block_times.append(time.perf_counter() - t0)

    def consumer():
        for _ in range(items):
            _, sent = q.get()
            latencies.append(time.perf_counter() - sent)
            if consumer_delay:
                time.sleep(consumer_delay)     # slow consumer

    p, c = threading.Thread(target=producer), threading.Thread(target=consumer)
    p.start(); c.start(); p.join(); c.join()
    return {"maxsize": maxsize, "consumer_delay_ms": consumer_delay * 1e3,
            "min_ms": min(latencies) * 1e3, "max_ms": max(latencies) * 1e3,
            "avg_ms": sum(latencies) / len(latencies) * 1e3,
            "producer_blocked_puts": sum(1 for b in block_times if b > 0.001)}


def semaphore_run(count=3, tasks=8, seed=1):
    random.seed(seed)
    sem = threading.Semaphore(count)
    waits = {}
    holds = [random.uniform(0.05, 0.15) for _ in range(tasks)]

    def task(tid):
        t0 = time.perf_counter()
        sem.acquire()
        waits[tid] = time.perf_counter() - t0
        time.sleep(holds[tid])                 # use the resource
        sem.release()

    ts = [threading.Thread(target=task, args=(i,)) for i in range(tasks)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return {"count": count, "waits_ms": [waits[i] * 1e3 for i in range(tasks)]}


def dispatch_samples(n=200):
    """Time from starting a thread that sets an Event until event.wait() returns (microseconds)."""
    out = []
    for _ in range(n):
        ev = threading.Event()
        t = threading.Thread(target=ev.set)
        t0 = time.perf_counter()
        t.start()
        ev.wait()
        out.append((time.perf_counter() - t0) * 1e6)
        t.join()
    return out


def collect_mp31(quick=False, need=("1", "2", "3", "4")):
    res = {}
    if "1" in need:
        res["race5"] = [race_unsafe(5) for _ in range(3)]
        res["race10"] = [race_unsafe(10) for _ in range(3)]
    if "2" in need:
        res["sem"] = {c: semaphore_run(c) for c in (1, 3, 6)}
    if "3" in need:
        res["timing"] = {"samples": dispatch_samples(200)}
    if "4" in need:
        res["queue_sweep"] = [queue_run(m, 0.002) for m in (1, 2, 5, 10, 20)]
    return res


# =====================================================================================
#  MINI PROJECT 3.2 experiment (corrected version: global lock order buffer -> stats)
# =====================================================================================
def buffer_fixed(capacity=5, n_prod=3, n_cons=3, total=600, seed=7):
    random.seed(seed)
    buffer, buffer_lock, stats_lock = [], threading.Lock(), threading.Lock()
    stats = {"produced": 0, "consumed": 0, "prod_wait": 0.0, "cons_wait": 0.0}
    per_p, per_c = total // n_prod, total // n_cons

    def producer():
        for _ in range(per_p):
            time.sleep(random.uniform(0.0005, 0.005))          # make the item
            t0 = time.perf_counter()
            while True:
                with buffer_lock:                              # lock order: buffer first ...
                    if len(buffer) < capacity:
                        buffer.append(1)
                        with stats_lock:                       # ... then stats
                            stats["produced"] += 1
                            stats["prod_wait"] += time.perf_counter() - t0
                        break
                time.sleep(0.0003)                             # buffer full: back off

    def consumer():
        for _ in range(per_c):
            t0 = time.perf_counter()
            while True:
                with buffer_lock:
                    if buffer:
                        buffer.pop()
                        with stats_lock:
                            stats["consumed"] += 1
                            stats["cons_wait"] += time.perf_counter() - t0
                        break
                time.sleep(0.0003)                             # buffer empty: back off
            time.sleep(random.uniform(0.0005, 0.005))          # use the item

    ts = [threading.Thread(target=producer, daemon=True) for _ in range(n_prod)] + \
         [threading.Thread(target=consumer, daemon=True) for _ in range(n_cons)]
    t_start = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = time.perf_counter() - t_start
    return {"capacity": capacity, "throughput": total / elapsed,
            "avg_prod_block_ms": stats["prod_wait"] / total * 1e3,
            "avg_cons_block_ms": stats["cons_wait"] / total * 1e3}


def collect_mp32(quick=False):
    caps = (1, 2, 5, 10, 25, 50)
    total = 300 if quick else 600
    reps = 2 if quick else 3
    sweep = []
    for cap in caps:
        runs = [buffer_fixed(cap, total=total, seed=s) for s in range(reps)]
        sweep.append({"capacity": cap,
                      **{k: sum(r[k] for r in runs) / reps
                         for k in ("throughput", "avg_prod_block_ms", "avg_cons_block_ms")}})
        print(f"  capacity {cap:>3}: {sweep[-1]['throughput']:.0f} items/s")
    return {"sweep": sweep}


# =====================================================================================
#  MINI PROJECT 3.3 experiment (top half / bottom half, strict priority, 1 ms slices)
# =====================================================================================
SLICE = 0.001
BASE = [dict(name="A (hi)", prio=1, period_ms=20, top_ms=0.5, bottom_ms=3),
        dict(name="B (mid)", prio=2, period_ms=50, top_ms=0.5, bottom_ms=8),
        dict(name="C (lo)", prio=3, period_ms=100, top_ms=0.5, bottom_ms=15)]


def cfg(**over):
    return [dict(s, **over.get(s["name"][0], {})) for s in BASE]


def irq_run(sources, duration=6.0, record_window=False):
    irq_lock = threading.Lock()                # top halves occupy the CPU one at a time
    cv = threading.Condition()
    heap, cnt = [], itertools.count()
    stats = {s["name"]: {"irq_lat": [], "top": [], "resp": []} for s in sources}
    segs = []
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
            with irq_lock:                                     # top half starts
                start = time.perf_counter(); irq_active[0] = 1
                time.sleep(s["top_ms"] / 1e3)                  # top-half work
                end = time.perf_counter(); irq_active[0] = 0
            stats[s["name"]]["irq_lat"].append((start - assertion) * 1e3)
            stats[s["name"]]["top"].append((end - start) * 1e3)
            if record_window:
                segs.append((start - t0, end - t0, s["name"], "top"))
            with cv:                                           # defer the rest: bottom half
                heapq.heappush(heap, [s["prio"], next(cnt), assertion, s["bottom_ms"] / 1e3, s["name"]])
                cv.notify()

    def cpu():
        while True:
            with cv:
                while not heap:
                    if time.perf_counter() > stop + 0.5:
                        return
                    cv.wait(0.05)
                task = heapq.heappop(heap)                     # highest priority ready bottom half
            while irq_active[0]:
                time.sleep(0.0002)                             # CPU busy with a top half
            st = time.perf_counter()
            time.sleep(min(SLICE, task[3]))                    # run one 1 ms slice
            en = time.perf_counter()
            task[3] -= min(SLICE, task[3])
            if record_window:
                segs.append((st - t0, en - t0, task[4], "bottom"))
            if task[3] <= 1e-9:
                stats[task[4]]["resp"].append((en - task[2]) * 1e3)
            else:
                with cv:
                    heapq.heappush(heap, task)                 # requeue remainder (preemptible)

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
        out[n] = {"n": len(r), "min": r[0], "mean": statistics.mean(r), "max": r[-1],
                  "p95": r[max(0, int(0.95 * len(r)) - 1)],
                  "irq_lat_mean": statistics.mean(d["irq_lat"]), "irq_lat_max": max(d["irq_lat"]),
                  "top_mean": statistics.mean(d["top"])}
    return out, segs


def collect_mp33(quick=False, need=("6", "7")):
    res = {}
    if "7" in need:
        d = 2.0 if quick else 4.0
        res["sweep"] = {}
        for p in (40, 20, 10, 7):
            r, _ = irq_run(cfg(A=dict(period_ms=p)), d)
            res["sweep"][p] = r
            print(f"  A period {p:>2} ms -> C mean {r['C (lo)']['mean']:.1f}  max {r['C (lo)']['max']:.1f}")
    if "6" in need:
        _, segs = irq_run(BASE, 1.0, record_window=True)
        res["segs"] = [s for s in segs if 0.25 < s[0] < 0.45]
    return res


# =====================================================================================
#  PLOTTING  (one function per figure; each takes the collected data dict)
# =====================================================================================
def _save(fig, outdir, name, dpi):
    path = os.path.join(outdir, name)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)
    print("  wrote", path)


def fig1(d, outdir, dpi):
    fig, ax = plt.subplots(figsize=(5.8, 2.7))
    x, w = range(3), 0.4
    ax.bar([i - w / 2 for i in x], d["race5"], w, label="5 threads (expected 5000)")
    ax.bar([i + w / 2 for i in x], d["race10"], w, label="10 threads (expected 10000)")
    ax.set_xticks(list(x)); ax.set_xticklabels([f"run {i+1}" for i in x])
    ax.set_ylabel("final counter")
    ax.set_title("race_demo.py: unprotected counter")
    ax.set_ylim(0, max(d["race5"] + d["race10"]) * 1.35)
    ax.legend(loc="upper right"); ax.grid(alpha=0.3)
    _save(fig, outdir, "fig1_race_demo.png", dpi)


def fig2(d, outdir, dpi):
    counts = sorted(int(c) for c in d["sem"])
    fig, axes = plt.subplots(1, 3, figsize=(7.7, 2.7), sharey=True)
    for ax, c in zip(axes, counts):
        waits = d["sem"][c]["waits_ms"] if c in d["sem"] else d["sem"][str(c)]["waits_ms"]
        ax.bar(range(len(waits)), waits)
        ax.set_title(f"Semaphore({c})"); ax.set_xlabel("task"); ax.grid(alpha=0.3)
    axes[0].set_ylabel("wait (ms)")
    _save(fig, outdir, "fig2_semaphore_waits.png", dpi)


def fig3(d, outdir, dpi):
    s = d["timing"]["samples"]
    fig, ax = plt.subplots(figsize=(5.8, 2.7))
    ax.hist(s, bins=40)
    ax.set_yscale("log")
    ax.set_xlabel("dispatch latency (µs)"); ax.set_ylabel("count (log)")
    ax.set_title(f"timing_overhead.py: {len(s)} samples")
    ax.grid(alpha=0.3)
    _save(fig, outdir, "fig3_dispatch_latency.png", dpi)


def fig4(d, outdir, dpi):
    q = d["queue_sweep"]
    fig, ax = plt.subplots(figsize=(5.8, 2.7))
    ax.plot([r["maxsize"] for r in q], [r["avg_ms"] for r in q], "o-")
    ax.set_xlabel("queue maxsize"); ax.set_ylabel("avg latency (ms)")
    ax.set_title("queue_sim.py: latency vs queue size (consumer delay 2 ms)")
    ax.grid(alpha=0.3)
    _save(fig, outdir, "fig4_queue_latency.png", dpi)


def fig5(d, outdir, dpi):
    sw = d["sweep"]
    cap = [r["capacity"] for r in sw]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(7.7, 2.8))
    a1.semilogx(cap, [r["throughput"] for r in sw], "o-")
    a1.set_title("Throughput"); a1.set_xlabel("buffer capacity"); a1.set_ylabel("items/s")
    a2.semilogx(cap, [r["avg_prod_block_ms"] for r in sw], "o-", label="producer")
    a2.semilogx(cap, [r["avg_cons_block_ms"] for r in sw], "s-", label="consumer")
    a2.set_title("Blocking time"); a2.set_xlabel("buffer capacity"); a2.set_ylabel("avg blocking (ms/item)")
    a2.legend()
    for a in (a1, a2):
        a.grid(alpha=0.3)
    _save(fig, outdir, "fig5_buffer_capacity.png", dpi)


def fig6(d, outdir, dpi):
    segs = d["segs"]
    row = {"A (hi)": 0, "B (mid)": 1, "C (lo)": 2}
    color = {"A (hi)": "#2f7ac0", "B (mid)": "#6aa84f", "C (lo)": "#ef7d2d"}
    fig, ax = plt.subplots(figsize=(7.4, 2.6))
    for st, en, name, kind in segs:
        ax.barh(row[name], (en - st) * 1e3, left=st * 1e3, height=0.7,
                color="#c0392b" if kind == "top" else color[name])
    ax.set_yticks(list(row.values())); ax.set_yticklabels(list(row.keys()))
    lo = min(s[0] for s in segs) * 1e3; hi = max(s[1] for s in segs) * 1e3
    ax.set_xlim(lo - 8, hi + 8)
    ax.set_xlabel("time (ms)")
    ax.set_title("Preemption timeline (red = top half, colour = bottom-half slices)")
    ax.grid(alpha=0.3)
    _save(fig, outdir, "fig6_preemption_timeline.png", dpi)


def fig7(d, outdir, dpi):
    periods = sorted((int(p) for p in d["sweep"]), reverse=True)
    get = lambda p: d["sweep"][p]["C (lo)"] if p in d["sweep"] else d["sweep"][str(p)]["C (lo)"]
    fig, ax = plt.subplots(figsize=(5.8, 2.7))
    ax.plot(periods, [get(p)["max"] for p in periods], "o-", label="max")
    ax.plot(periods, [get(p)["mean"] for p in periods], "s-", label="mean")
    ax.invert_xaxis()                              # faster arrivals (shorter period) to the right
    ax.set_xlabel("period of highest-priority source A (ms)")
    ax.set_ylabel("C response latency (ms)")
    ax.set_title("Arrival-rate sweep")
    ax.legend(); ax.grid(alpha=0.3)
    _save(fig, outdir, "fig7_arrival_rate_sweep.png", dpi)


FIGS = {"1": ("mp31", fig1), "2": ("mp31", fig2), "3": ("mp31", fig3), "4": ("mp31", fig4),
        "5": ("mp32", fig5), "6": ("mp33", fig6), "7": ("mp33", fig7)}


# =====================================================================================
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--fig", nargs="+", choices=sorted(FIGS), help="figure numbers (default: all)")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--quick", action="store_true", help="shorter runs")
    ap.add_argument("--save-json", metavar="DIR", help="store raw measurements as DIR/mp3x.json")
    ap.add_argument("--from-json", metavar="DIR", help="redraw from DIR/mp31.json, mp32.json, mp33.json")
    a = ap.parse_args()

    wanted = a.fig or sorted(FIGS)
    os.makedirs(a.outdir, exist_ok=True)
    data = {}

    if a.from_json:
        for key in {FIGS[f][0] for f in wanted}:
            with open(os.path.join(a.from_json, key + ".json")) as f:
                data[key] = json.load(f)
        print(f"loaded results from {a.from_json}/")
    else:
        need31 = tuple(f for f in wanted if FIGS[f][0] == "mp31")
        need33 = tuple(f for f in wanted if FIGS[f][0] == "mp33")
        if need31:
            print("Running Mini Project 3.1 experiments ...")
            data["mp31"] = collect_mp31(a.quick, need31)
        if any(FIGS[f][0] == "mp32" for f in wanted):
            print("Running Mini Project 3.2 buffer sweep ...")
            data["mp32"] = collect_mp32(a.quick)
        if need33:
            print("Running Mini Project 3.3 interrupt simulator ...")
            data["mp33"] = collect_mp33(a.quick, need33)
        if a.save_json:
            os.makedirs(a.save_json, exist_ok=True)
            for key, val in data.items():
                with open(os.path.join(a.save_json, key + ".json"), "w") as f:
                    json.dump(val, f, default=str)
            print(f"saved raw measurements to {a.save_json}/")

    print("Drawing figures ...")
    for f in wanted:
        key, fn = FIGS[f]
        fn(data[key], a.outdir, a.dpi)
    print("done.")


if __name__ == "__main__":
    main()
