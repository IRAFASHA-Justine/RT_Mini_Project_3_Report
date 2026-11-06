# Real-Time Systems — Mini Project 3 (Group 4)

**Course:** Real Time Systems · Unit 3 — Concurrency, Synchronization, Deadlock and Interrupt Latency
**Institution:** University of Rwanda · CST · Computer Engineering · Level IV · Gako Campus

**Group 4 members:**
- Abdul SIBO — 223026681
- Justine IRAFASHA — 223026693
- Fils NIYONZIMA — 223026876

**Submission date:** 27/10/2026

---

## What this repository contains

| Folder | Contents |
|---|---|
| `report/` | The full written report (PDF + DOCX) |
| `figures/` | The 7 figures embedded in the report |
| `results/` | Raw JSON measurements from every experiment |
| `mini_project_3_1/` | Concurrency, race conditions, queue, semaphore, dispatch latency, livelock |
| `mini_project_3_2/` | Bounded-buffer producer/consumer with deadlock and lock-ordering fix |
| `mini_project_3_3/` | Priority-based interrupt latency simulator (top half / bottom half) |
| `generate_figures.py` | One script that reruns every experiment and draws all 7 figures |

---

## Quick start

```bash
# 1. Install the only external dependency
pip install -r requirements.txt

# 2. Run every experiment and generate all 7 figures (~30-60 s)
python generate_figures.py --save-json results

# 3. Or faster (~15 s, slightly noisier results)
python generate_figures.py --quick

# 4. Or redraw figures from the already-saved JSON (no experiments)
python generate_figures.py --from-json results

# 5. Generate only specific figures
python generate_figures.py --fig 5 6 7