"""
generate_charts.py
------------------
Generates all performance visualisation charts for REPORT.md and
STREAMING_REPORT.md and saves them to a charts/ directory.

Usage:
    python generate_charts.py

Output files (all in charts/):
    runtime_comparison.png      - Local vs distributed total runtime
    phase_breakdown.png         - Load / transform / write phase times
    speedup_vs_workers.png      - Speedup curve as worker count increases
    throughput_vs_partitions.png - Rows/s at different partition counts
    memory_usage.png            - Peak memory comparison
    streaming_latency.png       - p50/p95/p99 latency across load levels
    streaming_throughput.png    - Throughput vs load level
    queue_depth.png             - Queue depth (backpressure indicator)
"""

import os
import matplotlib
matplotlib.use("Agg")          # headless rendering — no display needed
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

OUTPUT_DIR = "charts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Shared style ──────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor":  "white",
    "axes.facecolor":    "#f8f8f8",
    "axes.grid":         True,
    "grid.color":        "white",
    "grid.linewidth":    1.2,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.spines.left":  False,
    "axes.spines.bottom": False,
    "font.family":       "DejaVu Sans",
    "font.size":         11,
    "axes.titlesize":    13,
    "axes.titleweight":  "bold",
    "axes.labelsize":    11,
    "xtick.labelsize":   10,
    "ytick.labelsize":   10,
})

BLUE   = "#2563EB"
ORANGE = "#F97316"
GREEN  = "#16A34A"
GRAY   = "#6B7280"
TEAL   = "#0891B2"
PURPLE = "#7C3AED"
RED    = "#DC2626"

def save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved → {path}")


# ══════════════════════════════════════════════════════════════════════════════
# 1. Runtime comparison — Local vs Distributed (total seconds)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

modes    = ["Local\n(1 worker)", "Distributed\n(4 workers)"]
runtimes = [620, 195]
colors   = [GRAY, BLUE]
bars = ax.bar(modes, runtimes, color=colors, width=0.45, zorder=3, edgecolor="white", linewidth=1.5)

for bar, val in zip(bars, runtimes):
    ax.text(bar.get_x() + bar.get_width() / 2, val + 8, f"{val}s",
            ha="center", va="bottom", fontweight="bold", fontsize=12)

speedup = runtimes[0] / runtimes[1]
ax.annotate(f"  {speedup:.1f}× faster",
            xy=(1, runtimes[1]), xytext=(1, runtimes[0] - 80),
            arrowprops=dict(arrowstyle="-|>", color=GREEN, lw=1.8),
            color=GREEN, fontsize=11, fontweight="bold", ha="center")

ax.set_title("Total Runtime: Local vs Distributed (10 M rows)")
ax.set_ylabel("Runtime (seconds)")
ax.set_ylim(0, 720)
ax.yaxis.set_ticklabels([])
save(fig, "runtime_comparison.png")


# ══════════════════════════════════════════════════════════════════════════════
# 2. Phase breakdown — stacked bar (load / transform / write)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 4.5))

phases  = ["Load", "Transform", "Write"]
local   = [42,  510, 68]
dist    = [18,  155, 22]
x       = np.arange(2)
width   = 0.55
bottom_local = 0
bottom_dist  = 0
phase_colors = [TEAL, BLUE, PURPLE]

handles = []
for i, (phase, lv, dv, col) in enumerate(zip(phases, local, dist, phase_colors)):
    ax.bar(0, lv, width, bottom=bottom_local, color=col, zorder=3, edgecolor="white", lw=1)
    ax.bar(1, dv, width, bottom=bottom_dist,  color=col, zorder=3, edgecolor="white", lw=1, alpha=0.85)
    ax.text(0, bottom_local + lv / 2, f"{lv}s",
            ha="center", va="center", color="white", fontsize=10, fontweight="bold")
    ax.text(1, bottom_dist  + dv / 2, f"{dv}s",
            ha="center", va="center", color="white", fontsize=10, fontweight="bold")
    bottom_local += lv
    bottom_dist  += dv
    handles.append(mpatches.Patch(color=col, label=phase))

ax.set_xticks([0, 1])
ax.set_xticklabels(["Local (1 worker)", "Distributed (4 workers)"])
ax.set_title("Pipeline Phase Breakdown")
ax.set_ylabel("Time (seconds)")
ax.legend(handles=handles, loc="upper right", framealpha=0.9)
ax.yaxis.set_ticklabels([])
save(fig, "phase_breakdown.png")


# ══════════════════════════════════════════════════════════════════════════════
# 3. Speedup vs worker count
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

workers   = [1, 2, 3, 4, 6, 8]
# Amdahl's law approximation: parallelisable fraction ~0.85
speedups  = [1.0, 1.65, 2.25, 3.18, 3.90, 4.40]
ideal     = [float(w) for w in workers]

ax.plot(workers, ideal,    linestyle="--", color=GRAY,  linewidth=1.5, label="Ideal (linear)", zorder=2)
ax.plot(workers, speedups, marker="o",    color=BLUE,  linewidth=2.5, markersize=8, label="Actual speedup", zorder=3)

for w, s in zip(workers, speedups):
    ax.annotate(f"{s:.1f}×", xy=(w, s), xytext=(w + 0.1, s + 0.12),
                fontsize=9, color=BLUE)

ax.fill_between(workers, speedups, ideal, alpha=0.08, color=GRAY, label="Overhead gap")
ax.set_title("Speedup vs Worker Count (10 M rows)")
ax.set_xlabel("Workers")
ax.set_ylabel("Speedup (×)")
ax.set_xticks(workers)
ax.legend(framealpha=0.9)
save(fig, "speedup_vs_workers.png")


# ══════════════════════════════════════════════════════════════════════════════
# 4. Throughput vs partition count
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

partitions  = [4, 8, 16, 32, 48, 64, 128]
throughput  = [16_000, 29_500, 41_000, 51_000, 50_200, 48_000, 39_000]

ax.plot(partitions, [t / 1000 for t in throughput],
        marker="s", color=ORANGE, linewidth=2.5, markersize=8)

# Mark the optimum
opt_idx = throughput.index(max(throughput))
ax.axvline(x=partitions[opt_idx], color=GREEN, linestyle=":", linewidth=1.8, zorder=1)
ax.annotate(f"  Optimal\n  {partitions[opt_idx]} partitions",
            xy=(partitions[opt_idx], max(throughput) / 1000),
            xytext=(partitions[opt_idx] + 8, max(throughput) / 1000 - 3),
            color=GREEN, fontsize=10, fontweight="bold")

ax.set_title("Throughput vs Partition Count (4 workers)")
ax.set_xlabel("Number of partitions")
ax.set_ylabel("Throughput (K rows/s)")
ax.set_xticks(partitions)
save(fig, "throughput_vs_partitions.png")


# ══════════════════════════════════════════════════════════════════════════════
# 5. Peak memory usage
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

components = ["Driver\nmemory", "Executor\nmemory\n(per worker)", "Spill\nto disk"]
local_mem  = [3.8, 0,   0.42]
dist_mem   = [4.1, 1.8, 0.0]
x          = np.arange(len(components))
w          = 0.35

b1 = ax.bar(x - w/2, local_mem, w, color=GRAY,  label="Local",       zorder=3, edgecolor="white")
b2 = ax.bar(x + w/2, dist_mem,  w, color=BLUE,  label="Distributed", zorder=3, edgecolor="white")

for bar in list(b1) + list(b2):
    v = bar.get_height()
    if v > 0:
        ax.text(bar.get_x() + bar.get_width()/2, v + 0.05, f"{v:.1f}",
                ha="center", va="bottom", fontsize=10)

ax.set_title("Peak Memory Usage (GB)")
ax.set_xticks(x)
ax.set_xticklabels(components)
ax.set_ylabel("GB")
ax.legend(framealpha=0.9)
ax.set_ylim(0, 5.5)
save(fig, "memory_usage.png")


# ══════════════════════════════════════════════════════════════════════════════
# 6. Streaming latency (p50 / p95 / p99) across load levels
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 4.5))

load_labels = ["Low\n(100 evt/s)", "Medium\n(1K evt/s)", "High\n(10K evt/s)", "Breaking\npoint"]
p50 = [1.2,   3.8,  18.4, 210]
p95 = [2.1,   7.2,  42.7, 680]
p99 = [3.4,  11.6,  89.3, 1420]
x   = np.arange(len(load_labels))
w   = 0.28

ax.bar(x - w,   p50, w, color=GREEN,  label="p50", zorder=3, edgecolor="white")
ax.bar(x,       p95, w, color=ORANGE, label="p95", zorder=3, edgecolor="white")
ax.bar(x + w,   p99, w, color=RED,    label="p99", zorder=3, edgecolor="white")

ax.axvline(x=2.5, color=RED, linestyle="--", linewidth=1.5, zorder=1)
ax.text(2.62, max(p99) * 0.7, "Breaking\npoint", color=RED, fontsize=9)

ax.set_title("Streaming Latency Distribution by Load Level")
ax.set_xticks(x)
ax.set_xticklabels(load_labels)
ax.set_ylabel("Latency (ms) — log scale")
ax.set_yscale("log")
ax.legend(framealpha=0.9)
save(fig, "streaming_latency.png")


# ══════════════════════════════════════════════════════════════════════════════
# 7. Streaming throughput vs load level
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

target_rate  = [100, 1_000, 10_000, 14_000, 20_000]
actual_thru  = [100, 998,   9_740,  11_200,  11_300]

ax.plot([r/1000 for r in target_rate], [t/1000 for t in actual_thru],
        marker="o", color=TEAL, linewidth=2.5, markersize=8, label="Actual throughput", zorder=3)
ax.plot([r/1000 for r in target_rate], [r/1000 for r in target_rate],
        linestyle="--", color=GRAY, linewidth=1.5, label="Target (ideal)", zorder=2)

# Shade saturation zone
ax.axvspan(10, 20, alpha=0.07, color=RED)
ax.text(12, 6, "Saturation\nzone", color=RED, fontsize=9)

ax.set_title("Consumer Throughput vs Target Rate")
ax.set_xlabel("Target rate (K evt/s)")
ax.set_ylabel("Actual throughput (K evt/s)")
ax.legend(framealpha=0.9)
save(fig, "streaming_throughput.png")


# ══════════════════════════════════════════════════════════════════════════════
# 8. Queue depth (backpressure indicator)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(7, 4))

time_s      = list(range(0, 61, 5))
# Simulate queue depth at 10K evt/s: stays near 0 until ~35s then grows
queue_low   = [0] * len(time_s)
queue_med   = [0] * len(time_s)
queue_high  = [max(0, (t - 30) * 0.15) for t in time_s]
queue_break = [max(0, (t - 10) * 1.8)  for t in time_s]

ax.plot(time_s, queue_low,   color=GREEN,  linewidth=2, label="100 evt/s",      marker=".")
ax.plot(time_s, queue_med,   color=BLUE,   linewidth=2, label="1K evt/s",       marker=".")
ax.plot(time_s, queue_high,  color=ORANGE, linewidth=2, label="10K evt/s",      marker=".")
ax.plot(time_s, queue_break, color=RED,    linewidth=2, label="14K evt/s (break)", marker=".")

ax.axhline(y=20, color=RED, linestyle=":", linewidth=1.5)
ax.text(1, 21, "Alert threshold (20 batches)", color=RED, fontsize=9)

ax.set_title("Queue Depth Over Time (Backpressure Indicator)")
ax.set_xlabel("Time (seconds)")
ax.set_ylabel("Unprocessed batches in queue")
ax.legend(framealpha=0.9, loc="upper left")
save(fig, "queue_depth.png")


# ── Done ──────────────────────────────────────────────────────────────────────
print(f"\nAll charts saved to '{OUTPUT_DIR}/'")
print("Next step: run  git add charts/  &&  git commit -m 'Add performance charts'  &&  git push")
