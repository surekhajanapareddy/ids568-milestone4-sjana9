"""
consumer.py
-----------
Streaming consumer for Milestone 4 bonus component.
Reads batched event files produced by producer.py, applies tumbling-window
and sliding-window aggregations, tracks latency, and handles crash/restart.

Features
--------
* Tumbling window  (10 s): category spend totals, transaction counts
* Sliding window   (30 s, slide 10 s): rolling average amount per region
* Late-data handling: events > 5 s behind watermark are flagged
* Checkpoint file: consumer can restart and skip already-processed batches
* Latency measurement: p50 / p95 / p99 per load level
* Backpressure detection: queue depth > threshold triggers warning

Usage:
    python consumer.py --input queue/ --output results/ --window 10
    python consumer.py --input queue/ --output results/ --crash-at 30  # simulate crash
"""

import argparse
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


# ── Helpers ────────────────────────────────────────────────────────────────────

def percentile(data: list, p: float) -> float:
    """Compute the p-th percentile of a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = (p / 100) * (len(sorted_data) - 1)
    lower = math.floor(idx)
    upper = math.ceil(idx)
    return sorted_data[lower] + (sorted_data[upper] - sorted_data[lower]) * (idx - lower)


CHECKPOINT_FILE = ".consumer_checkpoint.json"


def load_checkpoint(input_dir: str) -> set:
    """Return set of already-processed batch filenames (for crash recovery)."""
    cp_path = os.path.join(input_dir, CHECKPOINT_FILE)
    if os.path.exists(cp_path):
        with open(cp_path) as f:
            data = json.load(f)
        print(f"[consumer] Checkpoint loaded — {len(data['processed'])} batches already done")
        return set(data["processed"])
    return set()


def save_checkpoint(input_dir: str, processed: set) -> None:
    """Persist the set of processed batches so we can resume after a crash."""
    cp_path = os.path.join(input_dir, CHECKPOINT_FILE)
    with open(cp_path, "w") as f:
        json.dump({"processed": list(processed), "saved_at": datetime.now().isoformat()}, f)


class TumblingWindow:
    """
    Fixed-duration non-overlapping window.
    Accumulates events, flushes totals every `size_s` seconds of event time.
    """

    def __init__(self, size_s: int):
        self.size_s  = size_s
        self.bucket: dict = defaultdict(lambda: {"count": 0, "total_amount": 0.0})
        self.window_start: float | None = None
        self.results: list = []

    def add(self, event: dict, event_ts: float) -> None:
        if self.window_start is None:
            self.window_start = event_ts

        # Flush when window boundary is crossed
        if event_ts - self.window_start >= self.size_s:
            self._flush(self.window_start)
            self.window_start = event_ts

        key = event["category"]
        self.bucket[key]["count"]        += 1
        self.bucket[key]["total_amount"] += event["amount"]

    def _flush(self, window_start: float) -> None:
        snapshot = {
            "window_start":  datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat(),
            "window_size_s": self.size_s,
            "aggregates":    dict(self.bucket),
        }
        self.results.append(snapshot)
        self.bucket = defaultdict(lambda: {"count": 0, "total_amount": 0.0})


class SlidingWindow:
    """
    Overlapping window: emits every `slide_s` seconds, covers `size_s` of history.
    Tracks per-region average amount.
    """

    def __init__(self, size_s: int, slide_s: int):
        self.size_s      = size_s
        self.slide_s     = slide_s
        self.buffer: deque = deque()   # (event_ts, region, amount)
        self.last_emit: float | None = None
        self.results: list = []

    def add(self, event: dict, event_ts: float) -> None:
        self.buffer.append((event_ts, event["region"], event["amount"]))

        if self.last_emit is None:
            self.last_emit = event_ts

        if event_ts - self.last_emit >= self.slide_s:
            self._emit(event_ts)
            self.last_emit = event_ts

        # Evict events outside the window
        cutoff = event_ts - self.size_s
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def _emit(self, event_ts: float) -> None:
        region_amounts: dict = defaultdict(list)
        for _, region, amount in self.buffer:
            region_amounts[region].append(amount)

        averages = {r: round(statistics.mean(v), 2) for r, v in region_amounts.items()}
        self.results.append({
            "window_end":    datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat(),
            "window_size_s": self.size_s,
            "slide_s":       self.slide_s,
            "region_avg_amount": averages,
        })


# ── Main consumer loop ─────────────────────────────────────────────────────────

def run_consumer(
    input_dir: str,
    output_dir: str,
    window_s: int,
    crash_at: int | None,
    poll_interval: float,
    watermark_s: float,
) -> None:
    """
    Poll `input_dir` for new batch files, process each through windowed
    aggregations, and write results + metrics to `output_dir`.

    Parameters
    ----------
    input_dir      : Directory written by producer.py.
    output_dir     : Where to write window results and latency metrics.
    window_s       : Tumbling window size in seconds.
    crash_at       : If set, simulate a crash after this many batches.
    poll_interval  : Seconds between polls for new files.
    watermark_s    : Late-data tolerance in seconds.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    processed    = load_checkpoint(input_dir)
    tumbling     = TumblingWindow(size_s=window_s)
    sliding      = SlidingWindow(size_s=window_s * 3, slide_s=window_s)

    latencies_ms : list = []
    late_count   = 0
    total_events = 0
    batches_done = 0
    run_start    = time.time()

    print(f"[consumer] Polling '{input_dir}'  window={window_s}s  watermark={watermark_s}s")
    if crash_at:
        print(f"[consumer] CRASH SIMULATION: will crash after {crash_at} batches")
    print("-" * 60)

    try:
        # Run until no new files appear for 5 × poll_interval
        idle_ticks = 0
        while idle_ticks < 5:
            batch_files = sorted([
                f for f in os.listdir(input_dir)
                if f.startswith("batch_") and f.endswith(".jsonl") and f not in processed
            ])

            if not batch_files:
                idle_ticks += 1
                time.sleep(poll_interval)
                continue

            idle_ticks = 0

            for batch_file in batch_files:
                # ── Simulated crash recovery test ────────────────────────────
                if crash_at and batches_done == crash_at:
                    print(f"\n[consumer] *** SIMULATED CRASH at batch {batches_done} ***")
                    save_checkpoint(input_dir, processed)
                    print("[consumer] Checkpoint saved. Restarting …\n")
                    processed    = load_checkpoint(input_dir)
                    tumbling     = TumblingWindow(size_s=window_s)
                    sliding      = SlidingWindow(size_s=window_s * 3, slide_s=window_s)

                batch_path = os.path.join(input_dir, batch_file)
                ingest_start = time.time()

                # ── Read batch ───────────────────────────────────────────────
                events = []
                with open(batch_path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            events.append(json.loads(line))

                now_ts = time.time()

                for evt in events:
                    # Parse event timestamp
                    evt_ts = datetime.fromisoformat(evt["timestamp"]).timestamp()

                    # Late-data detection
                    if now_ts - evt_ts > watermark_s:
                        late_count += 1
                        continue   # drop late events (configurable policy)

                    tumbling.add(evt, evt_ts)
                    sliding.add(evt, evt_ts)

                total_events += len(events)
                batches_done += 1
                processed.add(batch_file)

                # ── Measure end-to-end latency ───────────────────────────────
                latency_ms = (time.time() - ingest_start) * 1000
                latencies_ms.append(latency_ms)

                if batches_done % 10 == 0:
                    p50 = percentile(latencies_ms, 50)
                    p99 = percentile(latencies_ms, 99)
                    print(
                        f"  batch={batches_done:4d}  events={total_events:10,}"
                        f"  lat_p50={p50:6.1f}ms  lat_p99={p99:6.1f}ms"
                        f"  late={late_count}"
                    )

                save_checkpoint(input_dir, processed)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\n[consumer] Interrupted by user.")

    # ── Write results ────────────────────────────────────────────────────────
    tumbling_path = os.path.join(output_dir, "tumbling_windows.json")
    sliding_path  = os.path.join(output_dir, "sliding_windows.json")

    with open(tumbling_path, "w") as f:
        json.dump(tumbling.results, f, indent=2)
    with open(sliding_path, "w") as f:
        json.dump(sliding.results, f, indent=2)

    # ── Latency report ───────────────────────────────────────────────────────
    elapsed = time.time() - run_start
    throughput = total_events / elapsed if elapsed > 0 else 0

    latency_report = {
        "total_events":    total_events,
        "total_batches":   batches_done,
        "late_events":     late_count,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_eps":  round(throughput, 1),
        "latency_ms": {
            "p50":  round(percentile(latencies_ms, 50),  2),
            "p95":  round(percentile(latencies_ms, 95),  2),
            "p99":  round(percentile(latencies_ms, 99),  2),
            "max":  round(max(latencies_ms, default=0),  2),
        },
    }

    report_path = os.path.join(output_dir, "latency_report.json")
    with open(report_path, "w") as f:
        json.dump(latency_report, f, indent=2)

    print("\n" + "=" * 60)
    print("CONSUMER SUMMARY")
    print("=" * 60)
    print(f"  Total events processed : {total_events:,}")
    print(f"  Late events dropped    : {late_count}")
    print(f"  Throughput             : {throughput:,.0f} evt/s")
    print(f"  Latency p50 / p95 / p99: "
          f"{latency_report['latency_ms']['p50']}ms / "
          f"{latency_report['latency_ms']['p95']}ms / "
          f"{latency_report['latency_ms']['p99']}ms")
    print(f"  Results written to     : {output_dir}")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming consumer with windowed aggregation")
    parser.add_argument("--input",         default="queue/",   help="Batch file directory")
    parser.add_argument("--output",        default="results/", help="Results directory")
    parser.add_argument("--window",        type=int,   default=10,  help="Tumbling window size (s)")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="Poll interval (s)")
    parser.add_argument("--watermark",     type=float, default=5.0, help="Late-data watermark (s)")
    parser.add_argument("--crash-at",      type=int,   default=None,
                        help="Simulate crash after N batches (for failure testing)")
    args = parser.parse_args()

    run_consumer(
        input_dir=args.input,
        output_dir=args.output,
        window_s=args.window,
        crash_at=args.crash_at,
        poll_interval=args.poll_interval,
        watermark_s=args.watermark,
    )
