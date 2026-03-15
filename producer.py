"""
producer.py
-----------
Streaming event producer for Milestone 4 bonus component.
Writes JSON-serialised transaction events to a shared in-memory queue
(or a local file-based queue directory) at a configurable rate,
simulating realistic burst + steady-state traffic patterns.

Usage:
    python producer.py --rate 500  --duration 60  --output queue/
    python producer.py --rate 1000 --duration 60  --output queue/
    python producer.py --rate 10000 --duration 60 --output queue/
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Constants ──────────────────────────────────────────────────────────────────
CATEGORIES   = ["electronics", "clothing", "food", "sports", "books", "home"]
REGIONS      = ["north", "south", "east", "west", "central"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]

BURST_PROBABILITY  = 0.05    # 5 % chance of starting a burst at each second
BURST_MULTIPLIER   = 3.0     # burst = 3× normal rate
BURST_DURATION_S   = 3       # burst lasts 3 seconds


def make_event(seq: int, rng: random.Random) -> dict:
    """Fabricate one transaction event."""
    return {
        "event_id":        seq,
        "user_id":         rng.randint(1, 100_000),
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "amount":          round(rng.lognormvariate(3.5, 1.2), 2),
        "category":        rng.choice(CATEGORIES),
        "region":          rng.choice(REGIONS),
        "device_type":     rng.choice(DEVICE_TYPES),
        "session_duration": rng.randint(30, 3600),
        "items_in_cart":   rng.randint(1, 20),
        "is_returned":     int(rng.random() < 0.12),
        "discount_pct":    round(rng.uniform(0, 0.5), 3),
        "page_views":      rng.randint(1, 50),
    }


def run_producer(rate: int, duration: int, output_dir: str, seed: int) -> None:
    """
    Produce events at `rate` events/second for `duration` seconds.

    Events are written as newline-delimited JSON into rolling batch files
    inside `output_dir`. Each second's batch becomes one file so the consumer
    can pick up complete batches atomically.

    Parameters
    ----------
    rate       : Target events per second (steady-state).
    duration   : How many seconds to run.
    output_dir : Directory for event batch files.
    seed       : RNG seed for reproducibility.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    rng  = random.Random(seed)
    seq  = 0
    total_produced = 0

    print(f"[producer] Starting — rate={rate} evt/s  duration={duration}s  seed={seed}")
    print(f"[producer] Output  → {output_dir}")
    print("-" * 60)

    burst_remaining = 0

    for tick in range(duration):
        tick_start = time.time()

        # ── Decide effective rate this second ────────────────────────────────
        if burst_remaining > 0:
            effective_rate = int(rate * BURST_MULTIPLIER)
            burst_remaining -= 1
        elif rng.random() < BURST_PROBABILITY:
            effective_rate  = int(rate * BURST_MULTIPLIER)
            burst_remaining = BURST_DURATION_S - 1
            print(f"  [producer] BURST started at tick {tick}  ({effective_rate} evt/s)")
        else:
            effective_rate = rate

        # ── Generate this second's events ────────────────────────────────────
        events = [make_event(seq + i, rng) for i in range(effective_rate)]
        seq           += effective_rate
        total_produced += effective_rate

        # ── Write batch file ─────────────────────────────────────────────────
        batch_path = os.path.join(output_dir, f"batch_{tick:06d}.jsonl")
        with open(batch_path, "w") as f:
            for evt in events:
                f.write(json.dumps(evt) + "\n")

        # ── Rate control ─────────────────────────────────────────────────────
        elapsed  = time.time() - tick_start
        sleep_for = max(0.0, 1.0 - elapsed)
        time.sleep(sleep_for)

        if tick % 10 == 0 or burst_remaining > 0:
            print(f"  tick={tick:4d}  this_sec={effective_rate:6,}  total={total_produced:10,}")

    print("-" * 60)
    print(f"[producer] Done — {total_produced:,} events in {duration}s")
    print(f"[producer] Avg rate: {total_produced / duration:,.0f} evt/s")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming event producer")
    parser.add_argument("--rate",     type=int, default=500,    help="Events per second")
    parser.add_argument("--duration", type=int, default=60,     help="Seconds to run")
    parser.add_argument("--output",   type=str, default="queue/", help="Output directory")
    parser.add_argument("--seed",     type=int, default=42,     help="RNG seed")
    args = parser.parse_args()

    run_producer(
        rate=args.rate,
        duration=args.duration,
        output_dir=args.output,
        seed=args.seed,
    )
