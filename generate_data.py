"""
generate_data.py
----------------
Synthetic dataset generation for Milestone 4.
Generates realistic e-commerce transaction data with configurable size and seed.

Usage:
    python generate_data.py --rows 10000000 --output data/ --seed 42
"""

import argparse
import os
import time
import numpy as np
import pandas as pd
from pathlib import Path


# ── Constants ──────────────────────────────────────────────────────────────────
CATEGORIES   = ["electronics", "clothing", "food", "sports", "books", "home"]
REGIONS      = ["north", "south", "east", "west", "central"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
CHUNK_SIZE   = 500_000   # rows per parquet partition


def generate_chunk(start_idx: int, chunk_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate one chunk of synthetic transaction data."""
    timestamps = pd.date_range(
        start="2023-01-01",
        periods=chunk_rows,
        freq="1s"
    ) + pd.to_timedelta(rng.integers(0, 31_536_000, size=chunk_rows), unit="s")

    return pd.DataFrame({
        "transaction_id":   np.arange(start_idx, start_idx + chunk_rows),
        "user_id":          rng.integers(1, 100_001,  size=chunk_rows),
        "timestamp":        timestamps,
        "amount":           np.round(rng.lognormal(mean=3.5, sigma=1.2, size=chunk_rows), 2),
        "category":         rng.choice(CATEGORIES,   size=chunk_rows),
        "region":           rng.choice(REGIONS,       size=chunk_rows),
        "device_type":      rng.choice(DEVICE_TYPES,  size=chunk_rows),
        "session_duration": rng.integers(30, 3601,    size=chunk_rows),   # seconds
        "items_in_cart":    rng.integers(1, 21,       size=chunk_rows),
        "is_returned":      rng.choice([0, 1], size=chunk_rows, p=[0.88, 0.12]),
        "discount_pct":     np.round(rng.uniform(0, 0.5, size=chunk_rows), 3),
        "page_views":       rng.integers(1, 51,       size=chunk_rows),
    })


def generate_data(rows: int, output_dir: str, seed: int) -> None:
    """
    Generate `rows` rows of synthetic data and save as partitioned Parquet files.

    Parameters
    ----------
    rows       : Total number of rows to generate.
    output_dir : Directory where Parquet files will be saved.
    seed       : Random seed for reproducibility.
    """
    print(f"[generate_data] Generating {rows:,} rows  |  seed={seed}  |  output='{output_dir}'")
    start = time.time()

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)

    chunks_written = 0
    rows_written   = 0

    while rows_written < rows:
        chunk_rows = min(CHUNK_SIZE, rows - rows_written)
        df = generate_chunk(rows_written, chunk_rows, rng)

        out_path = os.path.join(output_dir, f"part_{chunks_written:05d}.parquet")
        df.to_parquet(out_path, index=False)

        rows_written   += chunk_rows
        chunks_written += 1
        print(f"  Written {rows_written:>12,} / {rows:,} rows  ({chunks_written} files)", end="\r")

    elapsed = time.time() - start
    print(f"\n[generate_data] Done in {elapsed:.1f}s  —  {chunks_written} Parquet files saved to '{output_dir}'")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--rows",   type=int, default=10_000_000, help="Number of rows (default: 10M)")
    parser.add_argument("--output", type=str, default="data/",    help="Output directory")
    parser.add_argument("--seed",   type=int, default=42,         help="Random seed (default: 42)")
    args = parser.parse_args()

    generate_data(rows=args.rows, output_dir=args.output, seed=args.seed)
