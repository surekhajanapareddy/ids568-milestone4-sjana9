"""
pipeline.py
-----------
Distributed feature engineering pipeline using PySpark.
Runs both a local (single-machine) baseline and a distributed multi-worker
configuration, capturing quantitative performance metrics for comparison.

Usage:
    python pipeline.py --input data/ --output output/
    python pipeline.py --input data/ --output output/ --workers 4
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Spark Session Factories ────────────────────────────────────────────────────

def build_local_spark(app_name: str = "Milestone4-Local") -> SparkSession:
    """Create a single-machine (local) Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")                        # 1 core → single-machine baseline
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")   # disable AQE for fair comparison
        .getOrCreate()
    )


def build_distributed_spark(workers: int, app_name: str = "Milestone4-Distributed") -> SparkSession:
    """Create a multi-worker (local[N]) Spark session simulating distributed execution."""
    shuffle_partitions = workers * 8   # rule of thumb: 2–4× core count
    return (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{workers}]")               # N cores → distributed baseline
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ── Feature Engineering Transformations ───────────────────────────────────────

def add_time_features(df: DataFrame) -> DataFrame:
    """Extract hour-of-day and day-of-week from timestamp (cyclical encoding)."""
    df = df.withColumn("hour",        F.hour("timestamp"))
    df = df.withColumn("day_of_week", F.dayofweek("timestamp"))
    # Cyclical encoding keeps the circular nature of time
    df = df.withColumn("hour_sin",    F.sin(2 * 3.14159 * F.col("hour") / 24))
    df = df.withColumn("hour_cos",    F.cos(2 * 3.14159 * F.col("hour") / 24))
    df = df.withColumn("dow_sin",     F.sin(2 * 3.14159 * F.col("day_of_week") / 7))
    df = df.withColumn("dow_cos",     F.cos(2 * 3.14159 * F.col("day_of_week") / 7))
    return df


def add_log_transforms(df: DataFrame) -> DataFrame:
    """Log-transform skewed numeric columns to normalise distributions."""
    df = df.withColumn("log_amount",           F.log1p(F.col("amount")))
    df = df.withColumn("log_session_duration", F.log1p(F.col("session_duration")))
    df = df.withColumn("log_page_views",       F.log1p(F.col("page_views")))
    return df


def add_categorical_flags(df: DataFrame) -> DataFrame:
    """Binary flags for each category and device type (sparse one-hot encoding)."""
    for cat in ["electronics", "clothing", "food", "sports", "books", "home"]:
        df = df.withColumn(
            f"cat_{cat}",
            (F.col("category") == cat).cast("int")
        )
    for dev in ["mobile", "desktop", "tablet"]:
        df = df.withColumn(
            f"dev_{dev}",
            (F.col("device_type") == dev).cast("int")
        )
    return df


def add_user_aggregations(df: DataFrame) -> DataFrame:
    """
    User-level rolling statistics (shuffle-heavy — the main distributed benefit).
    Computes per-user: total spend, transaction count, avg basket size,
    return rate, and spend standard deviation.
    """
    user_agg = (
        df.groupBy("user_id")
        .agg(
            F.count("transaction_id")         .alias("user_tx_count"),
            F.sum("amount")                   .alias("user_total_spend"),
            F.avg("amount")                   .alias("user_avg_amount"),
            F.stddev("amount")                .alias("user_std_amount"),
            F.avg("items_in_cart")            .alias("user_avg_items"),
            F.avg("is_returned")              .alias("user_return_rate"),
            F.avg("session_duration")         .alias("user_avg_session"),
        )
    )
    df = df.join(user_agg, on="user_id", how="left")
    return df


def add_normalised_features(df: DataFrame) -> DataFrame:
    """
    Z-score normalisation of key numeric columns.
    Uses a full-pass aggregation → another distributed shuffle.
    """
    cols_to_norm = ["amount", "session_duration", "page_views", "items_in_cart"]
    stats = df.select([
        F.mean(c).alias(f"{c}_mean") for c in cols_to_norm
    ] + [
        F.stddev(c).alias(f"{c}_std") for c in cols_to_norm
    ]).first()

    for c in cols_to_norm:
        mean_val = stats[f"{c}_mean"]
        std_val  = stats[f"{c}_std"] or 1.0   # guard against zero std
        df = df.withColumn(
            f"{c}_zscore",
            ((F.col(c) - mean_val) / std_val).cast(DoubleType())
        )
    return df


def add_interaction_features(df: DataFrame) -> DataFrame:
    """Multiply correlated features to create interaction terms."""
    df = df.withColumn("spend_per_item",  F.col("amount")   / F.col("items_in_cart"))
    df = df.withColumn("spend_per_view",  F.col("amount")   / (F.col("page_views") + 1))
    df = df.withColumn("net_amount",      F.col("amount")   * (1 - F.col("discount_pct")))
    return df


def run_feature_engineering(spark: SparkSession, input_dir: str, output_dir: str,
                             num_partitions: int) -> dict:
    """
    Full feature engineering pipeline.

    Returns a dict of runtime metrics collected during execution.
    """
    metrics: dict = {}

    # ── Load ────────────────────────────────────────────────────────────────
    t0 = time.time()
    df = spark.read.parquet(input_dir)
    df = df.repartition(num_partitions)   # explicit partitioning strategy
    row_count = df.count()                # triggers first Spark job
    metrics["load_seconds"]  = round(time.time() - t0, 2)
    metrics["row_count"]     = row_count
    metrics["num_partitions"] = num_partitions
    log.info(f"Loaded {row_count:,} rows in {metrics['load_seconds']}s")

    # ── Transform ───────────────────────────────────────────────────────────
    t1 = time.time()
    df = add_time_features(df)
    df = add_log_transforms(df)
    df = add_categorical_flags(df)
    df = add_interaction_features(df)
    # Shuffle-heavy operations:
    df = add_user_aggregations(df)         # groupBy → shuffle
    df = add_normalised_features(df)       # full scan → stats → join
    metrics["transform_seconds"] = round(time.time() - t1, 2)
    log.info(f"Transformations done in {metrics['transform_seconds']}s")

    # ── Write ────────────────────────────────────────────────────────────────
    t2 = time.time()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    (
        df.write
        .mode("overwrite")
        .parquet(output_dir)
    )
    metrics["write_seconds"] = round(time.time() - t2, 2)
    metrics["total_seconds"] = round(time.time() - t0, 2)
    log.info(f"Written to '{output_dir}' in {metrics['write_seconds']}s")

    # ── Spark internals ──────────────────────────────────────────────────────
    sc = spark.sparkContext
    metrics["spark_master"]        = sc.master
    metrics["default_parallelism"] = sc.defaultParallelism
    metrics["shuffle_partitions"]  = int(
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    return metrics


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Distributed feature engineering pipeline")
    parser.add_argument("--input",   required=True,  help="Input Parquet directory")
    parser.add_argument("--output",  required=True,  help="Output Parquet directory")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of workers for distributed mode (default: 4)")
    args = parser.parse_args()

    all_metrics = {}

    # ── LOCAL (baseline) ────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 1: LOCAL EXECUTION (1 worker)")
    log.info("=" * 60)

    spark_local = build_local_spark()
    local_output = os.path.join(args.output, "local")
    local_metrics = run_feature_engineering(
        spark=spark_local,
        input_dir=args.input,
        output_dir=local_output,
        num_partitions=4,
    )
    all_metrics["local"] = local_metrics
    spark_local.stop()

    # ── DISTRIBUTED ─────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info(f"PHASE 2: DISTRIBUTED EXECUTION ({args.workers} workers)")
    log.info("=" * 60)

    spark_dist = build_distributed_spark(args.workers)
    dist_output = os.path.join(args.output, "distributed")
    dist_metrics = run_feature_engineering(
        spark=spark_dist,
        input_dir=args.input,
        output_dir=dist_output,
        num_partitions=args.workers * 8,
    )
    all_metrics["distributed"] = dist_metrics
    spark_dist.stop()

    # ── Summary ──────────────────────────────────────────────────────────────
    speedup = local_metrics["total_seconds"] / dist_metrics["total_seconds"]
    log.info("\n" + "=" * 60)
    log.info("PERFORMANCE SUMMARY")
    log.info("=" * 60)
    log.info(f"  Local total time      : {local_metrics['total_seconds']}s")
    log.info(f"  Distributed total time: {dist_metrics['total_seconds']}s")
    log.info(f"  Speedup               : {speedup:.2f}x")

    # Save metrics to JSON for REPORT.md
    metrics_path = os.path.join(args.output, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(all_metrics, f, indent=2)
    log.info(f"\nMetrics saved → {metrics_path}")


if __name__ == "__main__":
    main()
