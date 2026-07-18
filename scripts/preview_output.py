"""Preview one file-based pipeline dataset with PySpark."""

from __future__ import annotations

import argparse
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F

from network_metrics.filesystem import path_exists
from network_metrics.paths import PipelinePaths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview pipeline Parquet output")
    parser.add_argument("--output-root", default="output")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=[
            "bronze",
            "silver",
            "quarantine",
            "gold-hourly",
            "gold-daily",
            "run-logs",
            "quality-logs",
            "manifest",
            "anomalies",
        ],
    )
    parser.add_argument("--rows", type=int, default=20)
    return parser.parse_args()


def selected_path(paths: PipelinePaths, dataset: str) -> str:
    return {
        "bronze": paths.bronze,
        "silver": paths.silver,
        "quarantine": paths.quarantine,
        "gold-hourly": paths.gold_hourly,
        "gold-daily": paths.gold_daily,
        "run-logs": paths.pipeline_logs,
        "quality-logs": paths.quality_logs,
        "manifest": paths.processing_manifest,
        "anomalies": paths.hourly_anomalies,
    }[dataset]


def ordered(df: DataFrame, dataset: str) -> DataFrame:
    order_columns = {
        "bronze": ["timestamp", "region", "tower_id"],
        "silver": ["event_timestamp_utc", "region", "tower_id"],
        "quarantine": ["timestamp", "region", "tower_id"],
        "gold-hourly": ["event_hour_utc", "region"],
        "gold-daily": ["event_date", "region"],
        "run-logs": ["started_timestamp_utc", "stage"],
        "quality-logs": ["check_timestamp_utc", "stage", "check_name"],
        "manifest": ["event_timestamp_utc", "processing_date", "run_id"],
        "anomalies": ["event_hour_utc", "region"],
    }[dataset]
    existing = [column for column in order_columns if column in df.columns]
    return df.orderBy(*existing) if existing else df


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(f"preview-{args.dataset}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    paths = PipelinePaths(input_path="", output_root=args.output_root)
    dataset_path = selected_path(paths, args.dataset)

    try:
        if not path_exists(spark, dataset_path):
            print(f"No output exists yet for {args.dataset}: {dataset_path}")
            return

        df = spark.read.parquet(dataset_path)
        print("\n" + "=" * 80)
        print(f"DATASET : {args.dataset}")
        print(f"PATH    : {dataset_path}")
        print(f"ROWS    : {df.count()}")
        print("=" * 80)
        df.printSchema()
        ordered(df, args.dataset).show(args.rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
