from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import SparkSession, functions as F, types as T

from network_metrics.filesystem import path_exists


MANIFEST_SCHEMA = T.StructType(
    [
        T.StructField("source_path", T.StringType(), False),
        T.StructField("source_sha256", T.StringType(), False),
        T.StructField("processing_date", T.DateType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("event_timestamp_utc", T.TimestampType(), False),
        T.StructField("details_json", T.StringType(), True),
    ]
)


def successful_manifest_entry(
    spark: SparkSession,
    path: str,
    source_sha256: str,
) -> dict[str, Any] | None:
    """Return the latest successful entry for an identical source object."""
    if not path_exists(spark, path):
        return None
    rows = (
        spark.read.parquet(path)
        .filter(
            (F.col("source_sha256") == source_sha256)
            & (F.col("status") == "SUCCESS")
        )
        .orderBy(F.col("event_timestamp_utc").desc())
        .limit(1)
        .collect()
    )
    return rows[0].asDict(recursive=True) if rows else None


def write_manifest_event(
    spark: SparkSession,
    path: str,
    *,
    source_path: str,
    source_sha256: str,
    processing_date: str,
    run_id: str,
    status: str,
    details: dict[str, Any] | None = None,
) -> None:
    row = {
        "source_path": source_path,
        "source_sha256": source_sha256,
        "processing_date": date.fromisoformat(processing_date),
        "run_id": run_id,
        "status": status,
        "event_timestamp_utc": datetime.now(timezone.utc),
        "details_json": json.dumps(details or {}, default=str, sort_keys=True),
    }
    spark.createDataFrame([row], MANIFEST_SCHEMA).write.mode("append").parquet(
        path
    )
