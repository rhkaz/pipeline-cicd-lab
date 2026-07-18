from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


QUALITY_LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("observed_value", T.StringType(), True),
        T.StructField("expected_value", T.StringType(), True),
        T.StructField("handling", T.StringType(), True),
        T.StructField("check_timestamp_utc", T.TimestampType(), False),
    ]
)


def quality_summary(candidate_df: DataFrame) -> dict[str, int | float]:
    row = (
        candidate_df.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(
                F.when(F.size("_quality_errors") > 0, 1).otherwise(0)
            ).alias("invalid_rows"),
            F.sum(
                F.when(F.size("_quality_warnings") > 0, 1).otherwise(0)
            ).alias("warning_rows"),
            F.sum(
                F.when(
                    F.array_contains(
                        "_quality_warnings",
                        "event_date_differs_from_filename_date",
                    ),
                    1,
                ).otherwise(0)
            ).alias("date_mismatch_rows"),
        )
        .first()
    )
    input_rows = int(row["input_rows"] or 0)
    invalid_rows = int(row["invalid_rows"] or 0)
    return {
        "input_rows": input_rows,
        "invalid_rows": invalid_rows,
        "warning_rows": int(row["warning_rows"] or 0),
        "date_mismatch_rows": int(row["date_mismatch_rows"] or 0),
        "invalid_rate": invalid_rows / input_rows if input_rows else 1.0,
    }


def reconciliation_mismatch_count(
    silver_df: DataFrame,
    gold_df: DataFrame,
    group_columns: Sequence[str] = ("region", "event_date"),
    tolerance: float = 1e-6,
) -> int:
    """Count missing or numerically inconsistent Silver-to-Gold groups."""
    reconcile_tower_count = (
        "tower_id" in silver_df.columns
        and "distinct_tower_count" in gold_df.columns
    )
    silver_measures = [
        F.sum("data_volume_mb").alias("silver_total_volume"),
        F.avg("signal_strength").alias("silver_average_signal"),
        F.count(F.lit(1)).alias("silver_record_count"),
    ]
    if reconcile_tower_count:
        silver_measures.append(
            F.countDistinct("tower_id").alias("silver_tower_count")
        )
    silver_totals = (
        silver_df.groupBy(*group_columns)
        .agg(*silver_measures)
        .withColumn("silver_group_present", F.lit(True))
    )
    gold_columns = [
        *group_columns,
        F.col("total_data_volume_mb").alias("gold_total_volume"),
        F.col("average_signal_strength").alias("gold_average_signal"),
        F.col("record_count").alias("gold_record_count"),
    ]
    if reconcile_tower_count:
        gold_columns.append(
            F.col("distinct_tower_count").alias("gold_tower_count")
        )
    gold_totals = gold_df.select(*gold_columns).withColumn(
        "gold_group_present",
        F.lit(True),
    )

    volume_mismatch = (
        F.abs(F.col("silver_total_volume") - F.col("gold_total_volume"))
        > F.lit(tolerance)
    )
    signal_mismatch = (
        F.abs(F.col("silver_average_signal") - F.col("gold_average_signal"))
        > F.lit(tolerance)
    )
    count_mismatch = ~F.col("silver_record_count").eqNullSafe(
        F.col("gold_record_count")
    )
    tower_count_mismatch = (
        ~F.col("silver_tower_count").eqNullSafe(F.col("gold_tower_count"))
        if reconcile_tower_count
        else F.lit(False)
    )

    return (
        silver_totals.join(gold_totals, list(group_columns), "full")
        .filter(
            F.col("silver_group_present").isNull()
            | F.col("gold_group_present").isNull()
            | volume_mismatch
            | signal_mismatch
            | count_mismatch
            | tower_count_mismatch
        )
        .count()
    )


def write_quality_log(
    spark: SparkSession,
    output_path: str,
    rows: list[dict[str, Any]],
) -> None:
    # Keep the UTC offset attached while crossing the Python/JVM boundary.
    # Stripping tzinfo makes PySpark interpret the value in the host timezone,
    # which can shift monitoring timestamps on non-UTC machines.
    now = datetime.now(timezone.utc)
    normalized = [
        {
            **row,
            "check_timestamp_utc": row.get("check_timestamp_utc", now),
        }
        for row in rows
    ]
    spark.createDataFrame(normalized, schema=QUALITY_LOG_SCHEMA).write.mode(
        "append"
    ).parquet(output_path)
