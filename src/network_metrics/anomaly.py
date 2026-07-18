from __future__ import annotations

from datetime import date, timedelta
from functools import reduce

from pyspark.sql import DataFrame, functions as F


ANOMALY_METRICS = {
    "volume": "total_data_volume_mb",
    "signal": "average_signal_strength",
    "records": "record_count",
    "towers": "distinct_tower_count",
}


def _robust_z(value: str, median: str, mad: str) -> F.Column:
    difference = F.col(value) - F.col(median)
    return (
        F.when(
            F.col(mad) > 0,
            F.lit(0.6745) * difference / F.col(mad),
        )
        .when(F.abs(difference) <= F.lit(1e-9), F.lit(0.0))
        .when(difference > 0, F.lit(1e12))
        .otherwise(F.lit(-1e12))
    )


def _anomalies_for_date(
    hourly_df: DataFrame,
    event_date: date,
    *,
    lookback_days: int,
    min_history: int,
    mad_threshold: float,
) -> DataFrame:
    start_date = event_date - timedelta(days=lookback_days)
    baseline_source = (
        hourly_df.filter(
            (F.col("event_date") >= F.lit(start_date))
            & (F.col("event_date") < F.lit(event_date))
        )
        .withColumn("hour_of_day_utc", F.hour("event_hour_utc"))
    )
    current = (
        hourly_df.filter(F.col("event_date") == F.lit(event_date))
        .withColumn("hour_of_day_utc", F.hour("event_hour_utc"))
        .select(
            "region",
            "event_hour_utc",
            "event_date",
            "hour_of_day_utc",
            *ANOMALY_METRICS.values(),
        )
    )

    median_expressions = [
        F.expr(f"percentile_approx({column}, 0.5, 10000)").alias(
            f"{name}_median"
        )
        for name, column in ANOMALY_METRICS.items()
    ]
    baseline = baseline_source.groupBy("region", "hour_of_day_utc").agg(
        F.count(F.lit(1)).alias("baseline_count"),
        *median_expressions,
    )

    deviations = baseline_source.join(
        baseline, ["region", "hour_of_day_utc"], "inner"
    )
    mad_expressions = [
        F.expr(
            "percentile_approx("
            f"abs({column} - {name}_median), 0.5, 10000)"
        ).alias(f"{name}_mad")
        for name, column in ANOMALY_METRICS.items()
    ]
    mad = deviations.groupBy("region", "hour_of_day_utc").agg(
        *mad_expressions
    )

    scored = current.join(
        baseline, ["region", "hour_of_day_utc"], "left"
    ).join(mad, ["region", "hour_of_day_utc"], "left")
    for name, column in ANOMALY_METRICS.items():
        scored = scored.withColumn(
            f"{name}_robust_z",
            _robust_z(column, f"{name}_median", f"{name}_mad"),
        )

    reasons = F.array(
        F.when(
            F.abs(F.col("volume_robust_z")) >= mad_threshold,
            F.lit("data_volume_deviation"),
        ),
        F.when(
            F.col("signal_robust_z") <= -mad_threshold,
            F.lit("signal_strength_degradation"),
        ),
        F.when(
            F.col("records_robust_z") <= -mad_threshold,
            F.lit("record_count_drop"),
        ),
        F.when(
            F.col("towers_robust_z") <= -mad_threshold,
            F.lit("active_tower_drop"),
        ),
    )
    return (
        scored.withColumn("_anomaly_reasons_raw", reasons)
        .withColumn(
            "anomaly_reasons",
            F.expr("filter(_anomaly_reasons_raw, x -> x is not null)"),
        )
        .drop("_anomaly_reasons_raw")
        .withColumn(
            "anomaly_status",
            F.when(
                F.coalesce(F.col("baseline_count"), F.lit(0)) < min_history,
                F.lit("INSUFFICIENT_HISTORY"),
            )
            .when(F.size("anomaly_reasons") > 0, F.lit("ANOMALY"))
            .otherwise(F.lit("PASS")),
        )
        .withColumn("_updated_timestamp_utc", F.current_timestamp())
    )


def build_hourly_anomalies(
    hourly_df: DataFrame,
    affected_dates: list[date],
    *,
    lookback_days: int = 28,
    min_history: int = 7,
    mad_threshold: float = 6.0,
) -> DataFrame:
    """Score current region/hour metrics against a robust historical baseline."""
    frames = [
        _anomalies_for_date(
            hourly_df,
            event_date,
            lookback_days=lookback_days,
            min_history=min_history,
            mad_threshold=mad_threshold,
        )
        for event_date in sorted(set(affected_dates))
    ]
    if not frames:
        raise ValueError("affected_dates must not be empty")
    return reduce(lambda left, right: left.unionByName(right), frames)
