from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession, functions as F

from network_metrics.anomaly import build_hourly_anomalies
from network_metrics.config import PipelineConfig
from network_metrics.filesystem import (
    delete_dataset,
    source_glob,
    validate_source_file,
)
from network_metrics.paths import PipelinePaths
from network_metrics.quality import (
    quality_summary,
    reconciliation_mismatch_count,
    write_quality_log,
)
from network_metrics.schema import NETWORK_METRICS_SCHEMA
from network_metrics.transforms import (
    add_bronze_metadata,
    build_daily_gold,
    build_hourly_gold,
    build_silver_candidate,
    latest_record_per_business_key,
)


def configure_spark(spark: SparkSession, timezone_name: str) -> None:
    spark.conf.set("spark.sql.session.timeZone", timezone_name)
    # Only partitions present in a write are replaced. This lets corrected and
    # late-arriving batches republish affected dates without deleting history.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def run_bronze(
    spark: SparkSession,
    *,
    config: PipelineConfig,
    pipeline_id: str,
    run_id: str,
    stage_id: str = "bronze",
    source_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    configure_spark(spark, config.timezone)
    paths = PipelinePaths(config.input_path, config.output_root)
    file_info = source_info or validate_source_file(
        spark, config.input_path, config.processing_date
    )
    processed_timestamp = datetime.now(timezone.utc).isoformat()
    raw = (
        spark.read.schema(NETWORK_METRICS_SCHEMA)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(source_glob(config.input_path, config.processing_date))
    )
    bronze = add_bronze_metadata(
        raw,
        pipeline_id=pipeline_id,
        run_id=run_id,
        stage_id=stage_id,
        processed_timestamp_utc=processed_timestamp,
    )
    (
        bronze.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("_ingestion_date", "_run_id")
        .parquet(paths.bronze)
    )
    return {"stage": "bronze", "rows": bronze.count(), **file_info}


def affected_dates_for_run(
    spark: SparkSession,
    config: PipelineConfig,
    run_id: str,
) -> list[Any]:
    paths = PipelinePaths(config.input_path, config.output_root)
    current = spark.read.parquet(paths.bronze).filter(F.col("_run_id") == run_id)
    affected_dates = [
        row["event_date"]
        for row in build_silver_candidate(
            current,
            signal_strength_min=config.signal_strength_min,
            signal_strength_max=config.signal_strength_max,
        )
        .filter(F.col("event_date").isNotNull())
        .select("event_date")
        .distinct()
        .collect()
    ]
    if not affected_dates:
        raise RuntimeError(
            "No valid event dates were found for "
            f"run_id={run_id}. All separately executed stages must use "
            "the same run ID as the Bronze stage."
        )
    return affected_dates


def run_silver(
    spark: SparkSession,
    *,
    config: PipelineConfig,
    run_id: str,
) -> dict[str, Any]:
    configure_spark(spark, config.timezone)
    paths = PipelinePaths(config.input_path, config.output_root)
    current_bronze = spark.read.parquet(paths.bronze).filter(
        F.col("_run_id") == run_id
    )
    if not current_bronze.take(1):
        raise RuntimeError(
            "No Bronze records were found for "
            f"run_id={run_id}. The Silver stage must use the same run ID "
            "as the Bronze stage."
        )
    current_candidate = build_silver_candidate(
        current_bronze,
        signal_strength_min=config.signal_strength_min,
        signal_strength_max=config.signal_strength_max,
    ).cache()
    metrics = quality_summary(current_candidate)

    invalid = current_candidate.filter(F.size("_quality_errors") > 0)
    ingestion_dates = [
        str(row["_ingestion_date"])
        for row in current_candidate.select("_ingestion_date").distinct().collect()
    ]
    for ingestion_date in ingestion_dates:
        delete_dataset(
            spark,
            f"{paths.quarantine}/_ingestion_date={ingestion_date}/_run_id={run_id}",
        )
    if metrics["invalid_rows"] > 0:
        (
            invalid.withColumn(
                "_quarantined_timestamp_utc",
                F.current_timestamp(),
            )
            .write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("_ingestion_date", "_run_id")
            .parquet(paths.quarantine)
        )

    invalid_rate = float(metrics["invalid_rate"])
    if invalid_rate >= config.invalid_rate_failure_threshold:
        status = "FAIL"
        severity = "ERROR"
    elif (
        invalid_rate >= config.invalid_rate_warning_threshold
        or metrics["warning_rows"] > 0
    ):
        status = "WARN"
        severity = "WARNING"
    else:
        status = "PASS"
        severity = "INFO"

    write_quality_log(
        spark,
        paths.quality_logs,
        [
            {
                "run_id": run_id,
                "stage": "silver",
                "check_name": "row_quality_summary",
                "status": status,
                "severity": severity,
                "observed_value": str(metrics),
                "expected_value": (
                    "invalid_rate <= "
                    f"{config.invalid_rate_failure_threshold}"
                ),
                "handling": (
                    "quarantine invalid rows; retain warning-only rows"
                ),
            }
        ],
    )

    if status == "FAIL":
        current_candidate.unpersist()
        raise RuntimeError(
            f"Invalid-row rate {invalid_rate:.2%} exceeded "
            f"{config.invalid_rate_failure_threshold:.2%}"
        )

    affected_dates = [
        row["event_date"]
        for row in (
            current_candidate.filter(F.col("event_date").isNotNull())
            .select("event_date")
            .distinct()
            .collect()
        )
    ]

    # Rebuild only affected event-date partitions from retained Bronze history.
    # Ranking before filtering errors ensures that an invalid correction cannot
    # silently expose an older valid version of the same business key.
    all_candidates = build_silver_candidate(
        spark.read.parquet(paths.bronze),
        signal_strength_min=config.signal_strength_min,
        signal_strength_max=config.signal_strength_max,
    ).filter(F.col("event_date").isin(affected_dates))
    silver = latest_record_per_business_key(all_candidates).filter(
        F.size("_quality_errors") == 0
    ).cache()
    published_rows = silver.count()
    (
        silver.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(paths.silver)
    )
    silver.unpersist()
    current_candidate.unpersist()

    return {
        "stage": "silver",
        "affected_dates": [str(value) for value in affected_dates],
        "published_rows": published_rows,
        **metrics,
    }


def run_gold_hourly(
    spark: SparkSession,
    *,
    config: PipelineConfig,
    run_id: str,
) -> dict[str, Any]:
    configure_spark(spark, config.timezone)
    paths = PipelinePaths(config.input_path, config.output_root)
    affected_dates = affected_dates_for_run(spark, config, run_id)
    silver = spark.read.parquet(paths.silver).filter(
        F.col("event_date").isin(affected_dates)
    )
    gold = build_hourly_gold(silver).cache()
    row_count = gold.count()
    (
        gold.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(paths.gold_hourly)
    )
    gold.unpersist()
    return {
        "stage": "gold_hourly",
        "rows": row_count,
        "affected_dates": [str(value) for value in affected_dates],
    }


def run_gold_daily(
    spark: SparkSession,
    *,
    config: PipelineConfig,
    run_id: str,
) -> dict[str, Any]:
    configure_spark(spark, config.timezone)
    paths = PipelinePaths(config.input_path, config.output_root)
    affected_dates = affected_dates_for_run(spark, config, run_id)
    silver = spark.read.parquet(paths.silver).filter(
        F.col("event_date").isin(affected_dates)
    )
    gold = build_daily_gold(silver).cache()
    row_count = gold.count()
    (
        gold.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(paths.gold_daily)
    )
    gold.unpersist()
    return {
        "stage": "gold_daily",
        "rows": row_count,
        "affected_dates": [str(value) for value in affected_dates],
    }


def run_monitoring(
    spark: SparkSession,
    *,
    config: PipelineConfig,
    run_id: str,
) -> dict[str, Any]:
    configure_spark(spark, config.timezone)
    paths = PipelinePaths(config.input_path, config.output_root)
    affected_dates = affected_dates_for_run(spark, config, run_id)
    silver = spark.read.parquet(paths.silver).filter(
        F.col("event_date").isin(affected_dates)
    )
    all_hourly = spark.read.parquet(paths.gold_hourly)
    hourly = all_hourly.filter(
        F.col("event_date").isin(affected_dates)
    )
    daily = spark.read.parquet(paths.gold_daily).filter(
        F.col("event_date").isin(affected_dates)
    )
    hourly_mismatch_count = reconciliation_mismatch_count(
        silver,
        hourly,
        group_columns=("region", "event_hour_utc"),
    )
    daily_mismatch_count = reconciliation_mismatch_count(silver, daily)
    anomaly_results = build_hourly_anomalies(
        all_hourly,
        affected_dates,
        lookback_days=config.anomaly_lookback_days,
        min_history=config.anomaly_min_history,
        mad_threshold=config.anomaly_mad_threshold,
    ).cache()
    anomaly_summary = anomaly_results.agg(
        F.count(F.lit(1)).alias("evaluated_rows"),
        F.sum(
            F.when(F.col("anomaly_status") == "ANOMALY", 1).otherwise(0)
        ).alias("anomaly_rows"),
        F.sum(
            F.when(F.col("anomaly_status") == "PASS", 1).otherwise(0)
        ).alias("baseline_eligible_rows"),
    ).first()
    evaluated_rows = int(anomaly_summary["evaluated_rows"] or 0)
    anomaly_rows = int(anomaly_summary["anomaly_rows"] or 0)
    baseline_eligible_rows = int(
        anomaly_summary["baseline_eligible_rows"] or 0
    ) + anomaly_rows
    (
        anomaly_results.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(paths.hourly_anomalies)
    )
    anomaly_results.unpersist()
    if baseline_eligible_rows == 0:
        anomaly_status = "INSUFFICIENT_HISTORY"
        anomaly_severity = "INFO"
    elif anomaly_rows > 0:
        anomaly_status = "WARN"
        anomaly_severity = "WARNING"
    else:
        anomaly_status = "PASS"
        anomaly_severity = "INFO"
    write_quality_log(
        spark,
        paths.quality_logs,
        [
            {
                "run_id": run_id,
                "stage": "gold",
                "check_name": "silver_gold_hourly_reconciliation",
                "status": "PASS" if hourly_mismatch_count == 0 else "FAIL",
                "severity": "INFO" if hourly_mismatch_count == 0 else "ERROR",
                "observed_value": str(hourly_mismatch_count),
                "expected_value": "0 mismatched region/hour groups",
                "handling": (
                    "fail Spark application if group presence, volume, "
                    "average signal or row count does not reconcile"
                ),
            },
            {
                "run_id": run_id,
                "stage": "gold",
                "check_name": "silver_gold_daily_reconciliation",
                "status": "PASS" if daily_mismatch_count == 0 else "FAIL",
                "severity": "INFO" if daily_mismatch_count == 0 else "ERROR",
                "observed_value": str(daily_mismatch_count),
                "expected_value": "0 mismatched region/date groups",
                "handling": (
                    "fail Spark application if group presence, volume, "
                    "average signal or row count does not reconcile"
                ),
            },
            {
                "run_id": run_id,
                "stage": "gold",
                "check_name": "historical_anomaly_detection",
                "status": anomaly_status,
                "severity": anomaly_severity,
                "observed_value": str(
                    {
                        "evaluated_rows": evaluated_rows,
                        "baseline_eligible_rows": baseline_eligible_rows,
                        "anomaly_rows": anomaly_rows,
                    }
                ),
                "expected_value": (
                    "robust median/MAD score within "
                    f"{config.anomaly_mad_threshold} using at least "
                    f"{config.anomaly_min_history} historical observations"
                ),
                "handling": (
                    "persist row-level anomaly reasons; warn operations; "
                    "do not remove data"
                ),
            },
        ],
    )
    if hourly_mismatch_count or daily_mismatch_count:
        raise RuntimeError(
            "Silver-to-Gold reconciliation failed: "
            f"hourly_mismatches={hourly_mismatch_count}, "
            f"daily_mismatches={daily_mismatch_count}"
        )
    return {
        "stage": "monitoring",
        "status": "PASS",
        "affected_dates": [str(value) for value in affected_dates],
        "hourly_mismatches": hourly_mismatch_count,
        "daily_mismatches": daily_mismatch_count,
        "anomaly_status": anomaly_status,
        "anomaly_rows": anomaly_rows,
        "baseline_eligible_rows": baseline_eligible_rows,
    }
