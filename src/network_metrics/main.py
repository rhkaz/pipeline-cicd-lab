from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import SparkSession, types as T

from network_metrics.config import PipelineConfig
from network_metrics.filesystem import validate_source_file
from network_metrics.jobs import (
    run_bronze,
    run_gold_daily,
    run_gold_hourly,
    run_monitoring,
    run_silver,
)
from network_metrics.paths import PipelinePaths
from network_metrics.manifest import (
    successful_manifest_entry,
    write_manifest_event,
)


LOGGER = logging.getLogger("network-metrics-pipeline")

RUN_LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("started_timestamp_utc", T.TimestampType(), False),
        T.StructField("completed_timestamp_utc", T.TimestampType(), False),
        T.StructField("details_json", T.StringType(), True),
        T.StructField("error_type", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
    ]
)


def create_spark(app_name: str = "network-metrics-pipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def write_run_log(
    spark: SparkSession,
    path: str,
    *,
    run_id: str,
    stage: str,
    status: str,
    started: datetime,
    details: dict[str, Any] | None = None,
    error: Exception | None = None,
) -> None:
    completed = datetime.now(timezone.utc)
    row = {
        "run_id": run_id,
        "stage": stage,
        "status": status,
        "started_timestamp_utc": started,
        "completed_timestamp_utc": completed,
        "details_json": json.dumps(details or {}, default=str),
        "error_type": type(error).__name__ if error else None,
        "error_message": str(error) if error else None,
    }
    spark.createDataFrame([row], RUN_LOG_SCHEMA).write.mode("append").parquet(path)


def execute_stage(
    spark: SparkSession,
    config: PipelineConfig,
    run_id: str,
    stage: str,
    function: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    paths = PipelinePaths(config.input_path, config.output_root)
    try:
        result = function()
        write_run_log(
            spark,
            paths.pipeline_logs,
            run_id=run_id,
            stage=stage,
            status="SUCCESS",
            started=started,
            details=result,
        )
        return result
    except Exception as exc:
        LOGGER.exception("Stage failed: %s", stage)
        try:
            write_run_log(
                spark,
                paths.pipeline_logs,
                run_id=run_id,
                stage=stage,
                status="FAILED",
                started=started,
                error=exc,
            )
        except Exception:
            # Failure logging is best effort. A storage or Spark-session problem
            # must not replace the original stage exception seen by orchestration.
            LOGGER.exception(
                "Unable to persist FAILED run log. run_id=%s stage=%s",
                run_id,
                stage,
            )
        LOGGER.error(
            "INCIDENT run_id=%s stage=%s error_type=%s error=%s",
            run_id,
            stage,
            type(exc).__name__,
            exc,
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tool-agnostic Spark/PySpark telecom pipeline"
    )
    parser.add_argument(
        "--config",
        default="config/pipeline.example.json",
        help="JSON configuration file",
    )
    parser.add_argument("--input-path")
    parser.add_argument("--output-root")
    parser.add_argument("--processing-date")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--stage",
        default="all",
        choices=[
            "all",
            "bronze",
            "silver",
            "gold-hourly",
            "gold-daily",
            "monitoring",
        ],
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    config = PipelineConfig.from_json(args.config).with_overrides(
        input_path=args.input_path,
        output_root=args.output_root,
        processing_date=args.processing_date,
    )
    run_id = args.run_id or (
        f"network-metrics-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    pipeline_id = "network_metrics_medallion"
    spark = create_spark()
    source_info: dict[str, Any] | None = None

    stage_functions: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        (
            "bronze",
            lambda: run_bronze(
                spark,
                config=config,
                pipeline_id=pipeline_id,
                run_id=run_id,
                source_info=source_info,
            ),
        ),
        (
            "silver",
            lambda: run_silver(spark, config=config, run_id=run_id),
        ),
        (
            "gold-hourly",
            lambda: run_gold_hourly(spark, config=config, run_id=run_id),
        ),
        (
            "gold-daily",
            lambda: run_gold_daily(spark, config=config, run_id=run_id),
        ),
        (
            "monitoring",
            lambda: run_monitoring(spark, config=config, run_id=run_id),
        ),
    ]

    try:
        if args.stage == "all":
            paths = PipelinePaths(config.input_path, config.output_root)
            source_info = validate_source_file(
                spark, config.input_path, config.processing_date
            )
            completed = successful_manifest_entry(
                spark,
                paths.processing_manifest,
                source_info["source_sha256"],
            )
            if completed:
                details = {
                    "reason": "identical source content already completed",
                    "source_sha256": source_info["source_sha256"],
                    "original_run_id": completed["run_id"],
                }
                now = datetime.now(timezone.utc)
                write_run_log(
                    spark,
                    paths.pipeline_logs,
                    run_id=run_id,
                    stage="pipeline",
                    status="SKIPPED",
                    started=now,
                    details=details,
                )
                LOGGER.info(
                    "Idempotent skip. source_sha256=%s original_run_id=%s",
                    source_info["source_sha256"],
                    completed["run_id"],
                )
                return
            write_manifest_event(
                spark,
                paths.processing_manifest,
                source_path=source_info["source_path"],
                source_sha256=source_info["source_sha256"],
                processing_date=config.processing_date,
                run_id=run_id,
                status="RUNNING",
                details={"filename": source_info["filename"]},
            )

        selected = stage_functions if args.stage == "all" else [
            item for item in stage_functions if item[0] == args.stage
        ]
        stage_results: dict[str, Any] = {}
        for stage_name, function in selected:
            stage_results[stage_name] = execute_stage(
                spark,
                config,
                run_id,
                stage_name,
                function,
            )
        if args.stage == "all" and source_info:
            write_manifest_event(
                spark,
                PipelinePaths(
                    config.input_path, config.output_root
                ).processing_manifest,
                source_path=source_info["source_path"],
                source_sha256=source_info["source_sha256"],
                processing_date=config.processing_date,
                run_id=run_id,
                status="SUCCESS",
                details=stage_results,
            )
        LOGGER.info("Pipeline completed. run_id=%s", run_id)
    except Exception as exc:
        if args.stage == "all" and source_info:
            try:
                write_manifest_event(
                    spark,
                    PipelinePaths(
                        config.input_path, config.output_root
                    ).processing_manifest,
                    source_path=source_info["source_path"],
                    source_sha256=source_info["source_sha256"],
                    processing_date=config.processing_date,
                    run_id=run_id,
                    status="FAILED",
                    details={
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    },
                )
            except Exception:
                LOGGER.exception("Unable to persist FAILED manifest event")
        LOGGER.exception("Pipeline failed. run_id=%s", run_id)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
