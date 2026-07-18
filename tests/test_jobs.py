from datetime import date, datetime

import pytest

from network_metrics.config import PipelineConfig
from network_metrics.filesystem import path_exists
from network_metrics.jobs import affected_dates_for_run, run_silver
from network_metrics.main import execute_stage
from network_metrics.paths import PipelinePaths


def bronze_df(
    spark,
    run_id: str = "run-1",
    *,
    timestamp: int = 1753228800,
    processed: datetime = datetime(2025, 7, 23, 1, 0),
    volume: float = 100.0,
):
    return spark.createDataFrame(
        [
            (
                1,
                "North",
                timestamp,
                -80.0,
                volume,
                None,
                "file:///network_metrics_20250723.csv",
                date(2025, 7, 23),
                processed,
                date(2025, 7, 23),
                "pipeline",
                run_id,
                "bronze",
                True,
            )
        ],
        """
        tower_id int,
        region string,
        timestamp long,
        signal_strength double,
        data_volume_mb double,
        _corrupt_record string,
        _source_file string,
        _file_date date,
        _processed_timestamp_utc timestamp,
        _ingestion_date date,
        _pipeline_id string,
        _run_id string,
        _stage_id string,
        _file_name_valid boolean
        """,
    )


def test_affected_dates_requires_matching_run_id(spark, tmp_path):
    config = PipelineConfig(
        input_path="unused",
        output_root=str(tmp_path),
        processing_date="2025-07-23",
    )
    paths = PipelinePaths(config.input_path, config.output_root)
    bronze_df(spark).write.mode("overwrite").parquet(paths.bronze)

    with pytest.raises(RuntimeError, match="same run ID as the Bronze stage"):
        affected_dates_for_run(spark, config, "missing-run")


def test_silver_requires_matching_run_id(spark, tmp_path):
    config = PipelineConfig(
        input_path="unused",
        output_root=str(tmp_path),
        processing_date="2025-07-23",
    )
    paths = PipelinePaths(config.input_path, config.output_root)
    bronze_df(spark).write.mode("overwrite").parquet(paths.bronze)

    with pytest.raises(RuntimeError, match="same run ID as the Bronze stage"):
        run_silver(spark, config=config, run_id="missing-run")


def test_clean_silver_run_removes_stale_quarantine(spark, tmp_path):
    config = PipelineConfig(
        input_path="unused",
        output_root=str(tmp_path),
        processing_date="2025-07-23",
    )
    paths = PipelinePaths(config.input_path, config.output_root)
    bronze_df(spark).write.mode("overwrite").parquet(paths.bronze)
    stale_partition = (
        f"{paths.quarantine}/_ingestion_date=2025-07-23/_run_id=run-1"
    )
    spark.createDataFrame([("stale",)], "marker string").write.mode(
        "overwrite"
    ).parquet(stale_partition)
    assert path_exists(spark, stale_partition)

    result = run_silver(spark, config=config, run_id="run-1")

    assert result["invalid_rows"] == 0
    assert not path_exists(spark, stale_partition)


def test_silver_incrementally_replaces_latest_business_key(spark, tmp_path):
    config = PipelineConfig(
        input_path="unused",
        output_root=str(tmp_path),
        processing_date="2025-07-24",
    )
    paths = PipelinePaths(config.input_path, config.output_root)
    first = bronze_df(
        spark,
        "run-1",
        timestamp=1753315200,
        processed=datetime(2025, 7, 23, 1, 0),
        volume=100.0,
    )
    correction = bronze_df(
        spark,
        "run-2",
        timestamp=1753315200,
        processed=datetime(2025, 7, 24, 2, 0),
        volume=125.0,
    )
    first.unionByName(correction).write.mode("overwrite").parquet(paths.bronze)

    result = run_silver(spark, config=config, run_id="run-2")
    current = spark.read.parquet(paths.silver).collect()

    assert result["published_rows"] == 1
    assert len(current) == 1
    assert current[0].data_volume_mb == 125.0


def test_execute_stage_preserves_original_error_when_failure_log_breaks(
    spark,
    tmp_path,
    monkeypatch,
):
    config = PipelineConfig(
        input_path="unused",
        output_root=str(tmp_path),
        processing_date="2025-07-23",
    )

    def fail_to_write_log(*args, **kwargs):
        raise OSError("monitoring storage unavailable")

    def fail_stage():
        raise ValueError("original stage failure")

    monkeypatch.setattr("network_metrics.main.write_run_log", fail_to_write_log)

    with pytest.raises(ValueError, match="original stage failure"):
        execute_stage(
            spark,
            config,
            run_id="run-1",
            stage="silver",
            function=fail_stage,
        )
