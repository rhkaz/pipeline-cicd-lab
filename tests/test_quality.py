from datetime import date, datetime, timezone

from pyspark.sql import functions as F

from network_metrics.main import write_run_log
from network_metrics.quality import reconciliation_mismatch_count
from network_metrics.quality import write_quality_log


def test_reconciliation_passes_with_matching_totals(spark):
    silver = spark.createDataFrame(
        [("North", date(2025, 7, 23), 100.0, -80.0)],
        """
        region string,
        event_date date,
        data_volume_mb double,
        signal_strength double
        """,
    )
    gold = spark.createDataFrame(
        [("North", date(2025, 7, 23), 100.0, -80.0, 1)],
        """
        region string,
        event_date date,
        total_data_volume_mb double,
        average_signal_strength double,
        record_count long
        """,
    )
    assert reconciliation_mismatch_count(silver, gold) == 0


def test_reconciliation_detects_missing_zero_volume_group(spark):
    silver = spark.createDataFrame(
        [("North", date(2025, 7, 23), 0.0, -80.0)],
        """
        region string,
        event_date date,
        data_volume_mb double,
        signal_strength double
        """,
    )
    gold = spark.createDataFrame(
        [],
        """
        region string,
        event_date date,
        total_data_volume_mb double,
        average_signal_strength double,
        record_count long
        """,
    )
    assert reconciliation_mismatch_count(silver, gold) == 1


def test_reconciliation_detects_average_or_count_mismatch(spark):
    silver = spark.createDataFrame(
        [("North", date(2025, 7, 23), 100.0, -80.0)],
        """
        region string,
        event_date date,
        data_volume_mb double,
        signal_strength double
        """,
    )
    gold = spark.createDataFrame(
        [("North", date(2025, 7, 23), 100.0, -70.0, 2)],
        """
        region string,
        event_date date,
        total_data_volume_mb double,
        average_signal_strength double,
        record_count long
        """,
    )
    assert reconciliation_mismatch_count(silver, gold) == 1


def test_quality_log_preserves_utc_timestamp(spark, tmp_path):
    output = str(tmp_path / "quality-log")
    timestamp = datetime(2025, 7, 23, 12, 30, tzinfo=timezone.utc)
    write_quality_log(
        spark,
        output,
        [
            {
                "run_id": "run-1",
                "stage": "silver",
                "check_name": "utc_test",
                "status": "PASS",
                "severity": "INFO",
                "observed_value": "ok",
                "expected_value": "ok",
                "handling": "continue",
                "check_timestamp_utc": timestamp,
            }
        ],
    )

    observed = (
        spark.read.parquet(output)
        .select(
            F.date_format(
                "check_timestamp_utc", "yyyy-MM-dd HH:mm:ss"
            ).alias("timestamp_text")
        )
        .first()
        .timestamp_text
    )
    assert observed == "2025-07-23 12:30:00"


def test_run_log_preserves_utc_start_timestamp(spark, tmp_path):
    output = str(tmp_path / "run-log")
    started = datetime(2025, 7, 23, 12, 30, tzinfo=timezone.utc)
    write_run_log(
        spark,
        output,
        run_id="run-1",
        stage="bronze",
        status="SUCCESS",
        started=started,
    )

    observed = (
        spark.read.parquet(output)
        .select(
            F.date_format(
                "started_timestamp_utc", "yyyy-MM-dd HH:mm:ss"
            ).alias("timestamp_text")
        )
        .first()
        .timestamp_text
    )
    assert observed == "2025-07-23 12:30:00"
