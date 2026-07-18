from datetime import date, datetime

from pyspark.sql import functions as F

from network_metrics.transforms import (
    build_daily_gold,
    build_hourly_gold,
    build_silver_candidate,
    latest_record_per_business_key,
)


def bronze_df(spark, rows):
    return spark.createDataFrame(
        rows,
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


def test_timestamp_conversion_and_hour_bucket(spark):
    source = bronze_df(
        spark,
        [
            (
                1,
                " North ",
                1753228800,
                -80.0,
                100.0,
                None,
                "file:///network_metrics_20250723.csv",
                date(2025, 7, 23),
                datetime(2025, 7, 23, 1, 0),
                date(2025, 7, 23),
                "pipeline",
                "run-1",
                "bronze",
                True,
            )
        ],
    )
    row = (
        build_silver_candidate(source)
        .select(
            "region",
            F.date_format(
                "event_timestamp_utc", "yyyy-MM-dd HH:mm:ss"
            ).alias("event_timestamp_utc_text"),
            F.date_format(
                "event_hour_utc", "yyyy-MM-dd HH:mm:ss"
            ).alias("event_hour_utc_text"),
            F.date_format("event_date", "yyyy-MM-dd").alias(
                "event_date_text"
            ),
            "_quality_status",
        )
        .first()
    )
    assert row.region == "North"
    assert row.event_timestamp_utc_text == "2025-07-23 00:00:00"
    assert row.event_hour_utc_text == "2025-07-23 00:00:00"
    assert row.event_date_text == "2025-07-23"
    assert row._quality_status == "VALID"


def test_cross_midnight_record_is_warning(spark):
    source = bronze_df(
        spark,
        [
            (
                1,
                "North",
                1753315200,
                -80.0,
                100.0,
                None,
                "file:///network_metrics_20250723.csv",
                date(2025, 7, 23),
                datetime(2025, 7, 23, 1, 0),
                date(2025, 7, 23),
                "pipeline",
                "run-1",
                "bronze",
                True,
            )
        ],
    )
    row = build_silver_candidate(source).first()
    assert row.event_date == date(2025, 7, 24)
    assert row._quality_errors == []
    assert "event_date_differs_from_filename_date" in row._quality_warnings
    assert row._quality_status == "VALID_WITH_WARNING"


def test_invalid_values_are_quarantinable(spark):
    source = bronze_df(
        spark,
        [
            (
                -1,
                "",
                1753228800,
                -500.0,
                -1.0,
                None,
                "file:///network_metrics_20250723.csv",
                date(2025, 7, 23),
                datetime(2025, 7, 23, 1, 0),
                date(2025, 7, 23),
                "pipeline",
                "run-1",
                "bronze",
                True,
            )
        ],
    )
    row = build_silver_candidate(source).first()
    assert row._quality_status == "INVALID"
    assert "tower_id_non_positive" in row._quality_errors
    assert "region_null_or_blank" in row._quality_errors
    assert "signal_strength_out_of_range" in row._quality_errors
    assert "data_volume_mb_negative" in row._quality_errors


def test_duplicates_are_scoped_to_the_ingestion_run(spark):
    base = (
        1,
        "North",
        1753228800,
        -80.0,
        100.0,
        None,
        "file:///network_metrics_20250723.csv",
        date(2025, 7, 23),
        datetime(2025, 7, 23, 1, 0),
        date(2025, 7, 23),
        "pipeline",
    )
    source = bronze_df(
        spark,
        [
            (*base, "run-1", "bronze", True),
            (*base, "run-1", "bronze", True),
            (
                *base[:8],
                datetime(2025, 7, 23, 2, 0),
                *base[9:],
                "run-2",
                "bronze",
                True,
            ),
        ],
    )

    candidates = build_silver_candidate(source)
    run_1 = candidates.filter(F.col("_run_id") == "run-1").collect()
    run_2 = candidates.filter(F.col("_run_id") == "run-2").first()

    assert all("duplicate_business_key" in row._quality_errors for row in run_1)
    assert "duplicate_business_key" not in run_2._quality_errors


def test_latest_valid_replay_wins_for_the_business_key(spark):
    source = spark.createDataFrame(
        [
            ("North", 1, 1753228800, datetime(2025, 7, 23, 1, 0), "run-1", 90.0),
            ("North", 1, 1753228800, datetime(2025, 7, 23, 2, 0), "run-2", 100.0),
        ],
        """
        region string,
        tower_id int,
        timestamp long,
        _processed_timestamp_utc timestamp,
        _run_id string,
        data_volume_mb double
        """,
    )

    latest = latest_record_per_business_key(source).first()
    assert latest._run_id == "run-2"
    assert latest.data_volume_mb == 100.0


def test_required_gold_aggregations(spark):
    source = spark.createDataFrame(
        [
            (
                "North",
                datetime(2025, 7, 23, 0, 0),
                date(2025, 7, 23),
                100.0,
                -80.0,
                1,
            ),
            (
                "North",
                datetime(2025, 7, 23, 0, 0),
                date(2025, 7, 23),
                50.0,
                -70.0,
                2,
            ),
        ],
        """
        region string,
        event_hour_utc timestamp,
        event_date date,
        data_volume_mb double,
        signal_strength double,
        tower_id int
        """,
    )
    hourly = build_hourly_gold(source).first()
    daily = build_daily_gold(source).first()
    assert hourly.total_data_volume_mb == 150.0
    assert hourly.average_signal_strength == -75.0
    assert daily.total_data_volume_mb == 150.0
    assert daily.average_signal_strength == -75.0
