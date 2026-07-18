from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def add_bronze_metadata(
    raw_df: DataFrame,
    *,
    pipeline_id: str,
    run_id: str,
    stage_id: str,
    processed_timestamp_utc: str,
) -> DataFrame:
    source_file = F.input_file_name()
    file_date_text = F.regexp_extract(
        source_file,
        r"network_metrics_(\d{8})\.csv$",
        1,
    )
    return (
        raw_df.withColumn("_source_file", source_file)
        .withColumn("_file_date", F.to_date(file_date_text, "yyyyMMdd"))
        .withColumn(
            "_processed_timestamp_utc",
            F.to_timestamp(F.lit(processed_timestamp_utc)),
        )
        .withColumn("_ingestion_date", F.to_date("_processed_timestamp_utc"))
        .withColumn("_pipeline_id", F.lit(pipeline_id))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_stage_id", F.lit(stage_id))
        .withColumn("_file_name_valid", file_date_text != F.lit(""))
    )


def build_silver_candidate(
    bronze_df: DataFrame,
    *,
    signal_strength_min: float = -140.0,
    signal_strength_max: float = -20.0,
    check_duplicates: bool = True,
) -> DataFrame:
    parsed = (
        bronze_df.withColumn("region", F.trim(F.col("region")))
        .withColumn(
            "event_timestamp_utc",
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))),
        )
        .withColumn(
            "event_hour_utc",
            F.date_trunc("hour", F.col("event_timestamp_utc")),
        )
        .withColumn("event_date", F.to_date(F.col("event_timestamp_utc")))
    )

    if check_duplicates:
        # A business key may legitimately reappear in a later pipeline run when a
        # source file is replayed or corrected. Only duplicate keys inside the
        # same ingestion run are data-quality failures; cross-run versions are
        # resolved deterministically by latest_record_per_business_key().
        duplicate_window = Window.partitionBy(
            "_run_id",
            "region",
            "tower_id",
            "timestamp",
        )
        parsed = parsed.withColumn(
            "_duplicate_count", F.count(F.lit(1)).over(duplicate_window)
        )
    else:
        parsed = parsed.withColumn("_duplicate_count", F.lit(1))

    errors = F.array(
        F.when(~F.col("_file_name_valid"), F.lit("invalid_file_name")),
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_csv_record")),
        F.when(F.col("tower_id").isNull(), F.lit("tower_id_null")),
        F.when(F.col("tower_id") <= 0, F.lit("tower_id_non_positive")),
        F.when(
            F.col("region").isNull() | (F.length(F.col("region")) == 0),
            F.lit("region_null_or_blank"),
        ),
        F.when(F.col("timestamp").isNull(), F.lit("timestamp_null")),
        F.when(
            F.col("event_timestamp_utc").isNull(),
            F.lit("timestamp_unparseable"),
        ),
        F.when(F.col("signal_strength").isNull(), F.lit("signal_strength_null")),
        F.when(
            ~F.col("signal_strength").between(
                signal_strength_min,
                signal_strength_max,
            ),
            F.lit("signal_strength_out_of_range"),
        ),
        F.when(F.col("data_volume_mb").isNull(), F.lit("data_volume_mb_null")),
        F.when(F.col("data_volume_mb") < 0, F.lit("data_volume_mb_negative")),
        F.when(
            F.col("_duplicate_count") > 1,
            F.lit("duplicate_business_key"),
        ),
    )

    warnings = F.array(
        F.when(
            F.col("_file_date").isNotNull()
            & F.col("event_date").isNotNull()
            & (F.col("_file_date") != F.col("event_date")),
            F.lit("event_date_differs_from_filename_date"),
        )
    )

    return (
        parsed.withColumn("_quality_errors_raw", errors)
        .withColumn("_quality_warnings_raw", warnings)
        .withColumn(
            "_quality_errors",
            F.expr("filter(_quality_errors_raw, x -> x is not null)"),
        )
        .withColumn(
            "_quality_warnings",
            F.expr("filter(_quality_warnings_raw, x -> x is not null)"),
        )
        .drop(
            "_quality_errors_raw",
            "_quality_warnings_raw",
            "_duplicate_count",
        )
        .withColumn(
            "_quality_status",
            F.when(F.size("_quality_errors") > 0, F.lit("INVALID"))
            .when(
                F.size("_quality_warnings") > 0,
                F.lit("VALID_WITH_WARNING"),
            )
            .otherwise(F.lit("VALID")),
        )
    )


def latest_record_per_business_key(df: DataFrame) -> DataFrame:
    """
    Select the latest record when the input contains multiple file runs.

    The full-refresh path normally supplies one current run. The same
    deterministic ordering policy can support a future consolidated dataset
    containing corrected versions from retained Bronze history.
    """
    ordering = Window.partitionBy("region", "tower_id", "timestamp").orderBy(
        F.col("_processed_timestamp_utc").desc(),
        F.col("_run_id").desc(),
    )
    return (
        df.withColumn("_record_rank", F.row_number().over(ordering))
        .filter(F.col("_record_rank") == 1)
        .drop("_record_rank")
    )


def build_hourly_gold(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("region", "event_hour_utc")
        .agg(
            F.sum("data_volume_mb").alias("total_data_volume_mb"),
            F.avg("signal_strength").alias("average_signal_strength"),
            F.count(F.lit(1)).alias("record_count"),
            F.countDistinct("tower_id").alias("distinct_tower_count"),
        )
        .withColumn("event_date", F.to_date("event_hour_utc"))
        .withColumn("_updated_timestamp_utc", F.current_timestamp())
    )


def build_daily_gold(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("region", "event_date")
        .agg(
            F.sum("data_volume_mb").alias("total_data_volume_mb"),
            F.avg("signal_strength").alias("average_signal_strength"),
            F.count(F.lit(1)).alias("record_count"),
            F.countDistinct("tower_id").alias("distinct_tower_count"),
        )
        .withColumn("_updated_timestamp_utc", F.current_timestamp())
    )
