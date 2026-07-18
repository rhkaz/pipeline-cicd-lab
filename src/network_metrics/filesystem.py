from __future__ import annotations

import re
from typing import Any

from pyspark.sql import SparkSession

from network_metrics.schema import REQUIRED_SOURCE_COLUMNS


def source_glob(input_path: str, processing_date: str) -> str:
    date_token = processing_date.replace("-", "")
    return f"{input_path.rstrip('/')}/network_metrics_{date_token}.csv"


def _filesystem_statuses(spark: SparkSession, path_pattern: str) -> list[Any]:
    """
    Resolve files through Spark's Hadoop filesystem abstraction.

    The same implementation supports any URI understood by the configured Spark
    runtime, for example local paths, file://, HDFS, or object storage connectors.
    """
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path_pattern)
    filesystem = hadoop_path.getFileSystem(conf)
    statuses = filesystem.globStatus(hadoop_path)
    return list(statuses) if statuses else []


def validate_source_file(
    spark: SparkSession,
    input_path: str,
    processing_date: str,
) -> dict[str, Any]:
    path_pattern = source_glob(input_path, processing_date)
    statuses = _filesystem_statuses(spark, path_pattern)

    if not statuses:
        raise FileNotFoundError(f"No source file found: {path_pattern}")
    if len(statuses) != 1:
        raise ValueError(
            f"Expected exactly one daily file, found {len(statuses)}: {path_pattern}"
        )

    status = statuses[0]
    filename = status.getPath().getName()
    if not re.fullmatch(r"network_metrics_\d{8}\.csv", filename):
        raise ValueError(f"Invalid filename: {filename}")
    if int(status.getLen()) <= 0:
        raise ValueError(f"Source file is empty: {path_pattern}")

    observed_columns = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(path_pattern)
        .columns
    )
    if observed_columns != REQUIRED_SOURCE_COLUMNS:
        raise ValueError(
            "Header mismatch. "
            f"Expected {REQUIRED_SOURCE_COLUMNS}; observed {observed_columns}"
        )

    stream = status.getPath().getFileSystem(
        spark._jsc.hadoopConfiguration()
    ).open(status.getPath())
    try:
        source_sha256 = str(
            spark._jvm.org.apache.commons.codec.digest.DigestUtils.sha256Hex(
                stream
            )
        )
    finally:
        stream.close()

    return {
        "source_path": status.getPath().toString(),
        "filename": filename,
        "size_bytes": int(status.getLen()),
        "modification_time_ms": int(status.getModificationTime()),
        "source_sha256": source_sha256,
    }


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(conf)
    return bool(filesystem.exists(hadoop_path))


def delete_dataset(spark: SparkSession, path: str) -> None:
    """Delete one explicit generated dataset before a local full refresh."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(conf)
    if filesystem.exists(hadoop_path) and not filesystem.delete(hadoop_path, True):
        raise OSError(f"Unable to replace generated dataset: {path}")
