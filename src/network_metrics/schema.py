from pyspark.sql import types as T


NETWORK_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("tower_id", T.IntegerType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("timestamp", T.LongType(), True),
        T.StructField("signal_strength", T.DoubleType(), True),
        T.StructField("data_volume_mb", T.DoubleType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)

REQUIRED_SOURCE_COLUMNS = [
    "tower_id",
    "region",
    "timestamp",
    "signal_strength",
    "data_volume_mb",
]
