from datetime import date, datetime, timedelta

from network_metrics.anomaly import build_hourly_anomalies


def hourly_history(spark, *, historical_days: int, anomalous_current: bool):
    current_date = date(2025, 7, 29)
    rows = []
    for offset in range(historical_days, 0, -1):
        event_date = current_date - timedelta(days=offset)
        rows.append(
            (
                "North",
                datetime.combine(event_date, datetime.min.time()),
                event_date,
                100.0,
                -80.0,
                20,
                10,
            )
        )
    rows.append(
        (
            "North",
            datetime.combine(current_date, datetime.min.time()),
            current_date,
            10.0 if anomalous_current else 100.0,
            -100.0 if anomalous_current else -80.0,
            2 if anomalous_current else 20,
            2 if anomalous_current else 10,
        )
    )
    return spark.createDataFrame(
        rows,
        """
        region string,
        event_hour_utc timestamp,
        event_date date,
        total_data_volume_mb double,
        average_signal_strength double,
        record_count long,
        distinct_tower_count long
        """,
    ), current_date


def test_median_mad_anomaly_detection_flags_network_degradation(spark):
    hourly, current_date = hourly_history(
        spark, historical_days=8, anomalous_current=True
    )

    result = build_hourly_anomalies(
        hourly,
        [current_date],
        lookback_days=28,
        min_history=7,
        mad_threshold=6.0,
    ).first()

    assert result.anomaly_status == "ANOMALY"
    assert "data_volume_deviation" in result.anomaly_reasons
    assert "signal_strength_degradation" in result.anomaly_reasons
    assert "active_tower_drop" in result.anomaly_reasons


def test_anomaly_detection_reports_insufficient_history(spark):
    hourly, current_date = hourly_history(
        spark, historical_days=2, anomalous_current=False
    )

    result = build_hourly_anomalies(
        hourly,
        [current_date],
        lookback_days=28,
        min_history=7,
        mad_threshold=6.0,
    ).first()

    assert result.anomaly_status == "INSUFFICIENT_HISTORY"
