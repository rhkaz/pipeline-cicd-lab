from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelinePaths:
    """Logical medallion datasets stored as files, not database tables."""

    input_path: str
    output_root: str

    @property
    def bronze(self) -> str:
        return f"{self.output_root.rstrip('/')}/01_bronze/network_metrics"

    @property
    def silver(self) -> str:
        return f"{self.output_root.rstrip('/')}/02_silver/network_metrics"

    @property
    def quarantine(self) -> str:
        return f"{self.output_root.rstrip('/')}/02_silver/network_metrics_quarantine"

    @property
    def gold_hourly(self) -> str:
        return f"{self.output_root.rstrip('/')}/03_gold/region_hourly_metrics"

    @property
    def gold_daily(self) -> str:
        return f"{self.output_root.rstrip('/')}/03_gold/region_daily_metrics"

    @property
    def pipeline_logs(self) -> str:
        return f"{self.output_root.rstrip('/')}/04_monitoring/pipeline_run_logs"

    @property
    def quality_logs(self) -> str:
        return f"{self.output_root.rstrip('/')}/04_monitoring/data_quality_logs"

    @property
    def processing_manifest(self) -> str:
        return f"{self.output_root.rstrip('/')}/04_monitoring/processing_manifest"

    @property
    def hourly_anomalies(self) -> str:
        return f"{self.output_root.rstrip('/')}/04_monitoring/hourly_anomalies"
