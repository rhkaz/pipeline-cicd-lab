from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime settings containing no platform-specific assumptions."""

    input_path: str
    output_root: str
    processing_date: str
    timezone: str = "UTC"
    signal_strength_min: float = -140.0
    signal_strength_max: float = -20.0
    invalid_rate_warning_threshold: float = 0.001
    invalid_rate_failure_threshold: float = 0.01
    anomaly_lookback_days: int = 28
    anomaly_min_history: int = 7
    anomaly_mad_threshold: float = 6.0

    def __post_init__(self) -> None:
        if not self.input_path.strip() or not self.output_root.strip():
            raise ValueError("input_path and output_root must not be blank")
        try:
            date.fromisoformat(self.processing_date)
        except ValueError as exc:
            raise ValueError(
                "processing_date must use YYYY-MM-DD format"
            ) from exc
        if not self.timezone.strip():
            raise ValueError("timezone must not be blank")
        if self.signal_strength_min >= self.signal_strength_max:
            raise ValueError(
                "signal_strength_min must be lower than signal_strength_max"
            )
        if not (
            0 <= self.invalid_rate_warning_threshold
            <= self.invalid_rate_failure_threshold
            <= 1
        ):
            raise ValueError(
                "Invalid-rate thresholds must satisfy 0 <= warning <= failure <= 1"
            )
        if self.anomaly_lookback_days < 1:
            raise ValueError("anomaly_lookback_days must be positive")
        if not 1 <= self.anomaly_min_history <= self.anomaly_lookback_days:
            raise ValueError(
                "anomaly_min_history must be between 1 and anomaly_lookback_days"
            )
        if self.anomaly_mad_threshold <= 0:
            raise ValueError("anomaly_mad_threshold must be positive")

    @classmethod
    def from_json(cls, path: str | Path) -> "PipelineConfig":
        payload: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(**payload)

    def with_overrides(self, **overrides: Any) -> "PipelineConfig":
        defined = {key: value for key, value in overrides.items() if value is not None}
        return replace(self, **defined)
