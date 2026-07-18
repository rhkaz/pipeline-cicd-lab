import pytest

from network_metrics.config import PipelineConfig


def test_config_rejects_invalid_threshold_order():
    with pytest.raises(ValueError, match="warning <= failure"):
        PipelineConfig(
            input_path="input",
            output_root="output",
            processing_date="2025-07-23",
            invalid_rate_warning_threshold=0.2,
            invalid_rate_failure_threshold=0.1,
        )


def test_config_rejects_invalid_processing_date():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        PipelineConfig(
            input_path="input",
            output_root="output",
            processing_date="23-07-2025",
        )
