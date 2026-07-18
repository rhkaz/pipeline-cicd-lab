from network_metrics.manifest import (
    successful_manifest_entry,
    write_manifest_event,
)


def test_successful_manifest_entry_supports_idempotent_skip(spark, tmp_path):
    path = str(tmp_path / "manifest")
    common = {
        "source_path": "file:///network_metrics_20250723.csv",
        "source_sha256": "abc123",
        "processing_date": "2025-07-23",
        "run_id": "run-1",
    }
    write_manifest_event(spark, path, status="RUNNING", **common)
    assert successful_manifest_entry(spark, path, "abc123") is None

    write_manifest_event(spark, path, status="SUCCESS", **common)
    completed = successful_manifest_entry(spark, path, "abc123")

    assert completed is not None
    assert completed["run_id"] == "run-1"
    assert completed["status"] == "SUCCESS"
