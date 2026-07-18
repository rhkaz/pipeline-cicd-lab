# Assignment compliance review

This page maps the take-home brief to the submitted implementation. It is a
review aid, not a second specification.

| Requirement | Implementation evidence | Status |
| --- | --- | --- |
| Detect the daily S3-style file | `filesystem.source_glob()` and `validate_source_file()` resolve exactly one date-named object through Hadoop FileSystem | Implemented; local sample path used for the demo |
| Bronze-Silver-Gold pipeline | `jobs.py`, stage wrappers under `scripts/`, and `main.py --stage all` | Implemented |
| Unix timestamp conversion | `build_silver_candidate()` creates UTC event timestamp, hour and date | Implemented |
| Region/hour aggregation | `build_hourly_gold()` calculates volume sum and signal average | Implemented |
| Region/day report | `build_daily_gold()` calculates volume sum and signal average | Implemented |
| Mock table views | CSV previews under `mock_tables/` | Implemented |
| File and schema validation | Filename, existence, single-file, size and exact-header checks | Implemented |
| Null/range/duplicate DQ | Silver quality arrays, quarantine and configurable failure thresholds | Implemented |
| Validation logs | Static PASS/WARN evidence, one labelled synthetic FAIL example, and executable Parquet quality/run logs | Implemented |
| Anomaly detection | `anomaly.py` calculates median/MAD scores for volume, signal, record count and active towers and persists row-level reasons | Implemented; sample returns `INSUFFICIENT_HISTORY` by design |
| Monitoring and failure handling | Per-stage records, hourly/daily reconciliation, original exception preservation and process exit code 1 | Implemented |
| Incident notifications | Airflow callbacks send structured failure/recovery webhook events or emit the same JSON to logs | Implemented and unit tested |
| Scheduling and freshness | Airflow 3 DAG with a daily schedule and `S3KeySensor` | Implemented as a deployable reference workflow |
| Idempotency and incremental publishing | SHA-256 processing manifest, retained Bronze history and affected-date replacement | Implemented |
| CI and submission hygiene | GitHub Actions plus `scripts/validate_submission.py` | Implemented |
| S3 medallion model | Same PySpark/Parquet pipeline supports `s3a://` roots; concrete daily S3 config and prefix mapping supplied | Implemented as deployment configuration; AWS identity remains external |
| README architecture/tasks/assumptions | `README.md` and `docs/architecture.md` | Implemented |
| Runnable demonstration | `notebooks/01_end_to_end_demo.ipynb` imports and executes shared pipeline modules | Implemented |

## Important engineering choices

- Invalid rows are explainable: error arrays remain on quarantine records.
- Cross-midnight records are warnings, not automatic failures, because the
  sample contains four legitimate 2025-07-24 events in the 2025-07-23 file.
- Duplicate keys are invalid within one ingestion run. Across runs, retained
  Bronze history resolves the latest version and republishes affected dates.
- Identical source content is skipped only after a `SUCCESS` manifest event.
- A failure-threshold breach is logged and quarantined before Silver is touched.
- Full UTC hour timestamps are used, preventing different dates from being
  mixed into the same hour-of-day bucket.
- Separately executed stages fail when their run ID does not match a Bronze run.
- A retry removes stale quarantine data for the same run partition while
  preserving quarantine history from other runs.

## Follow-up backlog for a production programme

1. Confirm the source-system business key and the expected cross-midnight policy.
2. Adopt a transactional table format if atomic multi-writer commits, schema
   evolution or time travel become mandatory.
3. Route the included webhook through the organisation's approved Teams or
   incident-management relay and provide the production runbook URL.
4. Retain enough history for the anomaly baseline and tune its thresholds using
   known incidents and planned-maintenance windows.
5. Add integration tests for missing files, malformed headers, threshold failure
   and late-arriving replay against an object-storage test environment.
6. Add dependency scanning and an object-storage smoke test to the included CI.
