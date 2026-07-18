# Operations Runbook — Network Metrics Pipeline

## 1. Purpose and scope

This runbook covers local operation of the PySpark network-metrics pipeline:

- prepare and verify the runtime;
- execute the pipeline or reviewer notebook;
- confirm the expected outputs and quality checks;
- diagnose common failures;
- recover and rerun safely.

Architecture and production design are documented in
[`architecture.md`](architecture.md). Assignment coverage is documented in
[`assignment_compliance.md`](assignment_compliance.md).

## 2. Pipeline order

Stages must run in this order:

```text
bronze -> silver -> gold-hourly -> gold-daily -> monitoring
```

When stages run separately, use the same `run_id` for every stage. The sample
processing date is `2025-07-23`.

## 3. Runtime requirements

| Component | Local recommendation |
|---|---|
| Python | 3.10 or 3.11 |
| Java | JDK 17 |
| PySpark | 3.5.x |
| Storage | Local Parquet |
| Windows shell | PowerShell 5.1 or 7 |

Native Windows Spark may require `winutils.exe` and `hadoop.dll` under
`%USERPROFILE%\hadoop\bin`. These community compatibility files are not part
of the repository. Prefer WSL or Linux if organisational policy does not allow
them.

## 4. Initial setup

Run commands from the repository root.

### Windows PowerShell

```powershell
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test,demo]"
.\scripts\check_environment.ps1
```

### Linux, macOS or WSL

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test,demo]"
java -version
python -c "import pyspark; print(pyspark.__version__)"
command -v spark-submit
```

The environment is ready when Java, Python, PySpark and `spark-submit` are
available and the following files exist:

```text
config/pipeline.example.json
sample_data/network_metrics_20250723.csv
src/network_metrics/main.py
```

## 5. Pre-run checks

Run the automated tests:

```bash
python -m pytest -q
```

Expected result:

```text
15 passed
```

Confirm the configured processing date and thresholds in
`config/pipeline.example.json`. The expected source contract is:

```text
network_metrics_YYYYMMDD.csv
tower_id,region,timestamp,signal_strength,data_volume_mb
```

## 6. Execute the pipeline

### Fast full run

Windows:

```powershell
.\scripts\run_local.ps1
```

Linux, macOS or WSL:

```bash
bash scripts/run_local.sh
```

These commands run all five stages in one Spark application.

### Stage-by-stage run with previews

Use this mode for troubleshooting or demonstrations.

Windows:

```powershell
.\scripts\run_stage_by_stage.ps1
```

Linux, macOS or WSL:

```bash
bash scripts/run_stage_by_stage.sh
```

Both runners use `run_id=local-20250723-001` for every stage and stop on the
first failure.

## 7. Expected sample result

For `sample_data/network_metrics_20250723.csv`:

| Check | Expected result |
|---|---:|
| Bronze rows | 100 |
| Silver rows | 100 |
| Invalid rows | 0 |
| Warning-only rows | 4 |
| Hourly Gold rows | 74 |
| Daily Gold rows | 8 |
| Silver/Gold mismatches | 0 |

The four warnings are expected: their event timestamps cross into
`2025-07-24`, although the source filename is dated `2025-07-23`. They remain
valid and are aggregated using the actual event date.

Generated datasets are written under:

```text
output/
├── 01_bronze/network_metrics/
├── 02_silver/network_metrics/
├── 02_silver/network_metrics_quarantine/  # only when invalid rows exist
├── 03_gold/region_hourly_metrics/
├── 03_gold/region_daily_metrics/
└── 04_monitoring/
    ├── pipeline_run_logs/
    ├── data_quality_logs/
    ├── processing_manifest/
    └── hourly_anomalies/
```

## 8. Operational validation

Preview a dataset without opening Parquet files directly:

```bash
spark-submit scripts/preview_output.py --output-root output --dataset silver --rows 20
spark-submit scripts/preview_output.py --output-root output --dataset gold-hourly --rows 20
spark-submit scripts/preview_output.py --output-root output --dataset gold-daily --rows 20
spark-submit scripts/preview_output.py --output-root output --dataset quality-logs --rows 50
spark-submit scripts/preview_output.py --output-root output --dataset run-logs --rows 50
spark-submit scripts/preview_output.py --output-root output --dataset manifest --rows 20
spark-submit scripts/preview_output.py --output-root output --dataset anomalies --rows 50
```

A successful sample run has:

- five `SUCCESS` run-log entries: Bronze, Silver, hourly Gold, daily Gold and
  monitoring;
- `row_quality_summary = WARN` because of the four date warnings;
- `silver_gold_hourly_reconciliation = PASS`;
- `silver_gold_daily_reconciliation = PASS`;
- `historical_anomaly_detection = INSUFFICIENT_HISTORY` because the sample has
  not yet accumulated seven comparable region/hour observations;
- one `SUCCESS` event exists in the processing manifest when `--stage all` is
  used.

Treat `FAILED`, reconciliation `FAIL`, or a missing stage log as an operational
failure. Do not publish Gold output until the failure is resolved.

## 9. Reviewer notebook

Start Jupyter from the repository root:

```bash
jupyter lab notebooks/01_end_to_end_demo.ipynb
```

The notebook contains saved reference outputs. Use **Restart Kernel and Run All
Cells** to reproduce them. Each execution writes to an isolated
`output/demo/<run-id>/` directory.

## 10. Failure handling

| Symptom | Operator action |
|---|---|
| Expected input file is missing | Confirm processing date, input path and `network_metrics_YYYYMMDD.csv` filename. |
| More than one matching CSV exists | Retain the approved source file and move duplicates outside the landing path. |
| Header/schema validation fails | Compare the header with the five-column contract; contact the source-data owner before changing the schema. |
| Invalid-row rate is at or above 1% | Inspect the quarantine and quality logs; correct the source or approved rule, then rerun with a new run ID. |
| Reconciliation fails | Block Gold publication, compare Silver and daily Gold by region/date, fix the cause and rerun Gold plus monitoring. |
| Windows reports missing Hadoop binaries | Run `scripts\check_environment.ps1`; install approved compatible files or use WSL/Linux. |
| Spark cannot bind its driver | Set `SPARK_LOCAL_IP=127.0.0.1` and rerun. |
| Logs contain entries from earlier demonstrations | Use a new output root or perform the clean-rerun procedure below. |

The application records the failed stage and exits with status code `1`.
Production orchestration should retry transient infrastructure failures only;
schema, source-contract and quality failures require investigation.

## 11. Clean rerun and recovery

The following removes only generated local output. Preserve it first when it is
needed for incident analysis.

Windows:

```powershell
Remove-Item -Recurse -Force .\output -ErrorAction SilentlyContinue
.\scripts\run_stage_by_stage.ps1
```

Linux, macOS or WSL:

```bash
rm -rf ./output
bash scripts/run_stage_by_stage.sh
```

After the rerun, confirm:

1. all five stage logs are `SUCCESS`;
2. invalid and warning counts are understood;
3. both hourly and daily reconciliations are `PASS`;
4. hourly and daily row counts match expectations;
5. the recovery is recorded in the incident or assessment notes.

## 12. Escalation information

Include the following when escalating:

- environment and Spark version;
- processing date, run ID and failed stage;
- input and output paths;
- exception type and message;
- observed and expected quality values;
- quarantine location, if created;
- last successful run and remediation attempted.

Assign the production contacts before deployment:

| Responsibility | Contact |
|---|---|
| Pipeline owner | To be assigned |
| Source-data owner | To be assigned |
| Spark/platform support | To be assigned |
| On-call channel | To be assigned |
