# S3 Operations Runbook

## 1. Purpose

This runbook explains how to run the telecom network-metrics PySpark pipeline
from VS Code on Windows while reading and writing data in Amazon S3. It covers
first-time checks, the repeatable run procedure, output validation, reruns and
common failures.

The validated deployment uses one private S3 bucket with separate prefixes:

```text
s3://ndi-network-landing/
├── network_metrics_20250723.csv
└── processed/network-metrics/
    ├── 01_bronze/
    ├── 02_silver/
    ├── 03_gold/
    └── 04_monitoring/
```

Using one bucket does not weaken the medallion design: the Bronze, Silver and
Gold datasets are isolated by S3 prefixes and have distinct schemas and grains.
A deployment may use separate landing and medallion buckets without changing
the PySpark transformations.

## 2. Validated runtime

The following combination has been validated for this project:

| Component | Validated value |
| --- | --- |
| Operating system | Windows 11 |
| Shell | VS Code PowerShell |
| Python | 3.11 virtual environment |
| PySpark | 3.5.x |
| Java | 17 |
| Hadoop runtime | 3.3.4 |
| S3 connector | `hadoop-aws:3.3.4` |
| AWS CLI | Version 2 |
| AWS CLI profile | `ndi-dev` |

Python 3.11 is supported by this project. The `hadoop-aws` version must match
the Hadoop version bundled with PySpark.

## 3. Storage contract

In the reference architecture, the S3 Bronze, Silver and Gold prefixes form the
**Medallion Data Plane**. The commands in this runbook perform the current
**Pipeline Control Plane** responsibilities: authenticate, start the Spark job,
check its status and verify quality and reconciliation outputs. A production
orchestrator can automate those same steps without changing the data model.

### Landing object

The pipeline derives the expected filename from `processing_date`:

```text
s3://ndi-network-landing/network_metrics_20250723.csv
```

Required header:

```text
tower_id,region,timestamp,signal_strength,data_volume_mb
```

### Generated datasets

| Dataset | S3 prefix |
| --- | --- |
| Bronze | `01_bronze/network_metrics/` |
| Silver | `02_silver/network_metrics/` |
| Quarantine | `02_silver/network_metrics_quarantine/` |
| Hourly Gold | `03_gold/region_hourly_metrics/` |
| Daily Gold | `03_gold/region_daily_metrics/` |
| Data-quality logs | `04_monitoring/data_quality_logs/` |
| Pipeline-run logs | `04_monitoring/pipeline_run_logs/` |
| Processing manifest | `04_monitoring/processing_manifest/` |
| Hourly anomalies | `04_monitoring/hourly_anomalies/` |

Spark stores the datasets as Snappy-compressed Parquet objects. `_SUCCESS`
objects are zero-byte markers showing that an individual dataset write
completed. They do not, by themselves, prove that the entire pipeline passed.

## 4. First-time workstation setup

Run these commands from the repository root.

### 4.1 Activate the project environment

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

If the environment does not exist:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test,demo]"
```

### 4.2 Configure Windows Hadoop compatibility

The expected compatibility files are:

```text
%USERPROFILE%\hadoop\bin\winutils.exe
%USERPROFILE%\hadoop\bin\hadoop.dll
```

Set the current terminal environment:

```powershell
$env:HADOOP_HOME = "$env:USERPROFILE\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

Then validate the environment:

```powershell
.\scripts\check_environment.ps1
```

Expected final line:

```text
Environment is ready.
```

### 4.3 Make AWS CLI available

If `aws` is not recognised but AWS CLI is installed in its default location:

```powershell
$env:PATH = "C:\Program Files\Amazon\AWSCLIV2;$env:PATH"
aws --version
```

Closing and reopening all VS Code windows normally refreshes the permanent
Windows PATH after an AWS CLI installation.

### 4.4 Verify AWS access

```powershell
aws sso login --profile ndi-dev
aws sts get-caller-identity --profile ndi-dev
aws s3 ls --profile ndi-dev
```

The bucket `ndi-network-landing` must appear. Keep S3 Block Public Access
enabled and never store access keys in source code or JSON configuration.

## 5. Pipeline configuration

Open `config/pipeline.s3.example.json` in the VS Code editor. It is already set
to the validated bucket and processing date shown below. Do not paste JSON
directly into PowerShell. Replace the bucket name and dates only when running a
different deployment or daily batch.

```json
{
  "input_path": "s3a://ndi-network-landing",
  "output_root": "s3a://ndi-network-landing/processed/network-metrics",
  "processing_date": "2025-07-23",
  "timezone": "UTC",
  "signal_strength_min": -140.0,
  "signal_strength_max": -20.0,
  "invalid_rate_warning_threshold": 0.001,
  "invalid_rate_failure_threshold": 0.01,
  "anomaly_lookback_days": 28,
  "anomaly_min_history": 7,
  "anomaly_mad_threshold": 6.0
}
```

Important rules:

- `input_path` is the bucket or input prefix only.
- Do not append `network_metrics_20250723.csv` to `input_path`; the application
  derives and appends the expected filename.
- Use `s3a://` for Spark paths and `s3://` for AWS CLI commands.
- Keep the consolidated output root stable across processing dates.

Validate the saved configuration:

```powershell
Get-Content .\config\pipeline.s3.example.json
```

## 6. Repeatable run procedure

Perform this procedure for every new PowerShell terminal or expired AWS SSO
session.

### Step 1: Activate and initialise the terminal

```powershell
.\.venv\Scripts\Activate.ps1
$env:HADOOP_HOME = "$env:USERPROFILE\hadoop"
$env:PATH = "C:\Program Files\Amazon\AWSCLIV2;$env:HADOOP_HOME\bin;$env:PATH"
```

### Step 2: Authenticate and export temporary credentials

```powershell
aws sso login --profile ndi-dev
aws configure export-credentials `
  --profile ndi-dev `
  --format powershell |
  Invoke-Expression
```

The exported credentials exist only in the current terminal and expire. Do not
print, save or commit them.

### Step 3: Upload or verify the daily source

Upload the supplied assessment file:

```powershell
aws s3 cp `
  .\sample_data\network_metrics_20250723.csv `
  s3://ndi-network-landing/network_metrics_20250723.csv `
  --profile ndi-dev
```

Verify the exact object:

```powershell
aws s3 ls `
  s3://ndi-network-landing/network_metrics_20250723.csv `
  --profile ndi-dev
```

### Step 4: Run all pipeline stages

```powershell
.\.venv\Scripts\spark-submit.cmd `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider" `
  .\src\network_metrics\main.py `
  --config .\config\pipeline.s3.example.json `
  --stage all
```

Successful completion is indicated by both:

```text
Pipeline completed. run_id=<generated-run-id>
SparkContext is stopping with exitCode 0
```

Record the run ID for troubleshooting and validation evidence.

## 7. Verify S3 outputs

### 7.1 List all medallion objects

```powershell
aws s3 ls `
  s3://ndi-network-landing/processed/network-metrics/ `
  --recursive `
  --profile ndi-dev |
  Select-String "01_bronze|02_silver|03_gold|04_monitoring"
```

Expected prefixes:

```text
01_bronze/network_metrics/
02_silver/network_metrics/
03_gold/region_hourly_metrics/
03_gold/region_daily_metrics/
04_monitoring/data_quality_logs/
04_monitoring/pipeline_run_logs/
04_monitoring/processing_manifest/
04_monitoring/hourly_anomalies/
```

The quarantine prefix is absent when there are no invalid records.

### 7.2 Expected sample outcome

| Check | Expected result |
| --- | ---: |
| Bronze rows | 100 |
| Silver rows | 100 |
| Hourly Gold groups | 74 |
| Daily Gold groups | 8 |
| Cross-midnight warnings | 4 |
| Silver/Gold reconciliation mismatches | 0 |

Silver and Gold contain `event_date=2025-07-23` and
`event_date=2025-07-24`. Four source events legitimately cross midnight and
are retained under their actual UTC event date.

Bronze `_ingestion_date` is the date on which the pipeline was executed; it is
not expected to equal the historical processing date.

### 7.3 Preview the daily Gold data

```powershell
.\.venv\Scripts\spark-submit.cmd `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider" `
  .\scripts\preview_output.py `
  --output-root "s3a://ndi-network-landing/processed/network-metrics" `
  --dataset gold-daily `
  --rows 20
```

Valid preview dataset names are `bronze`, `silver`, `quarantine`,
`gold-hourly`, `gold-daily`, `run-logs`, `quality-logs`, `manifest` and
`anomalies`.

## 8. Rerun behaviour

Within the consolidated output root:

- Bronze retains the run partition for lineage.
- Silver, hourly Gold and daily Gold replace only affected event-date partitions.
- An identical source with an existing `SUCCESS` manifest entry is skipped.
- A stale quarantine partition is removed only for the same run ID.
- Pipeline and quality logs are appended, preserving earlier failed and
  successful attempts.
- A new automatically generated run ID identifies each execution.

An earlier `FAILED` run log is therefore expected to remain after a successful
rerun. Use the run ID and status fields to distinguish attempts.

## 9. Troubleshooting

| Symptom | Cause | Resolution |
| --- | --- | --- |
| `aws is not recognized` | AWS CLI directory is missing from the current PATH | Add `C:\Program Files\Amazon\AWSCLIV2` to `$env:PATH` or restart VS Code |
| `No module named pyspark` | Dependencies were not installed in the project `.venv` | Run `.\.venv\Scripts\python.exe -m pip install -e ".[test,demo]"` |
| `spark-submit not found` | PySpark is absent from the current `.venv` | Reinstall the project dependencies and rerun the environment check |
| `was unexpected at this time` | A Windows parent path contains parentheses such as `(1)` | Move the project to a clean path without parentheses and recreate `.venv` |
| `bucket is null/empty` | An empty PowerShell bucket variable produced `s3a://` | Use the saved JSON configuration or set and verify variables before submitting |
| Filename appears twice in the S3 path | `input_path` incorrectly contains the CSV filename | Set `input_path` to `s3a://ndi-network-landing` only |
| `No source file found` | The dated object is absent or misnamed | Upload `network_metrics_YYYYMMDD.csv` to the configured input root |
| `AccessDenied` or credential-provider error | SSO is expired, credentials were not exported, or IAM permissions are insufficient | Log in again, export credentials and verify bucket permissions |
| `ClassNotFoundException: ...S3AFileSystem` | The S3A connector was not supplied | Add `--packages org.apache.hadoop:hadoop-aws:3.3.4` |
| Hadoop version conflict | `hadoop-aws` does not match the bundled Hadoop version | Print Hadoop `VersionInfo` and use the identical `hadoop-aws` version |
| Failure deleting a temporary JAR after `Pipeline completed` | Windows still holds a temporary dependency file | Treat as a non-blocking cleanup warning when the pipeline already exited successfully |

To print the Hadoop version:

```powershell
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.getOrCreate(); print(s._jvm.org.apache.hadoop.util.VersionInfo.getVersion()); s.stop()"
```

## 10. Security and cost controls

- Keep S3 Block Public Access enabled.
- Use MFA and temporary SSO credentials; never create root-user access keys.
- Do not store credentials in configuration files, notebooks, screenshots or
  terminal transcripts.
- Restrict IAM permissions to the required bucket and prefixes.
- Create an AWS Budget alert before using chargeable compute services.
- This procedure runs Spark locally; S3 stores the data. It does not create an
  EMR or Glue compute resource.

## 11. Optional cleanup

List the exact generated prefix before deleting anything:

```powershell
aws s3 ls `
  s3://ndi-network-landing/processed/network-metrics/ `
  --recursive `
  --profile ndi-dev
```

Only when the complete processed dataset is confirmed as disposable, remove
the generated root. This does not remove the landing CSV or the bucket:

```powershell
aws s3 rm `
  s3://ndi-network-landing/processed/network-metrics/ `
  --recursive `
  --profile ndi-dev
```

## 12. Completion checklist

- [ ] Environment check passes.
- [ ] AWS identity and bucket listing succeed.
- [ ] Source object exists with the exact dated filename.
- [ ] S3 configuration contains an input root, not a filename.
- [ ] Spark reports `Pipeline completed` and exit code 0.
- [ ] Bronze, Silver, hourly Gold and daily Gold prefixes exist.
- [ ] Monitoring and data-quality logs exist.
- [ ] Daily Gold preview returns eight rows.
- [ ] No credentials are stored in the project.
