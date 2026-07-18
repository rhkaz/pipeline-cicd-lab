# S3 medallion deployment mapping

## Purpose

The executable assessment uses local Parquet so reviewers do not need AWS
credentials. Spark uses the same Parquet reader/writer and Hadoop FileSystem
abstraction when the configured roots are `s3a://` URIs. No transformation code
changes are required.

The S3 prefixes below are the **Medallion Data Plane**. Submission, scheduling,
quality gates, run-state observation and reconciliation belong to the
cross-cutting **Pipeline Control Plane**; they govern the data flow rather than
forming another storage layer.

The example `config/pipeline.s3.example.json` uses one private bucket. Replace
`ndi-network-landing` with the deployment's bucket name. The orchestrator
substitutes only `processing_date`; the output root remains stable so historical
baselines and corrected date partitions can be consolidated.

For the complete validated Windows/VS Code execution procedure, output checks,
rerun behaviour and troubleshooting, see
[`s3_operations_runbook.md`](s3_operations_runbook.md).

## S3 prefix mapping

For the sample processing date, the source and generated prefixes are:

| Layer | S3 location |
| --- | --- |
| Landing | `s3://ndi-network-landing/network_metrics_20250723.csv` |
| Bronze | `s3://ndi-network-landing/processed/network-metrics/01_bronze/network_metrics/` |
| Silver | `s3://ndi-network-landing/processed/network-metrics/02_silver/network_metrics/` |
| Quarantine | `s3://ndi-network-landing/processed/network-metrics/02_silver/network_metrics_quarantine/` |
| Gold hourly | `s3://ndi-network-landing/processed/network-metrics/03_gold/region_hourly_metrics/` |
| Gold daily | `s3://ndi-network-landing/processed/network-metrics/03_gold/region_daily_metrics/` |
| Run logs | `s3://ndi-network-landing/processed/network-metrics/04_monitoring/pipeline_run_logs/` |
| Quality logs | `s3://ndi-network-landing/processed/network-metrics/04_monitoring/data_quality_logs/` |
| Manifest | `s3://ndi-network-landing/processed/network-metrics/04_monitoring/processing_manifest/` |
| Hourly anomalies | `s3://ndi-network-landing/processed/network-metrics/04_monitoring/hourly_anomalies/` |

Bronze retains each run, while Silver, Gold and anomaly datasets replace only
affected `event_date` partitions. This prevents cross-midnight events from being
double-counted when the next daily file arrives. SHA-256 source identity plus a
successful processing-manifest event makes identical reruns no-ops. A production
deployment can use separate landing and curated buckets without changing the
transformations.

## Spark submission

The selected Spark runtime must provide a compatible S3A connector. Credentials
must come from an instance profile, workload role or equivalent identity; never
place access keys in the JSON configuration.

```bash
export PYTHONPATH="$PWD/src"

spark-submit \
  src/network_metrics/main.py \
  --config config/pipeline.s3.example.json \
  --stage all
```

Managed Spark platforms normally supply the S3A libraries and workload identity.
For a self-managed cluster, add the `hadoop-aws` package version compatible with
that cluster's Hadoop distribution.

## Operational controls

- Configure one scheduled run per processing date.
- Wait for the expected object before submitting Spark.
- Use the included Airflow DAG for the deployable reference workflow.
- Reuse the same run ID when stages are submitted separately.
- Enable S3 versioning, encryption and least-privilege IAM policies.
- Retry transient S3 or compute failures; do not retry deterministic schema or
  quality failures without investigation.
- Compact small Parquet files when retained batch volume justifies it.
