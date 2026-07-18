# Airflow orchestration and alerting

The production reference workflow is
`orchestration/airflow/dags/network_metrics_pipeline.py`. It is deliberately
outside the PySpark transformation package: Airflow owns availability sensing,
scheduling, retries and notifications, while Spark owns data processing and
quality gates.

## Workflow

1. Run daily at 02:00 UTC.
2. Wait up to six hours for `network_metrics_YYYYMMDD.csv` in S3.
3. Submit the complete Spark pipeline with one processing date and run ID.
4. Retry a failed Spark submission twice with exponential backoff. Manifest
   idempotency makes every attempt safe; production environments can add
   platform-specific exit-code classification if deterministic failures should
   bypass retries.
5. Send a failure event for a missing source or failed Spark application.
6. Send a recovery event only when a retry succeeds.

The Spark manifest makes a repeated successful submission safe: an identical
SHA-256 source object is logged as `SKIPPED` and is not republished.

## Deployment settings

| Environment variable | Purpose |
| --- | --- |
| `NETWORK_METRICS_PROJECT_ROOT` | Location of the deployed repository on the Airflow worker |
| `NETWORK_METRICS_BUCKET` | Landing and processed S3 bucket |
| `NETWORK_METRICS_AWS_CONNECTION` | Airflow AWS connection used by the S3 sensor |
| `NETWORK_METRICS_ALERT_WEBHOOK_URL` | Teams workflow or alert-relay HTTPS endpoint |
| `NETWORK_METRICS_RUNBOOK_URL` | Link included in failure and recovery events |

Install the pinned Airflow deployment dependencies from
`orchestration/airflow/requirements.txt` in a Python 3.11 Linux environment.
The Spark runtime must still provide the S3A connector and an IAM workload role.

The webhook body includes status, pipeline ID, run ID, task, processing date,
error, attempt number, log URL and runbook URL. When the webhook is not set, the
same JSON is emitted as a structured `ALERT_EVENT` log rather than silently
discarded.
