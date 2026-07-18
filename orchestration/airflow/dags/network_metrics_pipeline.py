"""Production reference DAG for the daily S3 network-metrics pipeline."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from network_metrics.alerting import (
    airflow_failure_callback,
    airflow_recovery_callback,
)


PROJECT_ROOT = os.getenv("NETWORK_METRICS_PROJECT_ROOT", "/opt/network-metrics")
BUCKET = os.getenv("NETWORK_METRICS_BUCKET", "ndi-network-landing")
AWS_CONNECTION = os.getenv("NETWORK_METRICS_AWS_CONNECTION", "aws_default")


with DAG(
    dag_id="network_metrics_daily",
    description="Validate and publish daily cell-tower performance metrics",
    schedule="0 02 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "network-data-engineering",
        "on_failure_callback": airflow_failure_callback,
    },
    tags=["network", "spark", "s3", "data-quality"],
) as dag:
    wait_for_daily_source = S3KeySensor(
        task_id="wait_for_daily_source",
        bucket_name=BUCKET,
        bucket_key="network_metrics_{{ ds_nodash }}.csv",
        aws_conn_id=AWS_CONNECTION,
        poke_interval=300,
        timeout=6 * 60 * 60,
        mode="reschedule",
    )

    run_incremental_pipeline = BashOperator(
        task_id="run_incremental_pipeline",
        bash_command=f"""
set -euo pipefail
spark-submit "{PROJECT_ROOT}/src/network_metrics/main.py" \\
  --config "{PROJECT_ROOT}/config/pipeline.s3.example.json" \\
  --input-path "s3a://{BUCKET}" \\
  --output-root "s3a://{BUCKET}/processed/network-metrics" \\
  --processing-date "{{{{ ds }}}}" \\
  --run-id "airflow-{{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}" \\
  --stage all
""",
        env={
            "PYTHONPATH": f"{PROJECT_ROOT}/src",
        },
        append_env=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(hours=2),
        on_success_callback=airflow_recovery_callback,
    )

    wait_for_daily_source >> run_incremental_pipeline
