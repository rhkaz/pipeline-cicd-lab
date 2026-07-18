from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.request import Request, urlopen


LOGGER = logging.getLogger("network-metrics-alerting")


def build_alert_event(
    *,
    status: str,
    pipeline_id: str,
    run_id: str,
    stage: str,
    message: str,
    processing_date: str | None = None,
    runbook_url: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headline = f"[{status}] {pipeline_id} / {stage}"
    return {
        "text": f"{headline}: {message}",
        "status": status,
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "stage": stage,
        "processing_date": processing_date,
        "message": message,
        "runbook_url": runbook_url,
        "details": details or {},
        "event_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


def send_alert_event(
    event: dict[str, Any],
    *,
    webhook_url: str | None = None,
    opener: Callable[..., Any] = urlopen,
    timeout_seconds: int = 10,
) -> bool:
    """Send a generic JSON webhook suitable for a Teams workflow or relay."""
    destination = webhook_url or os.getenv("NETWORK_METRICS_ALERT_WEBHOOK_URL")
    if not destination:
        LOGGER.warning("ALERT_EVENT %s", json.dumps(event, sort_keys=True))
        return False
    request = Request(
        destination,
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    response = opener(request, timeout=timeout_seconds)
    try:
        status = int(getattr(response, "status", 200))
        if not 200 <= status < 300:
            raise RuntimeError(f"Alert endpoint returned HTTP {status}")
    finally:
        close = getattr(response, "close", None)
        if close:
            close()
    return True


def _airflow_context_payload(
    context: dict[str, Any], status: str
) -> dict[str, Any]:
    task_instance = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id", "unknown")
    stage = getattr(task_instance, "task_id", "unknown")
    exception = context.get("exception")
    message = str(exception) if exception else f"Task completed with status {status}"
    processing_date = context.get("ds")
    return build_alert_event(
        status=status,
        pipeline_id="network_metrics_medallion",
        run_id=str(run_id),
        stage=str(stage),
        message=message,
        processing_date=str(processing_date) if processing_date else None,
        runbook_url=os.getenv("NETWORK_METRICS_RUNBOOK_URL"),
        details={
            "try_number": getattr(task_instance, "try_number", None),
            "log_url": getattr(task_instance, "log_url", None),
        },
    )


def airflow_failure_callback(context: dict[str, Any]) -> None:
    event = _airflow_context_payload(context, "FAILED")
    try:
        send_alert_event(event)
    except Exception:
        LOGGER.exception("Unable to deliver pipeline failure alert")


def airflow_recovery_callback(context: dict[str, Any]) -> None:
    task_instance = context.get("task_instance") or context.get("ti")
    try_number = int(getattr(task_instance, "try_number", 1) or 1)
    if try_number <= 1:
        LOGGER.info("Task succeeded on its first attempt; no recovery alert required")
        return
    event = _airflow_context_payload(context, "RECOVERED")
    try:
        send_alert_event(event)
    except Exception:
        LOGGER.exception("Unable to deliver pipeline recovery alert")
