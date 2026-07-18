from network_metrics import alerting


class FakeTaskInstance:
    task_id = "run_incremental_pipeline"
    try_number = 2
    log_url = "https://airflow.example/log"


class FakeDagRun:
    run_id = "scheduled__2025-07-23"


class FakeResponse:
    status = 202

    def close(self):
        return None


def test_failure_callback_builds_and_sends_actionable_event(monkeypatch):
    delivered = []
    monkeypatch.setattr(
        alerting,
        "send_alert_event",
        lambda event: delivered.append(event) or True,
    )

    alerting.airflow_failure_callback(
        {
            "task_instance": FakeTaskInstance(),
            "dag_run": FakeDagRun(),
            "ds": "2025-07-23",
            "exception": RuntimeError("Silver quality threshold exceeded"),
        }
    )

    assert len(delivered) == 1
    assert delivered[0]["status"] == "FAILED"
    assert delivered[0]["stage"] == "run_incremental_pipeline"
    assert "quality threshold" in delivered[0]["message"]
    assert delivered[0]["details"]["log_url"] == "https://airflow.example/log"


def test_webhook_delivery_posts_json_payload():
    captured = {}

    def opener(request, timeout):
        captured["body"] = request.data.decode("utf-8")
        captured["timeout"] = timeout
        return FakeResponse()

    delivered = alerting.send_alert_event(
        {"status": "FAILED", "text": "pipeline failed"},
        webhook_url="https://alerts.example/webhook",
        opener=opener,
    )

    assert delivered is True
    assert '"status": "FAILED"' in captured["body"]
    assert captured["timeout"] == 10


def test_recovery_callback_sends_only_after_a_retry(monkeypatch):
    delivered = []
    monkeypatch.setattr(
        alerting,
        "send_alert_event",
        lambda event: delivered.append(event) or True,
    )

    alerting.airflow_recovery_callback(
        {
            "task_instance": FakeTaskInstance(),
            "dag_run": FakeDagRun(),
            "ds": "2025-07-23",
        }
    )

    assert delivered[0]["status"] == "RECOVERED"

    first_attempt = type(
        "FirstAttempt", (), {"task_id": "pipeline", "try_number": 1}
    )()
    alerting.airflow_recovery_callback({"task_instance": first_attempt})
    assert len(delivered) == 1
