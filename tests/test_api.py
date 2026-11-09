import pytest
from fastapi.testclient import TestClient

from flowlens.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    import flowlens.main as service
    monkeypatch.setattr(service, "DB_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as test_client:
        yield test_client


def test_health(client):
    assert client.get("/api/v1/health").json()["status"] == "ok"


def test_ingest_and_metrics(client):
    response = client.post("/api/v1/runs", json={
        "pipeline": "orders-etl", "status": "success",
        "duration_ms": 120, "records_processed": 12,
    })
    assert response.status_code == 201
    metrics = client.get("/api/v1/pipelines/orders-etl/metrics")
    assert metrics.status_code == 200
    assert metrics.json()["total_runs"] == 1
    assert metrics.json()["success_rate"] == 1


def test_failed_run_creates_alert(client):
    response = client.post("/api/v1/runs", json={
        "pipeline": "billing", "status": "failed",
        "duration_ms": 10, "error": "source unavailable",
    })
    assert response.status_code == 201
    alerts = client.get("/api/v1/alerts").json()["items"]
    assert any(alert["type"] == "failure" for alert in alerts)


def test_invalid_run_is_rejected(client):
    response = client.post("/api/v1/runs", json={
        "pipeline": "bad name", "status": "success", "duration_ms": -1,
    })
    assert response.status_code == 422


def test_pipeline_registration(client):
    response = client.post("/api/v1/pipelines", json={"name": "daily-load", "description": "Daily ETL"})
    assert response.status_code == 201
    assert client.get("/api/v1/pipelines").json()["items"][0]["name"] == "daily-load"
