# FlowLens

**Open-source data pipeline observability for developers.**

FlowLens is a self-hostable control plane for monitoring data pipelines. It accepts run events, tracks pipeline health and duration, detects unusual failures and latency using a transparent rolling-baseline detector, and exposes a live dashboard and API.

## What it does

- Register pipelines and ingest run events.
- Track success, failure, duration, and recent run history.
- Detect failure spikes and latency anomalies from recent historical runs.
- Inspect pipeline health, metrics, and alerts through a REST API.
- Explore a responsive browser dashboard with auto-refresh.
- Run locally with SQLite or deploy with Docker.
- Validate behavior with automated tests and GitHub Actions.

## Quick start

Requirements: Python 3.11+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn flowlens.main:app --reload
```

Open http://127.0.0.1:8000. API docs: http://127.0.0.1:8000/docs.

## Send a run event

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs \
  -H "Content-Type: application/json" \
  -d '{"pipeline":"orders-etl","status":"success","duration_ms":1250,"records_processed":4200}'
```

Create a failure to see alerting:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/runs \
  -H "Content-Type: application/json" \
  -d '{"pipeline":"orders-etl","status":"failed","duration_ms":800,"error":"Upstream API timeout"}'
```

## Architecture

```text
Producer / Scheduler
       |
       v
 FastAPI ingestion API
       |
       v
 SQLite event store ----> Rolling anomaly detector
       |                            |
       +------------+---------------+
                    v
             Metrics & alerts API
                    |
                    v
             Web dashboard
```

The initial release intentionally uses a lightweight SQLite store and deterministic statistical rules so it is easy to run, inspect, and extend. It does not claim to provide distributed tracing or replace a production metrics stack.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `FLOWLENS_DB_PATH` | `./flowlens.db` | SQLite database file |
| `FLOWLENS_CORS_ORIGINS` | `*` | Comma-separated allowed origins |
| `FLOWLENS_ANOMALY_MIN_SAMPLES` | `5` | Historical runs required before anomaly checks |
| `FLOWLENS_ANOMALY_LATENCY_FACTOR` | `2.0` | Latency threshold relative to baseline |

## API

- `GET /api/v1/health`
- `GET /api/v1/pipelines`
- `POST /api/v1/pipelines`
- `POST /api/v1/runs`
- `GET /api/v1/pipelines/{name}/runs`
- `GET /api/v1/pipelines/{name}/metrics`
- `GET /api/v1/alerts`

Interactive OpenAPI documentation is available at `/docs`.

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
ruff format --check .
```

## Security notes

FlowLens is an observability starter, not a hardened multi-tenant SaaS. Put it behind authentication and TLS before exposing it publicly. Do not send secrets or personal data in run metadata. CORS is configurable; set an explicit origin list for deployments.

## Roadmap

- [ ] PostgreSQL storage adapter
- [ ] Authentication and role-based access
- [ ] Webhook notifications
- [ ] OpenTelemetry ingestion
- [ ] Multi-worker queue for high-volume ingestion

## License

MIT
