from __future__ import annotations

import os
import sqlite3
import statistics
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DB_PATH = os.getenv("FLOWLENS_DB_PATH", "flowlens.db")
MIN_SAMPLES = int(os.getenv("FLOWLENS_ANOMALY_MIN_SAMPLES", "5"))
LATENCY_FACTOR = float(os.getenv("FLOWLENS_ANOMALY_LATENCY_FACTOR", "2.0"))


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def initialize() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('success','failed')),
            duration_ms REAL NOT NULL,
            records_processed INTEGER NOT NULL,
            error TEXT,
            created_at REAL NOT NULL
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_runs_pipeline_time ON runs(pipeline, created_at DESC)")


class RunIn(BaseModel):
    pipeline: str = Field(min_length=1, max_length=120, pattern=r"^[a-zA-Z0-9_.:-]+$")
    status: Literal["success", "failed"]
    duration_ms: float = Field(ge=0, le=86_400_000)
    records_processed: int = Field(default=0, ge=0)
    error: str | None = Field(default=None, max_length=2000)


class PipelineIn(BaseModel):
    name: str = Field(min_length=1, max_length=120, pattern=r"^[a-zA-Z0-9_.:-]+$")
    description: str = Field(default="", max_length=500)


def rows_for(name: str, limit: int = 100) -> list[dict]:
    with connect() as db:
        rows = db.execute(
            "SELECT * FROM runs WHERE pipeline=? ORDER BY created_at DESC,id DESC LIMIT ?",
            (name, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def anomaly_for(name: str) -> dict | None:
    runs = rows_for(name, max(MIN_SAMPLES + 1, 100))
    history = [r["duration_ms"] for r in runs[1:] if r["status"] == "success"]
    latest = runs[0] if runs else None
    if not latest:
        return None
    if latest["status"] == "failed":
        return {"type": "failure", "severity": "critical", "message": latest["error"] or "Pipeline run failed", "run_id": latest["id"]}
    if len(history) >= MIN_SAMPLES and latest["status"] == "success":
        baseline = statistics.median(history)
        if baseline > 0 and latest["duration_ms"] > baseline * LATENCY_FACTOR:
            return {"type": "latency", "severity": "warning", "message": f"Run took {latest['duration_ms']:.0f} ms; baseline median is {baseline:.0f} ms", "run_id": latest["id"]}
    return None


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize()
    yield


app = FastAPI(title="FlowLens API", version="0.1.0", description="Lightweight pipeline observability", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return HTMLResponse(DASHBOARD)


@app.get("/api/v1/health")
def health():
    return {"status": "ok", "service": "flowlens"}


@app.post("/api/v1/pipelines", status_code=201)
def create_pipeline(payload: PipelineIn):
    # Pipeline metadata is inferred from run events; this endpoint validates/records a zero-run pipeline.
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS pipelines (name TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL)")
        db.execute("INSERT OR IGNORE INTO pipelines(name,description,created_at) VALUES(?,?,?)", (payload.name, payload.description, time.time()))
    return {"name": payload.name, "description": payload.description}


@app.get("/api/v1/pipelines")
def list_pipelines():
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS pipelines (name TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL)")
        explicit = [dict(r) for r in db.execute("SELECT name,description FROM pipelines ORDER BY name").fetchall()]
        seen = {p["name"] for p in explicit}
        discovered = db.execute("SELECT DISTINCT pipeline FROM runs ORDER BY pipeline").fetchall()
    result = explicit + [{"name": r["pipeline"], "description": ""} for r in discovered if r["pipeline"] not in seen]
    return {"items": result}


@app.post("/api/v1/runs", status_code=201)
def ingest_run(payload: RunIn):
    now = time.time()
    with connect() as db:
        cur = db.execute(
            "INSERT INTO runs(pipeline,status,duration_ms,records_processed,error,created_at) VALUES(?,?,?,?,?,?)",
            (payload.pipeline, payload.status, payload.duration_ms, payload.records_processed, payload.error, now),
        )
        run_id = cur.lastrowid
    return {"id": run_id, "pipeline": payload.pipeline, "status": payload.status, "created_at": now, "anomaly": anomaly_for(payload.pipeline)}


@app.get("/api/v1/pipelines/{name}/runs")
def pipeline_runs(name: str, limit: int = Query(default=50, ge=1, le=500)):
    return {"items": rows_for(name, limit)}


@app.get("/api/v1/pipelines/{name}/metrics")
def pipeline_metrics(name: str):
    runs = rows_for(name, 500)
    if not runs:
        raise HTTPException(status_code=404, detail="Pipeline has no runs")
    durations = [r["duration_ms"] for r in runs]
    successes = sum(r["status"] == "success" for r in runs)
    return {
        "pipeline": name, "total_runs": len(runs), "successes": successes,
        "failures": len(runs) - successes, "success_rate": round(successes / len(runs), 4),
        "avg_duration_ms": round(statistics.mean(durations), 2),
        "latest_run": runs[0], "anomaly": anomaly_for(name),
    }


@app.get("/api/v1/alerts")
def alerts():
    with connect() as db:
        names = [r["pipeline"] for r in db.execute("SELECT DISTINCT pipeline FROM runs").fetchall()]
    return {"items": [a for name in names if (a := anomaly_for(name)) is not None]}


DASHBOARD = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FlowLens · Pipeline observability</title><style>
:root{color-scheme:dark;--bg:#0b1020;--panel:#121a2d;--line:#26334d;--muted:#94a3b8;--text:#e5edf8;--accent:#68e0c0}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80% 0,#172744,var(--bg) 45%);color:var(--text);font:15px system-ui,sans-serif}
main{max-width:1100px;margin:auto;padding:38px 22px}.top{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
h1{font-size:28px;margin:0}.sub{color:var(--muted);margin:7px 0 28px}.tag{color:var(--accent);font-size:12px;letter-spacing:2px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}.card,.panel{background:#121a2ddd;border:1px solid var(--line);border-radius:15px;padding:18px}
.k{color:var(--muted);font-size:12px}.v{font-size:28px;font-weight:700;margin-top:7px}.panel{margin-top:18px}h2{font-size:17px;margin:0 0 15px}
button{background:var(--accent);border:0;border-radius:9px;padding:10px 14px;font-weight:700;cursor:pointer}
table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:12px 8px;border-bottom:1px solid var(--line)}th{color:var(--muted);font-size:12px}
.pill{border-radius:30px;padding:4px 9px;font-size:11px;background:#183b35;color:#83f0c9}.failed{background:#48252c;color:#ff9da8}
.empty{color:var(--muted);padding:16px 0}@media(max-width:600px){main{padding:24px 14px}td,th{font-size:12px}}
</style></head><body><main><div class="top"><div><div class="tag">OBSERVABILITY / LIVE</div><h1>FlowLens</h1></div><button onclick="load()">Refresh data ↻</button></div>
<p class="sub">A clear view of your data pipelines. Auto-refreshes every 10 seconds.</p>
<div class="grid"><div class="card"><div class="k">Pipelines</div><div class="v" id="pipes">—</div></div><div class="card"><div class="k">Total runs</div><div class="v" id="runs">—</div></div><div class="card"><div class="k">Success rate</div><div class="v" id="rate">—</div></div><div class="card"><div class="k">Active alerts</div><div class="v" id="alerts">—</div></div></div>
<div class="panel"><h2>Pipeline health</h2><div id="table" class="empty">Loading…</div></div><div class="panel"><h2>Recent alerts</h2><div id="alertlist" class="empty">Loading…</div></div>
<p class="sub">API documentation · <a href="/docs" style="color:var(--accent)">OpenAPI / Swagger</a></p></main>
<script>
async function load(){try{const [p,a]=await Promise.all([fetch('/api/v1/pipelines').then(r=>r.json()),fetch('/api/v1/alerts').then(r=>r.json())]);let total=0,ok=0,html='';
for(const x of p.items){let m=await fetch('/api/v1/pipelines/'+encodeURIComponent(x.name)+'/metrics');if(!m.ok)continue;m=await m.json();total+=m.total_runs;ok+=m.successes;html+='<tr><td>'+esc(x.name)+'</td><td>'+m.total_runs+'</td><td>'+m.successes+'</td><td>'+m.failures+'</td><td><span class="pill '+(m.failures?'failed':'')+'">'+(m.failures?'Attention':'Healthy')+'</span></td><td>'+Math.round(m.avg_duration_ms)+' ms</td></tr>'}
document.getElementById('pipes').textContent=p.items.length;document.getElementById('runs').textContent=total;document.getElementById('rate').textContent=total?Math.round(ok/total*100)+'%':'—';document.getElementById('alerts').textContent=a.items.length;
document.getElementById('table').innerHTML=html?'<table><thead><tr><th>Pipeline</th><th>Runs</th><th>OK</th><th>Failed</th><th>Health</th><th>Avg duration</th></tr></thead><tbody>'+html+'</tbody></table>':'No runs yet. POST a run to /api/v1/runs to get started.';
document.getElementById('alertlist').innerHTML=a.items.length?a.items.map(x=>'<p><span class="pill failed">'+esc(x.severity)+'</span> '+esc(x.message)+' · '+esc(x.type)+'</p>').join(''):'No active alerts.'}catch(e){document.getElementById('table').textContent='Could not load API data: '+e}}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
load();setInterval(load,10000);
</script></body></html>"""
