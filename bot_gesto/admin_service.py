# admin_service.py
import os, json
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge
from db import init_db, get_unsent_leads
from redis import Redis

# ==============================
# Config
# ==============================
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

redis = Redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="Admin Service")

# ==============================
# Métricas Prometheus
# ==============================
LEADS_TOTAL = Counter("leads_total", "Total de leads processados", ["event_type"])
LEADS_SUCCESS = Counter("leads_success", "Eventos enviados com sucesso", ["event_type"])
LEADS_FAILED = Counter("leads_failed", "Eventos que falharam", ["event_type"])

PENDING_GAUGE = Gauge("leads_pending", "Leads pendentes no DB")

# ==============================
# Auth simples
# ==============================
def require_token(token: str):
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido")
    return True

# ==============================
# Endpoints
# ==============================
@app.on_event("startup")
async def startup_event():
    init_db()

@app.get("/health")
async def health():
    return {"status": "alive"}

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/stats")
async def stats(token: str = ""):
    require_token(token)
    try:
        leads = await get_unsent_leads(limit=100)
        PENDING_GAUGE.set(len(leads))
        return {"pending": len(leads)}
    except Exception as e:
        return {"error": str(e)}

@app.post("/retrofeed")
async def retrofeed(token: str = ""):
    """
    Retroalimenta leads não enviados de volta ao Redis para reprocessamento.
    """
    require_token(token)
    leads = await get_unsent_leads(limit=50)
    if not leads:
        return {"status": "no_leads"}

    for lead in leads:
        redis.xadd(STREAM, {"payload": json.dumps(lead)})

    return {"status": "requeued", "count": len(leads)}