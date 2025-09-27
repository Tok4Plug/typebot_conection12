# ==========================================
# bot_gesto/admin_service.py — v1.2 atualizado
# ==========================================
import os, json, time, logging
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge
from redis import Redis
from bot_gesto.db import init_db, get_unsent_leads
from bot_gesto.utils import clamp_event_time, build_event_id

# ==============================
# Configurações
# ==============================
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

# Redis (opcional)
redis = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# FastAPI app
app = FastAPI(title="Admin Service", version="1.2.0")

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("admin_service")

# ==============================
# Métricas Prometheus
# ==============================
LEADS_TOTAL = Counter("leads_total", "Total de leads processados", ["event_type"])
LEADS_SUCCESS = Counter("leads_success", "Eventos enviados com sucesso", ["event_type"])
LEADS_FAILED = Counter("leads_failed", "Eventos que falharam", ["event_type"])
PENDING_GAUGE = Gauge("leads_pending", "Leads pendentes no DB")
RETROFEED_RUNS = Counter("retrofeed_runs_total", "Número de execuções do retrofeed")
RETROFEED_ENRICHED = Counter("retrofeed_enriched_total", "Leads enriquecidos no retrofeed")

# ==============================
# Auth simples (aceita token direto ou Bearer)
# ==============================
def require_token(token: str = "", authorization: str = ""):
    supplied = token or (authorization.split(" ")[1] if authorization.startswith("Bearer ") else "")
    if ADMIN_TOKEN and supplied != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido")
    return True

# ==============================
# Enriquecimento para retrofeed
# ==============================
def enrich_for_retrofeed(lead: dict, default_event: str = "Lead") -> dict:
    """
    Garante campos essenciais para reprocessamento confiável no pixel.
    Não inventa dados sensíveis, só preenche os mínimos faltantes.
    """
    lead = dict(lead)  # cópia defensiva
    ts = int(lead.get("event_time") or time.time())
    lead["event_time"] = clamp_event_time(ts)

    if not lead.get("_fbp") and lead.get("telegram_id"):
        lead["_fbp"] = f"fb.1.{int(time.time())}.{lead['telegram_id']}"

    if not lead.get("_fbc") and lead.get("telegram_id"):
        lead["_fbc"] = f"fb.1.{int(time.time())}.retro.{lead['telegram_id']}"

    lead["external_id"] = lead.get("external_id") or str(lead.get("telegram_id") or "")
    lead["telegram_id"] = str(lead.get("telegram_id") or "")

    # Deduplication event_id
    lead["event_id"] = build_event_id(default_event, lead, lead["event_time"])

    return lead

# ==============================
# Lifecycle
# ==============================
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Inicializando Admin Service...")
    init_db()
    logger.info("✅ DB inicializado com sucesso")

# ==============================
# Endpoints
# ==============================
@app.get("/health")
async def health():
    redis_status = "ok"
    try:
        if redis:
            redis.ping()
    except Exception as e:
        redis_status = f"erro: {e}"

    return {
        "status": "alive",
        "version": "1.2.0",
        "redis": redis_status
    }

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/stats")
async def stats(token: str = "", authorization: str = Header(default="")):
    require_token(token, authorization)
    try:
        leads = await get_unsent_leads(limit=100)
        pending_count = len(leads)
        PENDING_GAUGE.set(pending_count)
        return {"pending": pending_count}
    except Exception as e:
        logger.error(f"❌ Erro em /stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/retrofeed")
async def retrofeed(token: str = "", authorization: str = Header(default="")):
    """
    Retroalimenta leads não enviados de volta ao Redis para reprocessamento.
    Garante enriquecimento mínimo antes de reencaminhar.
    """
    require_token(token, authorization)
    if not redis:
        raise HTTPException(status_code=500, detail="Redis não configurado")

    leads = await get_unsent_leads(limit=50)
    if not leads:
        logger.info("♻️ Nenhum lead pendente para retrofeed")
        return {"status": "no_leads"}

    RETROFEED_RUNS.inc()
    enriched_count = 0

    for lead in leads:
        enriched_lead = enrich_for_retrofeed(lead)
        enriched_count += 1
        redis.xadd(STREAM, {"payload": json.dumps(enriched_lead)})

    RETROFEED_ENRICHED.inc(enriched_count)
    logger.info(f"♻️ Retrofeed: {enriched_count} leads reprocessados e enriquecidos")

    return {"status": "requeued", "count": enriched_count}