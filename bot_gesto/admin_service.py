# ==========================================
# bot_gesto/admin_service.py — v1.3 avançado
# ==========================================
import os, json, time, logging
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, Histogram
from redis import Redis

from bot_gesto.db import init_db, get_unsent_leads
from bot_gesto.utils import clamp_event_time, build_event_id

# ==============================
# Configurações (sem novas ENVs)
# ==============================
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

# Redis (opcional)
redis: Optional[Redis] = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# FastAPI app
APP_VERSION = "1.3.0"
app = FastAPI(title="Admin Service", version=APP_VERSION)

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [admin] %(message)s")
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
RETROFEED_REQUEUED = Counter("retrofeed_requeued_total", "Leads re-enfileirados no Redis")
RETROFEED_SKIPPED = Counter("retrofeed_skipped_total", "Leads pulados por dedupe no retrofeed")
RETROFEED_LATENCY = Histogram("retrofeed_latency_seconds", "Latência da execução do retrofeed (s)")

# ==============================
# Auth simples (token direto ou Bearer)
# ==============================
def _require_token(token: str = "", authorization: str = "") -> None:
    """
    Aceita ?token=... OU Authorization: Bearer <token>.
    Não loga o token; erros retornam 403.
    """
    supplied = token or (authorization.split(" ")[1] if authorization.startswith("Bearer ") else "")
    if ADMIN_TOKEN and supplied != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido")

# ==============================
# Enriquecimento p/ retrofeed
# ==============================
def _enrich_for_retrofeed(lead: Dict[str, Any], default_event: str = "Lead") -> Dict[str, Any]:
    """
    Garante campos mínimos para reprocessar no pixel.
    Não inventa dados sensíveis, só complementa o essencial.
    """
    out = dict(lead or {})

    # event_time dentro da janela aceita pelo pixel
    ts = int(out.get("event_time") or time.time())
    out["event_time"] = clamp_event_time(ts)

    # IDs necessários para rastreio/dedupe
    tg_id = str(out.get("telegram_id") or "")
    out["telegram_id"] = tg_id
    out["external_id"] = out.get("external_id") or tg_id

    # Cookies essenciais (quando ausentes)
    now = int(time.time())
    if not out.get("_fbp") and tg_id:
        out["_fbp"] = f"fb.1.{now}.{tg_id}"
    if not out.get("_fbc") and tg_id:
        # se já temos fbclid no custom/lead, o pixel deduzirá; aqui criamos um marcador estável
        out["_fbc"] = f"fb.1.{now}.retro.{tg_id}"

    # event_id determinístico (dedupe)
    out["event_id"] = build_event_id(default_event, out, out["event_time"])
    return out

# ==============================
# Dedupe leve no Redis (interno)
# ==============================
# Sem novas ENVs: TTL fixo razoável (1h) para evitar reenfileirar o mesmo event_id repetidamente
_RETRO_DEDUPE_TTL = 3600  # 1 hora

def _dedupe_mark(event_id: str) -> bool:
    """
    Marca o event_id no Redis por TTL. Se já existir, retorna False (duplicado).
    Sem Redis disponível, sempre retorna True (não bloqueia operação).
    """
    if not redis:
        return True
    try:
        return bool(redis.set(f"retro:dedupe:{event_id}", "1", nx=True, ex=_RETRO_DEDUPE_TTL))
    except Exception:
        return True

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
    redis_status = "disabled"
    if redis:
        try:
            redis.ping()
            redis_status = "ok"
        except Exception as e:
            redis_status = f"erro: {e}"
    return {"status": "alive", "version": APP_VERSION, "redis": redis_status}

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/stats")
async def stats(token: str = "", authorization: str = Header(default="")):
    _require_token(token, authorization)
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
    Retroalimenta leads NÃO enviados de volta ao Redis:
      - Enriquecimento mínimo (event_time/event_id/_fbp/_fbc/external_id/telegram_id)
      - Idempotência leve por event_id (TTL interno no Redis)
      - Uso de pipeline para XADD em lote
    """
    _require_token(token, authorization)
    if not redis:
        raise HTTPException(status_code=500, detail="Redis não configurado")

    start = time.perf_counter()
    try:
        leads = await get_unsent_leads(limit=50)
        if not leads:
            logger.info("♻️ Nenhum lead pendente para retrofeed")
            RETROFEED_RUNS.inc()
            RETROFEED_LATENCY.observe(time.perf_counter() - start)
            return {"status": "no_leads"}

        RETROFEED_RUNS.inc()

        requeued = 0
        skipped = 0
        enriched_list: List[Dict[str, Any]] = []

        for lead in leads:
            enriched = _enrich_for_retrofeed(lead)
            enriched_list.append(enriched)

        # métrica de enriquecimento (todos os elegíveis, antes do dedupe)
        RETROFEED_ENRICHED.inc(len(enriched_list))

        pipe = redis.pipeline()
        for item in enriched_list:
            ev_id = item.get("event_id")
            if ev_id and not _dedupe_mark(ev_id):
                skipped += 1
                continue
            # enfileira no stream
            pipe.xadd(STREAM, {"payload": json.dumps(item)})
            requeued += 1

        # executa o pipeline (flush)
        pipe.execute()

        RETROFEED_REQUEUED.inc(requeued)
        RETROFEED_SKIPPED.inc(skipped)

        logger.info(f"♻️ Retrofeed concluído: requeued={requeued} skipped={skipped} total={len(enriched_list)}")
        RETROFEED_LATENCY.observe(time.perf_counter() - start)

        return {"status": "requeued", "count": requeued, "skipped": skipped, "total": len(enriched_list)}

    except HTTPException:
        # propaga HTTPException como está
        RETROFEED_LATENCY.observe(time.perf_counter() - start)
        raise
    except Exception as e:
        logger.error(f"❌ Erro em /retrofeed: {e}")
        RETROFEED_LATENCY.observe(time.perf_counter() - start)
        raise HTTPException(status_code=500, detail=str(e))