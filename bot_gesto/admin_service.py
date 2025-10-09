# ==========================================
# bot_gesto/admin_service.py — v4.0 avançado
# ==========================================
# - NÃO cria _fbp/_fbc, IP ou UA: só repassa o que existe (mantém score alto).
# - Dedupe por event_id (Redis SETNX+TTL + fallback em memória).
# - Retrofeed por API:
#     • mode=direct  -> envia aos pixels (FB/GA4) com concorrência controlada.
#     • mode=stream  -> reempilha no Redis Stream (compat com workers).
# - Métricas Prometheus; endpoints /health, /metrics, /stats, /unsent, /retrofeed.
# - Nenhuma ENV nova obrigatória (apenas opcionais com defaults seguros).

import os, json, time, logging, asyncio, random
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Header, Query, Body
from fastapi.responses import PlainTextResponse, JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge, Histogram
from redis import Redis

from bot_gesto.db import init_db, get_unsent_leads, save_lead
from bot_gesto.utils import clamp_event_time, get_or_build_event_id
from bot_gesto.fb_google import send_event_to_all

# ==============================
# Configurações (sem novas ENVs)
# ==============================
APP_VERSION = "4.0.0"

ADMIN_TOKEN        = os.getenv("ADMIN_TOKEN", "")
REDIS_URL          = os.getenv("REDIS_URL")
STREAM             = os.getenv("REDIS_STREAM", "buyers_stream")

# Retrofeed defaults (podem ser sobrescritos por query params no /retrofeed)
RETRO_DEFAULT_MODE        = (os.getenv("RETROFEED_MODE", "direct") or "direct").lower()  # direct|stream
RETRO_DEFAULT_LIMIT       = int(os.getenv("RETROFEED_BATCH", "200"))
RETRO_DEFAULT_CONCURRENCY = int(os.getenv("RETROFEED_CONCURRENCY", "8"))
RETRO_DEDUPE_TTL          = int(os.getenv("RETROFEED_DEDUPE_TTL", "86400"))  # 24h

# Redis (opcional)
redis: Optional[Redis] = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# ==============================
# FastAPI app + Logger
# ==============================
app = FastAPI(title="Admin Service", version=APP_VERSION)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [admin] %(message)s")
logger = logging.getLogger("admin_service")

# ==============================
# Métricas Prometheus
# ==============================
PENDING_GAUGE = Gauge("leads_pending", "Leads pendentes no DB")

RETRO_RUNS      = Counter("retro_runs_total", "Execuções do retrofeed", ["mode"])
RETRO_PROCESSED = Counter("retro_processed_total", "Leads processados pelo retrofeed", ["mode"])
RETRO_SENT_OK   = Counter("retro_sent_ok_total", "Leads enviados com sucesso (modo=direct)")
RETRO_SENT_FAIL = Counter("retro_sent_fail_total", "Leads falhos no envio (modo=direct)")
RETRO_PUBLISHED = Counter("retro_published_total", "Leads publicados no Redis Stream (modo=stream)")
RETRO_SKIPPED   = Counter("retro_skipped_total", "Leads pulados por dedupe")
RETRO_LATENCY   = Histogram("retro_latency_seconds", "Latência do retrofeed (s)")

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
# Helpers
# ==============================
_mem_dedupe: Dict[str, float] = {}  # {event_id: exp_ts}

def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

def _jitter(base: float = 1.0) -> float:
    return base + random.random() * 0.25

def _dedupe_mark(event_id: str, ttl: int = RETRO_DEDUPE_TTL) -> bool:
    """
    Marca event_id como visto.
    True  => marcou agora (pode processar)
    False => já visto (pular)
    """
    if not event_id:
        return True

    if redis:
        try:
            if redis.set(f"retro:dedupe:{event_id}", "1", nx=True, ex=ttl):
                return True
            return False
        except Exception:
            pass

    now = time.time()
    # clear expirados
    expired = [k for k, exp in _mem_dedupe.items() if exp <= now]
    for k in expired:
        _mem_dedupe.pop(k, None)

    if event_id in _mem_dedupe and _mem_dedupe[event_id] > now:
        return False
    _mem_dedupe[event_id] = now + ttl
    return True

def _coalesce_ids(lead: Dict[str, Any]) -> None:
    """
    Espelha telegram_id/external_id no topo e em user_data (sem inventar).
    """
    ud = lead.get("user_data") or {}
    tg = str(lead.get("telegram_id") or ud.get("telegram_id") or "")
    if tg:
        lead["telegram_id"] = tg
        ud.setdefault("telegram_id", tg)
        # external_id: preserva o existente, senão cai para telegram_id
        ud.setdefault("external_id", ud.get("external_id") or tg)
        lead["user_data"] = ud

def _ensure_core(lead: Dict[str, Any], default_event: str = "Lead") -> Dict[str, Any]:
    """
    Garante apenas o essencial: event_type, event_time clamp, event_id determinístico e IDs coerentes.
    NÃO cria _fbp/_fbc/IP/UA (mantém política 4.0).
    """
    out = dict(lead or {})

    # event_type canônico
    et = (out.get("event_type") or default_event or "Lead").strip().lower()
    out["event_type"] = "Subscribe" if et == "subscribe" else "Lead"

    # event_time dentro da janela CAPI
    ts = int(out.get("event_time") or time.time())
    out["event_time"] = clamp_event_time(ts)

    # IDs coerentes
    _coalesce_ids(out)

    # event_id estável (respeita existente)
    out["event_id"] = get_or_build_event_id(out["event_type"], out, out["event_time"])
    return out

async def _send_one_direct(item: Dict[str, Any]) -> Tuple[str, bool, Optional[str]]:
    """
    Envia 1 lead aos pixels (idempotente a jusante). Retorna (event_id, ok, err).
    """
    ev_id = str(item.get("event_id") or "")
    try:
        results = await send_event_to_all(item, et=item.get("event_type") or "Lead")
        ok = any(isinstance(v, dict) and v.get("ok") for v in (results or {}).values())
        return ev_id, ok, None if ok else "no_ok_platform"
    except Exception as e:
        return ev_id, False, str(e)

async def _send_batch_direct(items: List[Dict[str, Any]], concurrency: int) -> Tuple[int, int]:
    sem = asyncio.Semaphore(max(1, concurrency))
    ok_count = 0
    fail_count = 0

    async def _worker(payload: Dict[str, Any]):
        nonlocal ok_count, fail_count
        async with sem:
            ev_id, ok, err = await _send_one_direct(payload)
            try:
                # registra status no histórico (idempotente)
                await save_lead(payload, event_record={"status": "success" if ok else "failed"})
            except Exception as e:
                logger.warning(_safe_json({"event": "ADMIN_SAVE_STATUS_WARN", "event_id": ev_id, "error": str(e)}))
            if ok:
                ok_count += 1
                RETRO_SENT_OK.inc()
            else:
                fail_count += 1
                RETRO_SENT_FAIL.inc()
                logger.warning(_safe_json({"event": "ADMIN_DIRECT_FAIL", "event_id": ev_id, "reason": err or "unknown"}))

    await asyncio.gather(*[_worker(i) for i in items])
    return ok_count, fail_count

async def _publish_stream(items: List[Dict[str, Any]]) -> int:
    if not redis:
        raise HTTPException(status_code=500, detail="Redis não configurado")

    if not items:
        return 0

    pipe = redis.pipeline()
    for it in items:
        pipe.xadd(
            STREAM,
            {
                "payload": _safe_json(it),
                "event_id": it.get("event_id"),
                "type": it.get("event_type") or "Lead",
            }
        )
    pipe.execute()
    RETRO_PUBLISHED.inc(len(items))
    return len(items)

# ==============================
# Lifecycle
# ==============================
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Iniciando Admin Service...")
    init_db()
    if redis:
        try:
            redis.ping()
            logger.info("✅ Redis OK")
        except Exception as e:
            logger.warning(f"⚠️ Redis indisponível: {e}")
    logger.info("✅ DB inicializado")

# ==============================
# Endpoints
# ==============================
@app.get("/health")
async def health():
    rstatus = "disabled"
    if redis:
        try:
            redis.ping()
            rstatus = "ok"
        except Exception as e:
            rstatus = f"error:{e}"
    return {
        "status": "alive",
        "version": APP_VERSION,
        "redis": rstatus,
        "retro_defaults": {
            "mode": RETRO_DEFAULT_MODE,
            "limit": RETRO_DEFAULT_LIMIT,
            "concurrency": RETRO_DEFAULT_CONCURRENCY,
            "dedupe_ttl": RETRO_DEDUPE_TTL,
            "stream": STREAM,
        }
    }

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/stats")
async def stats(
    token: str = Query(default="", description="Admin token"),
    authorization: str = Header(default="")
):
    _require_token(token, authorization)
    leads = await get_unsent_leads(limit=1000)
    PENDING_GAUGE.set(len(leads))
    oldest_ts = min([int(l.get("event_time") or time.time()) for l in leads], default=None)
    return {
        "pending": len(leads),
        "oldest_pending_age_sec": (int(time.time()) - oldest_ts) if oldest_ts else 0
    }

@app.get("/unsent")
async def unsent(
    token: str = Query(default="", description="Admin token"),
    authorization: str = Header(default=""),
    limit: int = Query(default=50, ge=1, le=1000)
):
    _require_token(token, authorization)
    rows = await get_unsent_leads(limit=limit)
    # retorno enxuto (sem PII desnecessária)
    out = [{
        "event_id": r.get("event_id"),
        "event_type": r.get("event_type"),
        "event_time": r.get("event_time"),
        "telegram_id": r.get("telegram_id"),
        "src_url": r.get("src_url"),
        "utm_source": r.get("utm_source"),
        "gclid": r.get("gclid"),
        "fbclid": r.get("fbclid"),
        "sent_hint": False
    } for r in rows]
    return {"count": len(out), "items": out}

@app.post("/retrofeed")
async def retrofeed(
    token: str = Query(default="", description="Admin token"),
    authorization: str = Header(default=""),
    mode: str = Query(default=RETRO_DEFAULT_MODE, regex="^(direct|stream)$"),
    limit: int = Query(default=RETRO_DEFAULT_LIMIT, ge=1, le=5000),
    concurrency: int = Query(default=RETRO_DEFAULT_CONCURRENCY, ge=1, le=64),
    dry_run: bool = Query(default=False),
):
    """
    Reprocessa leads pendentes:
      - mode=direct  -> envia aos pixels (FB/GA4) com concorrência.
      - mode=stream  -> publica no Redis Stream para worker externo.
    Política 4.0: sem fabricar _fbp/_fbc/IP/UA — apenas normaliza time/ids/event_id.
    """
    _require_token(token, authorization)
    t0 = time.perf_counter()
    RETRO_RUNS.labels(mode=mode).inc()

    leads = await get_unsent_leads(limit=limit)
    if not leads:
        RETRO_LATENCY.observe(time.perf_counter() - t0)
        return {"status": "no_leads", "processed": 0}

    # pré-processamento + dedupe
    prepared: List[Dict[str, Any]] = []
    skipped = 0
    for lead in leads:
        l2 = _ensure_core(lead)
        evid = l2.get("event_id")
        if not _dedupe_mark(evid, ttl=RETRO_DEDUPE_TTL):
            skipped += 1
            continue
        prepared.append(l2)

    RETRO_PROCESSED.labels(mode=mode).inc(len(prepared))
    RETRO_SKIPPED.inc(skipped)

    if not prepared:
        RETRO_LATENCY.observe(time.perf_counter() - t0)
        return {"status": "skipped_all", "processed": 0, "skipped": skipped}

    # execução por modo
    if mode == "stream":
        if dry_run:
            for it in prepared:
                logger.info(_safe_json({"event": "ADMIN_DRY_RUN_XADD", "event_id": it.get("event_id")}))
            published = len(prepared)
        else:
            published = await _publish_stream(prepared)
        RETRO_LATENCY.observe(time.perf_counter() - t0)
        return {
            "status": "published",
            "published": published,
            "skipped": skipped,
            "stream": STREAM
        }

    # direct (envio aos pixels)
    if dry_run:
        for it in prepared:
            logger.info(_safe_json({"event": "ADMIN_DRY_RUN_DIRECT", "event_id": it.get("event_id")}))
        sent_ok, sent_fail = len(prepared), 0
    else:
        sent_ok, sent_fail = await _send_batch_direct(prepared, concurrency=concurrency)

    RETRO_LATENCY.observe(time.perf_counter() - t0)
    return {
        "status": "sent",
        "sent_ok": sent_ok,
        "sent_fail": sent_fail,
        "skipped": skipped
    }

# ------------------------------
# (Opcional) endpoint para enviar 1 lead “na mão”
# ------------------------------
@app.post("/send")
async def send_one(
    token: str = Query(default="", description="Admin token"),
    authorization: str = Header(default=""),
    lead: Dict[str, Any] = Body(..., description="Lead completo (payload compatível com db.get_unsent_leads())")
):
    """
    Envia um lead específico aos pixels. Útil para debug.
    Respeita política 4.0: não cria _fbp/_fbc/IP/UA (usa o que já veio).
    """
    _require_token(token, authorization)
    l2 = _ensure_core(lead)
    evid = l2.get("event_id")
    if not _dedupe_mark(evid, ttl=max(300, RETRO_DEDUPE_TTL // 8)):
        return JSONResponse({"status": "duplicate", "event_id": evid}, status_code=200)

    ok_id, ok, err = await _send_one_direct(l2)
    try:
        await save_lead(l2, event_record={"status": "success" if ok else "failed"})
    except Exception as e:
        logger.warning(_safe_json({"event": "ADMIN_SEND_SAVE_WARN", "event_id": evid, "error": str(e)}))

    if ok:
        return {"status": "sent", "event_id": ok_id}
    return JSONResponse({"status": "failed", "event_id": ok_id, "reason": err or "unknown"}, status_code=502)