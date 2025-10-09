# ======================================================
# retrofeed.py — v4.0 (payload completo + dedupe estável + modo direto/stream)
# - NÃO cria _fbp/_fbc, IP ou UA (só repassa o que veio do DB/Bridge/Bot).
# - Dedupe por event_id (get_or_build_event_id) com Redis SETNX+TTL + fallback em memória.
# - Dois modos compatíveis:
#     • RETROFEED_MODE=direct  -> envia aos pixels (FB/GA4) usando fb_google (idempotente).
#     • RETROFEED_MODE=stream  -> reempilha no Redis Stream para worker externo.
# - Concurrency control (modo direto), retry com jitter, logs estruturados.
# - Nenhuma ENV nova obrigatória (as extras são opcionais).
# ======================================================

import os, json, asyncio, logging, time, random
from typing import Dict, Any, Optional, List, Tuple
from redis import Redis

from bot_gesto.db import init_db, get_unsent_leads, save_lead
from bot_gesto.utils import clamp_event_time, get_or_build_event_id
from bot_gesto.fb_google import send_event_to_all

# =============================
# Configurações (sem novas ENVs obrigatórias)
# =============================
REDIS_URL       = os.getenv("REDIS_URL")
STREAM          = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE      = int(os.getenv("RETROFEED_BATCH", "200"))
RETRY_MAX       = int(os.getenv("RETROFEED_RETRY_MAX", "3"))
LOOP_INTERVAL   = int(os.getenv("RETROFEED_LOOP_INTERVAL", "300"))     # seg
DEDUPE_TTL      = int(os.getenv("RETROFEED_DEDUPE_TTL", "86400"))      # 24h
DRY_RUN         = os.getenv("RETROFEED_DRY_RUN", "0") == "1"

# Modo de operação (opcional): 'direct' (padrão) ou 'stream'
RETROFEED_MODE  = (os.getenv("RETROFEED_MODE", "direct") or "direct").lower()
# Concurrency no modo 'direct'
RETROFEED_CONCURRENCY = int(os.getenv("RETROFEED_CONCURRENCY", "8"))

# Redis opcional (necessário apenas para dedupe/pipeline)
redis: Optional[Redis] = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# =============================
# Logger padronizado
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [retrofeed] %(message)s"
)
logger = logging.getLogger("retrofeed")

# =============================
# Fallback de dedupe em memória
# =============================
_mem_seen: Dict[str, float] = {}  # {event_id: exp_ts}

# =============================
# Helpers
# =============================
def _jitter(base: float = 1.0) -> float:
    # leve jitter anti-thundering herd
    return base + random.random() * 0.25

def _dedupe_key(event_id: str) -> str:
    return f"retrofeed:seen:{event_id}"

def _safe_dumps(obj: Any) -> str:
    # serialização tolerante (default=str evita travar por tipos não-JSON)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

def _coalesce_ids(lead: Dict[str, Any]) -> None:
    """
    Garante telegram_id/external_id no topo e em user_data (espelhando o que já existe).
    NÃO inventa dados além do espelhamento simples.
    """
    ud = lead.get("user_data") or {}
    tg = str(lead.get("telegram_id") or ud.get("telegram_id") or "")
    if tg:
        lead["telegram_id"] = tg
        ud.setdefault("telegram_id", tg)
        # external_id: preserva o existente; se não houver, cai para telegram_id
        ud.setdefault("external_id", ud.get("external_id") or tg)
        lead["user_data"] = ud

def _norm_event_type(et: Optional[str]) -> str:
    e = (et or "Lead").strip().lower()
    return "Subscribe" if e == "subscribe" else "Lead"

def _ensure_core_fields(lead: Dict[str, Any], default_event: str = "Lead") -> Dict[str, Any]:
    """
    Ajusta apenas o essencial para dedupe e janela temporal.
    NÃO cria _fbp/_fbc e NÃO inventa IP/UA (para não derrubar score do pixel).
    """
    l = dict(lead or {})  # cópia defensiva

    # tipo de evento canônico
    l["event_type"] = _norm_event_type(l.get("event_type") or default_event)

    # event_time clamp (janela do CAPI)
    ts = int(l.get("event_time") or time.time())
    l["event_time"] = clamp_event_time(ts)

    # IDs coerentes
    _coalesce_ids(l)

    # event_id estável; se já veio, reaproveita; caso contrário gera determinístico
    l["event_id"] = get_or_build_event_id(l["event_type"], l, l["event_time"])
    return l

def _already_enqueued(event_id: str) -> bool:
    """
    Dedupe simples via Redis SETNX+TTL + fallback memória.
    True => já visto recentemente (pular).
    False => ainda não visto (pode processar).
    """
    if not event_id:
        return False

    # Redis
    if redis:
        try:
            key = _dedupe_key(event_id)
            if redis.setnx(key, "1"):
                redis.expire(key, DEDUPE_TTL)
                return False
            return True
        except Exception:
            pass  # fallback memória

    # Memória (TTL simples)
    now = time.time()
    # limpeza rápida
    expired = [k for k, exp in _mem_seen.items() if exp <= now]
    for k in expired:
        _mem_seen.pop(k, None)

    if event_id in _mem_seen and _mem_seen[event_id] > now:
        return True
    _mem_seen[event_id] = now + DEDUPE_TTL
    return False

async def _publish_stream(to_enqueue: List[Dict[str, Any]]) -> int:
    """
    Publica lote no Redis Stream (compat com workers).
    """
    if not redis:
        logger.error("[RETROFEED_ERROR] Redis não configurado (REDIS_URL ausente).")
        return 0

    if DRY_RUN:
        for item in to_enqueue:
            logger.info(_safe_dumps({
                "event": "RETROFEED_DRY_RUN_XADD",
                "telegram_id": item.get("telegram_id"),
                "event_id": item.get("event_id"),
                "type": item.get("event_type") or "Lead",
            }))
        return len(to_enqueue)

    retries = RETRY_MAX
    while retries > 0:
        try:
            pipe = redis.pipeline()
            for item in to_enqueue:
                pipe.xadd(
                    STREAM,
                    {
                        "payload": _safe_dumps(item),  # compat com worker atual
                        "event_id": item.get("event_id"),
                        "type": item.get("event_type") or "Lead",
                    }
                )
            pipe.execute()
            return len(to_enqueue)
        except Exception as e:
            retries -= 1
            if retries <= 0:
                logger.error(f"[RETROFEED_FAIL] Falha final ao publicar lote: {e}")
                break
            backoff = _jitter(1.0) * (RETRY_MAX - retries + 1)
            logger.warning(f"[RETROFEED_RETRY] Falha ao publicar lote (restam={retries}) err={e} backoff={backoff:.2f}s")
            await asyncio.sleep(backoff)
    return 0

async def _send_one_direct(lead: Dict[str, Any]) -> Tuple[str, bool, Optional[str]]:
    """
    Envia um lead diretamente para os pixels (FB/GA4) usando fb_google (que já é idempotente).
    Retorna (event_id, ok, erro_str).
    """
    ev_id = str(lead.get("event_id") or "")
    try:
        results = await send_event_to_all(lead, et=lead.get("event_type") or "Lead")
        ok = any(isinstance(v, dict) and v.get("ok") for v in (results or {}).values())
        return ev_id, ok, None if ok else "no_ok_platform"
    except Exception as e:
        return ev_id, False, str(e)

async def _send_direct(to_send: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Envia com concorrência limitada. Atualiza DB (save_lead) marcando sucesso/falha.
    Retorna (sent_ok, sent_fail).
    """
    sem = asyncio.Semaphore(max(1, RETROFEED_CONCURRENCY))
    sent_ok = 0
    sent_fail = 0

    async def _worker(item: Dict[str, Any]):
        nonlocal sent_ok, sent_fail
        async with sem:
            ev_id, ok, err = await _send_one_direct(item)
            # Persistência de status no histórico (idempotente)
            try:
                await save_lead(item, event_record={"status": "success" if ok else "failed"})
            except Exception as e:
                logger.warning(f"[RETROFEED_SAVE_STATUS_WARN] ev={ev_id} err={e}")
            if ok:
                sent_ok += 1
            else:
                sent_fail += 1
                logger.warning(_safe_dumps({
                    "event": "RETROFEED_DIRECT_FAIL",
                    "event_id": ev_id,
                    "reason": err or "unknown"
                }))

    await asyncio.gather(*[_worker(i) for i in to_send])
    return sent_ok, sent_fail

# =============================
# Função principal (1 passagem)
# =============================
async def retrofeed(batch_size: int = BATCH_SIZE) -> int:
    """
    Busca leads não enviados do DB (payload completo via db v3.3+),
    aplica garantias mínimas (event_time/event_id), dedupe,
    e processa conforme o modo:
      - 'direct': envia aos pixels (FB/GA4) via fb_google.
      - 'stream': reempilha no Redis Stream para worker externo.
    """
    init_db()
    leads = await get_unsent_leads(limit=batch_size)  # já retorna LEAD completo (com event_id se disponível)

    if not leads:
        logger.info(_safe_dumps({"event": "RETROFEED_EMPTY"}))
        return 0

    prepared: List[Dict[str, Any]] = []
    skipped = 0

    for lead in leads:
        enriched = _ensure_core_fields(lead, default_event="Lead")
        ev_id = enriched.get("event_id")

        if _already_enqueued(ev_id):
            skipped += 1
            logger.info(_safe_dumps({"event": "RETROFEED_SKIP_DEDUPE", "event_id": ev_id}))
            continue

        prepared.append(enriched)

    if not prepared:
        logger.info(_safe_dumps({"event": "RETROFEED_DONE", "processed": 0, "skipped": skipped, "mode": RETROFEED_MODE}))
        return 0

    if RETROFEED_MODE == "stream":
        published = await _publish_stream(prepared)
        logger.info(_safe_dumps({
            "event": "RETROFEED_STREAM_DONE",
            "published": published,
            "skipped": skipped,
            "stream": STREAM
        }))
        return published

    # Modo 'direct' (padrão)
    if DRY_RUN:
        for item in prepared:
            logger.info(_safe_dumps({
                "event": "RETROFEED_DRY_RUN_DIRECT",
                "telegram_id": item.get("telegram_id"),
                "event_id": item.get("event_id"),
                "type": item.get("event_type") or "Lead",
            }))
        sent_ok = len(prepared)
        sent_fail = 0
    else:
        sent_ok, sent_fail = await _send_direct(prepared)

    logger.info(_safe_dumps({
        "event": "RETROFEED_DIRECT_DONE",
        "sent_ok": sent_ok,
        "sent_fail": sent_fail,
        "skipped": skipped
    }))
    return sent_ok

# =============================
# Loop contínuo (opcional)
# =============================
async def retrofeed_loop(interval: int = LOOP_INTERVAL):
    """
    Executa retrofeed em loop contínuo a cada X segundos.
    Ideal para processos em background (supervisor/container).
    """
    logger.info(_safe_dumps({
        "event": "RETROFEED_LOOP_START",
        "interval_sec": interval,
        "mode": RETROFEED_MODE,
        "dry_run": DRY_RUN,
        "concurrency": RETROFEED_CONCURRENCY
    }))
    while True:
        try:
            await retrofeed(batch_size=BATCH_SIZE)
        except Exception as e:
            logger.error(_safe_dumps({"event": "RETROFEED_LOOP_ERROR", "error": str(e)}))
        await asyncio.sleep(interval)

# =============================
# Execução standalone
# =============================
if __name__ == "__main__":
    try:
        asyncio.run(retrofeed(batch_size=BATCH_SIZE))
    except KeyboardInterrupt:
        logger.warning("[RETROFEED_STOPPED] Retrofeed encerrado manualmente.")