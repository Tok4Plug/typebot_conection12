# fb_google.py — v3.2.1
# (Padrão A: envio direto, sem fila; idempotência estável; logs claros)
# - Usa get_or_build_event_id no dedupe (evita duplicidade entre envio imediato e sync_pending)
# - Alinhado a utils v3.x (fbc a partir de fbclid, login_id/external_id, ZIP/CEP)
# - Idempotência: Redis (SETNX+TTL) com fallback em memória
# - Retry exponencial com jitter + timeouts
# - Subscribe automático opcional a partir de Lead
# - GA4 opcional (Measurement Protocol), com modo debug opcional
# - Logs estruturados sem vazar segredos

import os, aiohttp, asyncio, json, logging, random, time
from typing import Dict, Any, Optional

# ============================
# Imports utilitários (compat)
# ============================
try:
    # Pacote
    from .utils import (
        build_fb_payload,
        build_ga4_payload,
        clamp_event_time,
        build_event_id,            # compat
        get_or_build_event_id,     # <- USADO no dedupe estável
    )
except Exception:
    # Fallback rodando solto
    import sys
    sys.path.append(os.path.dirname(__file__))
    from utils import (  # type: ignore
        build_fb_payload,
        build_ga4_payload,
        clamp_event_time,
        build_event_id,            # compat
        get_or_build_event_id,     # <- USADO no dedupe estável
    )

# ============================
# Redis (opcional) para idempotência
# ============================
_redis, _redis_ok = None, False
REDIS_URL = os.getenv("REDIS_URL", "")
FB_DEDUP_REDIS = os.getenv("FB_DEDUP_REDIS", "1") == "1"   # usa Redis se houver
FB_DEDUP_MEM = os.getenv("FB_DEDUP_MEM", "1") == "1"       # fallback memória
FB_DEDUP_TTL_SEC = int(os.getenv("FB_DEDUP_TTL_SEC", "3600"))  # 1h
FB_DEDUP_PREFIX = os.getenv("FB_DEDUP_PREFIX", "capiev:ev:")

if REDIS_URL and FB_DEDUP_REDIS:
    try:
        from redis import Redis
        _redis = Redis.from_url(
            REDIS_URL,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
            decode_responses=True,
        )
        try:
            _redis_ok = bool(_redis.ping())
        except Exception:
            _redis_ok = False
    except Exception:
        _redis_ok = False

# Cache em memória: {event_key: expires_ts}
_mem_dedupe: Dict[str, float] = {}

# ============================
# Configurações de ENV
# ============================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
FB_PIXEL_ID = os.getenv("FB_PIXEL_ID", "")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
FB_TEST_EVENT_CODE = (os.getenv("FB_TEST_EVENT_CODE") or "").strip()  # opcional

# Subscribe automático disparado a partir de um Lead
FB_AUTO_SUBSCRIBE_FROM_LEAD = os.getenv("FB_AUTO_SUBSCRIBE_FROM_LEAD", "1") == "1"

# GA4
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
GOOGLE_ENABLED = bool(GA4_MEASUREMENT_ID and GA4_API_SECRET)
GA4_DEBUG = os.getenv("GA4_DEBUG", "0") == "1"  # se True, usa endpoint de debug

# Rede/Retry
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "20"))
FB_RETRY_MAX = int(os.getenv("FB_RETRY_MAX", "3"))
GA_RETRY_MAX = int(os.getenv("GA_RETRY_MAX", "3"))

# Logs
FB_LOG_PAYLOAD_ON_ERROR = os.getenv("FB_LOG_PAYLOAD_ON_ERROR", "0") == "1"

logger = logging.getLogger("fb_google")
logger.setLevel(logging.INFO)

# ============================
# Helpers
# ============================
def _build_fb_url() -> str:
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{FB_PIXEL_ID}/events?access_token={FB_ACCESS_TOKEN}"
    if FB_TEST_EVENT_CODE:
        return base + f"&test_event_code={FB_TEST_EVENT_CODE}"
    return base

def _build_ga4_url() -> str:
    base = "https://www.google-analytics.com"
    path = "/debug/mp/collect" if GA4_DEBUG else "/mp/collect"
    return f"{base}{path}?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"

def _ensure_user_data_min(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que user_data exista e contenha campos mínimos (fbp/fbc/ip/ua)
    sem sobrescrever o que já veio. O enriquecimento fino é feito em utils.
    """
    ud: Dict[str, Any] = dict(lead.get("user_data") or {})

    # fbp/fbc: prioriza já existentes; senão, usa _fbp/_fbc do bridge
    if "fbp" not in ud and lead.get("_fbp"):
        ud["fbp"] = lead["_fbp"]
    if "fbc" not in ud and lead.get("_fbc"):
        ud["fbc"] = lead["_fbc"]

    # IP / UA
    if "ip" not in ud and lead.get("ip"):
        ud["ip"] = lead["ip"]
    if "ua" not in ud:
        ua = lead.get("user_agent") or lead.get("ua")
        if ua:
            ud["ua"] = ua

    return ud

def _coerce_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ajustes finais antes do envio:
    - event_source_url: fallback para landing/src_url
    - user_data: garante fbp/fbc/ip/ua mínimos
    - flag interna anti-reentrância (auto-subscribe)
    """
    out = dict(lead or {})
    if not out.get("event_source_url"):
        out["event_source_url"] = out.get("landing_url") or out.get("src_url")
    out["user_data"] = _ensure_user_data_min(out)
    out.setdefault("__suppress_auto_subscribe", False)
    return out

def _event_dedupe_key(event_name: str, lead: Dict[str, Any]) -> str:
    """
    Chave determinística compatível com dedupe por event_id.
    Usa o event_id EXISTENTE quando presente (get_or_build_event_id),
    evitando divergência entre o envio imediato (bot) e o reenvio (sync_pending).
    """
    ts = clamp_event_time(int(lead.get("event_time") or time.time()))
    eid = get_or_build_event_id(event_name, lead, ts)  # <- aqui está a correção
    return f"{event_name.lower()}:{eid}"

def _dedupe_check_and_mark(event_name: str, lead: Dict[str, Any]) -> bool:
    """
    True => enviar (não visto recentemente); marca como visto.
    False => duplicado (pula envio).
    """
    key = _event_dedupe_key(event_name, lead)

    # Redis SETNX com TTL
    if _redis_ok:
        try:
            ok = _redis.set(FB_DEDUP_PREFIX + key, "1", nx=True, ex=FB_DEDUP_TTL_SEC)
            if ok:
                return True
            return False
        except Exception:
            pass  # fallback memória

    if not FB_DEDUP_MEM:
        return True  # sem memória e sem Redis: permite seguir

    # Memória (TTL simples)
    now = time.time()
    # limpeza rápida
    expired = [k for k, exp in _mem_dedupe.items() if exp <= now]
    for k in expired:
        _mem_dedupe.pop(k, None)

    if key in _mem_dedupe and _mem_dedupe[key] > now:
        return False
    _mem_dedupe[key] = now + FB_DEDUP_TTL_SEC
    return True

async def _post_with_retry(
    url: str,
    payload: Dict[str, Any],
    retries: int,
    platform: str,
    et: Optional[str],
) -> Dict[str, Any]:
    """
    POST com retry exponencial + jitter. Timeout configurável.
    Retorna: {ok, status, body|error, platform, event}
    """
    last_err = None
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(retries):
            try:
                async with session.post(url, json=payload) as resp:
                    text = await resp.text()
                    if resp.status in (200, 201, 204):
                        return {"ok": True, "status": resp.status, "body": text, "platform": platform, "event": et}
                    # tenta extrair erro estruturado para log melhor
                    try:
                        j = json.loads(text)
                        if isinstance(j, dict) and "error" in j:
                            last_err = f"{resp.status}: {j['error']}"
                        else:
                            last_err = f"{resp.status}: {text}"
                    except Exception:
                        last_err = f"{resp.status}: {text}"
            except Exception as e:
                last_err = str(e)

            # backoff exponencial com jitter
            await asyncio.sleep((2 ** attempt) + random.random() * 0.5)

    # log final de erro (sem vazar segredo)
    meta = {}
    try:
        d0 = payload.get("data", [{}])[0]
        meta = {
            "event_name": d0.get("event_name"),
            "event_time": d0.get("event_time"),
            "event_id": d0.get("event_id"),
            "action_source": d0.get("action_source"),
        }
    except Exception:
        pass

    log_body = {
        "event": "POST_RETRY_FAILED",
        "platform": platform,
        "event_type": et,
        "url": url.split("?")[0],
        "status_or_error": last_err,
        "payload_meta": meta,
    }
    if FB_LOG_PAYLOAD_ON_ERROR:
        log_body["payload"] = payload  # token está na query, não no body
    logger.warning(json.dumps(log_body))

    return {"ok": False, "error": last_err, "platform": platform, "event": et}

# ============================
# Envio para Facebook CAPI
# ============================
async def send_event_fb(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dispara evento para o Facebook CAPI (com idempotência).
    """
    if not FB_PIXEL_ID or not FB_ACCESS_TOKEN:
        return {"skip": True, "reason": "fb creds missing"}

    # Idempotência: evita duplicar reenvios acidentais
    if not _dedupe_check_and_mark(event_name, lead):
        logger.info(json.dumps({
            "event": "FB_DEDUP_SKIP",
            "event_type": event_name,
            "telegram_id": lead.get("telegram_id"),
        }))
        return {"ok": True, "status": 209, "body": "dedup_skip", "platform": "facebook", "event": event_name}

    url = _build_fb_url()
    coerced = _coerce_lead(lead)
    payload = build_fb_payload(FB_PIXEL_ID, event_name, coerced)

    res = await _post_with_retry(url, payload, retries=FB_RETRY_MAX, platform="facebook", et=event_name)
    logger.info(json.dumps({
        "event": "FB_SEND",
        "event_type": event_name,
        "telegram_id": coerced.get("telegram_id"),
        "status": res.get("status"),
        "ok": res.get("ok"),
        "error": res.get("error")
    }))
    return res

# ============================
# Envio para Google GA4
# ============================
async def send_event_google(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dispara evento para o Google Analytics 4 (Measurement Protocol).
    """
    if not GOOGLE_ENABLED:
        return {"skip": True, "reason": "google disabled"}

    url = _build_ga4_url()
    coerced = _coerce_lead(lead)
    payload = build_ga4_payload(event_name, coerced)

    res = await _post_with_retry(url, payload, retries=GA_RETRY_MAX, platform="ga4", et=event_name)
    logger.info(json.dumps({
        "event": "GA4_SEND",
        "event_type": event_name,
        "telegram_id": coerced.get("telegram_id"),
        "status": res.get("status"),
        "ok": res.get("ok"),
        "error": res.get("error")
    }))
    return res

# ============================
# Função principal unificada
# ============================
async def send_event_to_all(lead: Dict[str, Any], et: str = "Lead") -> Dict[str, Any]:
    """
    Dispara evento (Lead/Subscribe) para:
      - Facebook (sempre)
      - Google GA4 (se configurado)

    Padrão A: Subscribe automático é opcional e só acontece AQUI
    quando FB_AUTO_SUBSCRIBE_FROM_LEAD=1 e o evento for "Lead".
    """
    results: Dict[str, Any] = {}

    # Envio principal
    results["facebook"] = await send_event_fb(et, lead)
    if GOOGLE_ENABLED:
        results["google"] = await send_event_google(et, lead)

    # Subscribe automático (somente quando o evento principal é Lead)
    if et.lower() == "lead" and FB_AUTO_SUBSCRIBE_FROM_LEAD and not lead.get("__suppress_auto_subscribe", False):
        clone = dict(lead)
        clone["__suppress_auto_subscribe"] = True  # evita reentrância
        clone["subscribe_from_lead"] = True
        results["facebook_subscribe"] = await send_event_fb("Subscribe", clone)
        if GOOGLE_ENABLED:
            results["google_subscribe"] = await send_event_google("Subscribe", clone)

    logger.info(json.dumps({
        "event": "SEND_EVENT_TO_ALL",
        "event_type": et,
        "telegram_id": lead.get("telegram_id"),
        "results": results
    }))
    return results

# ============================
# Retry wrapper (compat com bot.py)
# ============================
async def send_event_with_retry(
    event_type: str,
    lead: Dict[str, Any],
    retries: int = 5,
    base_delay: float = 1.5
) -> Dict[str, Any]:
    """
    Wrapper com retry exponencial; não duplica envios (idempotência aplicada em send_event_fb).
    """
    attempt = 0
    while attempt < retries:
        try:
            results = await send_event_to_all(lead, et=event_type)
            ok = any(isinstance(v, dict) and v.get("ok") for v in results.values())
            if ok:
                return {"status": "success", "results": results}
        except Exception as e:
            logger.warning(json.dumps({
                "event": "SEND_EVENT_RETRY_ERROR",
                "type": event_type,
                "attempt": attempt + 1,
                "telegram_id": lead.get("telegram_id"),
                "error": str(e)
            }))
        attempt += 1
        await asyncio.sleep((base_delay ** attempt) + 0.2 * attempt)

    logger.error(json.dumps({
        "event": "SEND_EVENT_FAILED",
        "type": event_type,
        "telegram_id": lead.get("telegram_id"),
    }))
    return {"status": "failed", "event": event_type}

# ============================
# Alias de compatibilidade
# ============================
async def send_event(event_type: str, lead: dict):
    """
    Compat com worker/bot antigos: usa o retry wrapper.
    """
    return await send_event_with_retry(event_type, lead)