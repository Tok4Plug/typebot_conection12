# fb_google.py — v3.0 (Padrão A: envio direto, sem fila) — robusto e alinhado ao bridge/bot/utils
import os, aiohttp, asyncio, json, logging, random
from typing import Dict, Any, Optional

# ============================
# Imports utilitários (compat)
# ============================
try:
    # se estiver como pacote
    from .utils import build_fb_payload, build_ga4_payload
except Exception:  # fallback quando rodando solto
    import sys
    sys.path.append(os.path.dirname(__file__))
    from utils import build_fb_payload, build_ga4_payload  # type: ignore

# ============================
# Configurações de ENV
# ============================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
FB_PIXEL_ID = os.getenv("FB_PIXEL_ID", "")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
FB_TEST_EVENT_CODE = os.getenv("FB_TEST_EVENT_CODE", "").strip()  # opcional (modo teste Pixel)

# Subscribe automático disparado a partir de um Lead (apenas aqui no módulo)
FB_AUTO_SUBSCRIBE_FROM_LEAD = os.getenv("FB_AUTO_SUBSCRIBE_FROM_LEAD", "1") == "1"

# GA4
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
GOOGLE_ENABLED = bool(GA4_MEASUREMENT_ID and GA4_API_SECRET)

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
def _mask_token(t: str) -> str:
    if not t:
        return ""
    return t[:4] + "***" + t[-4:] if len(t) > 8 else "***"

def _build_fb_url() -> str:
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{FB_PIXEL_ID}/events?access_token={FB_ACCESS_TOKEN}"
    if FB_TEST_EVENT_CODE:
        return base + f"&test_event_code={FB_TEST_EVENT_CODE}"
    return base

def _ensure_user_data(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Garante que user_data exista e contenha os campos mínimos esperados
    pelo normalizador do utils (fbp/fbc/ip/ua), sem sobrescrever se já existem.
    """
    ud: Dict[str, Any] = dict(lead.get("user_data") or {})

    # fbp/fbc: prioriza já existentes; senão, mapeia de _fbp/_fbc do bridge
    if "fbp" not in ud and lead.get("_fbp"):
        ud["fbp"] = lead["_fbp"]
    if "fbc" not in ud and lead.get("_fbc"):
        ud["fbc"] = lead["_fbc"]

    # IP / UA: se user_data não tem, tenta do topo
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
    - event_source_url: fallback para landing_url
    - user_data: garante fbp/fbc/ip/ua
    - previne reentrância do auto-subscribe
    """
    out = dict(lead or {})
    # fonte do evento
    if not out.get("event_source_url"):
        out["event_source_url"] = out.get("landing_url") or out.get("src_url")

    # normaliza user_data mínimo
    out["user_data"] = _ensure_user_data(out)

    # flag interna para evitar loop de auto-subscribe
    out.setdefault("__suppress_auto_subscribe", False)
    return out

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
                    last_err = f"{resp.status}: {text}"
            except Exception as e:
                last_err = str(e)

            # backoff exponencial com jitter
            await asyncio.sleep((2 ** attempt) + random.random() * 0.5)

    # log final de erro (opcional: payload)
    if FB_LOG_PAYLOAD_ON_ERROR:
        logger.warning(json.dumps({
            "event": "POST_RETRY_FAILED",
            "platform": platform,
            "event_type": et,
            "url": url.split("?")[0],
            "status_or_error": last_err,
            "payload": payload,  # cuidado: não tem token, ele está na query
        }))
    else:
        logger.warning(json.dumps({
            "event": "POST_RETRY_FAILED",
            "platform": platform,
            "event_type": et,
            "url": url.split("?")[0],
            "status_or_error": last_err,
        }))

    return {"ok": False, "error": last_err, "platform": platform, "event": et}

# ============================
# Envio para Facebook CAPI
# ============================
async def send_event_fb(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dispara evento para o Facebook CAPI.
    """
    if not FB_PIXEL_ID or not FB_ACCESS_TOKEN:
        return {"skip": True, "reason": "fb creds missing"}

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

    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"
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

    # Subscribe automático (somente a partir de LEAD e somente aqui)
    if et.lower() == "lead" and FB_AUTO_SUBSCRIBE_FROM_LEAD and not lead.get("__suppress_auto_subscribe", False):
        # evita reentrância se alguém reaproveitar este lead
        clone = dict(lead)
        clone["subscribe_from_lead"] = True
        clone["__suppress_auto_subscribe"] = True
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
    Wrapper com retry exponencial; não duplica envios.
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