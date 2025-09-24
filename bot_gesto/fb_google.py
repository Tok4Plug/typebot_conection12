# fb_google.py — versão 2.0 avançada (sincronizado com bot/bridge/utils)
import os, aiohttp, asyncio, json, logging
from typing import Dict, Any
from utils import build_fb_payload, build_ga4_payload

# =========================
# Configurações de ENV
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
FB_PIXEL_ID = os.getenv("FB_PIXEL_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")

GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
GOOGLE_ENABLED = bool(GA4_MEASUREMENT_ID and GA4_API_SECRET)

FB_RETRY_MAX = int(os.getenv("FB_RETRY_MAX", "3"))
QUEUE_WORKERS = int(os.getenv("EVENT_QUEUE_WORKERS", "3"))

logger = logging.getLogger("fb_google")
logger.setLevel(logging.INFO)

# =========================
# Helper de retry
# =========================
async def post_with_retry(session, url: str, payload: Dict[str, Any], retries: int = 3,
                          platform: str = "fb", et: str = None) -> Dict[str, Any]:
    """
    Envia request POST com retry exponencial progressivo.
    Retorna {ok, status, body, platform, event}.
    """
    last_err = None
    for i in range(retries):
        try:
            async with session.post(url, json=payload, timeout=20) as resp:
                txt = await resp.text()
                if resp.status in (200, 201, 204):
                    return {"ok": True, "status": resp.status, "body": txt,
                            "platform": platform, "event": et}
                else:
                    last_err = f"{resp.status}: {txt}"
        except Exception as e:
            last_err = str(e)
        await asyncio.sleep(2 * (i + 1))
    return {"ok": False, "error": last_err, "platform": platform, "event": et}

# =========================
# Envio para Facebook CAPI
# =========================
async def send_event_fb(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    if not FB_PIXEL_ID or not FB_ACCESS_TOKEN:
        return {"skip": True, "reason": "fb creds missing"}

    payload = build_fb_payload(FB_PIXEL_ID, event_name, lead)
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{FB_PIXEL_ID}/events?access_token={FB_ACCESS_TOKEN}"

    async with aiohttp.ClientSession() as session:
        res = await post_with_retry(session, url, payload,
                                    retries=FB_RETRY_MAX, platform="facebook", et=event_name)
        logger.info(json.dumps({
            "event": "FB_SEND",
            "event_type": event_name,
            "telegram_id": lead.get("telegram_id"),
            "status": res.get("status"),
            "ok": res.get("ok"),
            "error": res.get("error")
        }))
        return res

# =========================
# Envio para Google GA4
# =========================
async def send_event_google(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    if not GOOGLE_ENABLED:
        return {"skip": True, "reason": "google disabled"}

    payload = build_ga4_payload(event_name, lead)
    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"

    async with aiohttp.ClientSession() as session:
        res = await post_with_retry(session, url, payload,
                                    retries=3, platform="ga4", et=event_name)
        logger.info(json.dumps({
            "event": "GA4_SEND",
            "event_type": event_name,
            "telegram_id": lead.get("telegram_id"),
            "status": res.get("status"),
            "ok": res.get("ok"),
            "error": res.get("error")
        }))
        return res

# =========================
# Função principal unificada
# =========================
async def send_event_to_all(lead: Dict[str, Any], et: str = "Lead") -> Dict[str, Any]:
    """
    Dispara evento (Lead/Subscribe) para:
      - Facebook (sempre)
      - Google GA4 (se configurado)
    """
    results: Dict[str, Any] = {"facebook": await send_event_fb(et, lead)}
    if GOOGLE_ENABLED:
        results["google"] = await send_event_google(et, lead)

    logger.info(json.dumps({
        "event": "SEND_EVENT_TO_ALL",
        "event_type": et,
        "telegram_id": lead.get("telegram_id"),
        "results": results
    }))
    return results

# =========================
# Retry wrapper (chamado pelo bot.py)
# =========================
async def send_event_with_retry(event_type: str, lead: Dict[str, Any],
                                retries: int = 5, delay: float = 2.0) -> Dict[str, Any]:
    attempt = 0
    while attempt < retries:
        try:
            results = await send_event_to_all(lead, et=event_type)
            ok = any(r.get("ok") for r in results.values() if isinstance(r, dict))
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
        await asyncio.sleep(delay ** attempt)

    logger.error(json.dumps({
        "event": "SEND_EVENT_FAILED",
        "type": event_type,
        "telegram_id": lead.get("telegram_id"),
    }))
    return {"status": "failed", "event": event_type}

# =========================
# Queue de eventos (assíncrona)
# =========================
_event_queue: asyncio.Queue = asyncio.Queue()

async def enqueue_event(event_type: str, lead: Dict[str, Any]) -> None:
    """Coloca evento na fila para envio posterior (worker processa)."""
    await _event_queue.put((event_type, lead))
    logger.info(json.dumps({
        "event": "QUEUE_ENQ",
        "event_type": event_type,
        "telegram_id": lead.get("telegram_id")
    }))

async def _worker(worker_id: int):
    """Worker que consome eventos da fila e envia para os pixels."""
    while True:
        event_type, lead = await _event_queue.get()
        try:
            await send_event_with_retry(event_type, lead)
        except Exception as e:
            logger.error(json.dumps({
                "event": "QUEUE_WORKER_ERROR",
                "worker": worker_id,
                "event_type": event_type,
                "telegram_id": lead.get("telegram_id"),
                "error": str(e)
            }))
        finally:
            _event_queue.task_done()

async def process_event_queue():
    """
    Inicia múltiplos workers que processam a fila continuamente.
    """
    tasks = [asyncio.create_task(_worker(i)) for i in range(QUEUE_WORKERS)]
    await asyncio.gather(*tasks)