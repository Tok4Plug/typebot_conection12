# ======================================================
# retrofeed.py — v3.1 full (payload completo + dedupe + pipeline + robustez)
# ======================================================
import os, json, asyncio, logging, time, random
from typing import Dict, Any, Optional, List
from redis import Redis

from bot_gesto.db import init_db, get_unsent_leads
from bot_gesto.utils import clamp_event_time, build_event_id

# =============================
# Configurações (sem novas ENVs)
# =============================
REDIS_URL    = os.getenv("REDIS_URL")
STREAM       = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE   = int(os.getenv("RETROFEED_BATCH", "100"))
RETRY_MAX    = int(os.getenv("RETROFEED_RETRY_MAX", "3"))
LOOP_INTERVAL = int(os.getenv("RETROFEED_LOOP_INTERVAL", "300"))     # 5 min
DEDUPE_TTL   = int(os.getenv("RETROFEED_DEDUPE_TTL", "86400"))       # 24h
DRY_RUN      = os.getenv("RETROFEED_DRY_RUN", "0") == "1"            # não publica, só loga

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
    Não inventa dados além do espelhamento.
    """
    ud = lead.get("user_data") or {}
    tg = str(lead.get("telegram_id") or ud.get("telegram_id") or "")
    if tg:
        lead["telegram_id"] = tg
        ud.setdefault("telegram_id", tg)
        ud.setdefault("external_id", ud.get("external_id") or tg)
        lead["user_data"] = ud

def _ensure_core_fields(lead: Dict[str, Any], default_event: str = "Lead") -> Dict[str, Any]:
    """
    Ajusta apenas o essencial para dedupe e janela temporal.
    NÃO cria _fbp/_fbc e NÃO inventa IP/UA (para não derrubar score do pixel).
    """
    l = dict(lead)  # cópia defensiva

    # tipo de evento: padrão A (Lead principal; Subscribe é derivado pelo sender)
    et = (l.get("event_type") or default_event or "Lead").title()
    l["event_type"] = "Lead" if et.lower() not in ("lead", "subscribe") else et

    # event_time clamp (janela do CAPI)
    ts = int(l.get("event_time") or time.time())
    l["event_time"] = clamp_event_time(ts)

    # IDs coerentes
    _coalesce_ids(l)

    # event_id estável; se já veio, reaproveita
    l["event_id"] = l.get("event_id") or build_event_id(l["event_type"], l, l["event_time"])
    return l

def _already_enqueued(r: Redis, event_id: str) -> bool:
    """
    Dedupe simples via SETNX+TTL no Redis.
    True => já visto recentemente (pular).
    False => ainda não visto (pode enfileirar).
    """
    try:
        key = _dedupe_key(event_id)
        if r.setnx(key, "1"):
            r.expire(key, DEDUPE_TTL)
            return False
        return True
    except Exception:
        # Se o Redis falhar, não bloqueia o reenvio
        return False

# =============================
# Função principal de retrofeed
# =============================
async def retrofeed(batch_size: int = BATCH_SIZE) -> int:
    """
    Busca leads não enviados do DB (payload completo via db v3.1+),
    aplica garantias mínimas (event_time/event_id), dedupe,
    e reempilha no Redis Stream para reprocessamento pelo worker.
    """
    if not redis:
        logger.error("[RETROFEED_ERROR] Redis não configurado (REDIS_URL ausente).")
        return 0

    init_db()
    leads = await get_unsent_leads(limit=batch_size)  # já retorna LEAD completo

    if not leads:
        logger.info("[RETROFEED] Nenhum lead pendente.")
        return 0

    # Pré-processa, dedupe e prepara lote para pipeline
    to_enqueue: List[Dict[str, Any]] = []
    skipped = 0

    for lead in leads:
        enriched = _ensure_core_fields(lead, default_event="Lead")
        ev_id = enriched.get("event_id")
        tg_id = enriched.get("telegram_id")

        if not ev_id:
            # fallback paranoico (não deve acontecer)
            ev_id = build_event_id(enriched.get("event_type") or "Lead",
                                   enriched, enriched.get("event_time") or int(time.time()))
            enriched["event_id"] = ev_id

        if _already_enqueued(redis, ev_id):
            logger.info(f"[RETROFEED_SKIP] Já enfileirado recentemente event_id={ev_id} tg={tg_id}")
            skipped += 1
            continue

        to_enqueue.append(enriched)

    if not to_enqueue:
        logger.info(f"[RETROFEED_DONE] 0 reempilhados; skipped={skipped}.")
        return 0

    # Publica em pipeline
    published = 0
    if DRY_RUN:
        for item in to_enqueue:
            logger.info(f"[RETROFEED_DRY_RUN] Simulado XADD: tg={item.get('telegram_id')} ev={item.get('event_id')}")
        published = len(to_enqueue)
    else:
        retries = RETRY_MAX
        while retries > 0:
            try:
                pipe = redis.pipeline()
                for item in to_enqueue:
                    ev_id = item.get("event_id")
                    pipe.xadd(
                        STREAM,
                        {
                            "payload": _safe_dumps(item),  # compat com worker atual
                            "event_id": ev_id,
                            "type": item.get("event_type") or "Lead",
                        }
                    )
                pipe.execute()
                published = len(to_enqueue)
                break
            except Exception as e:
                retries -= 1
                if retries <= 0:
                    logger.error(f"[RETROFEED_FAIL] Falha final ao publicar lote: {e}")
                    break
                backoff = _jitter(1.0) * (RETRY_MAX - retries + 1)
                logger.warning(f"[RETROFEED_RETRY] Falha ao publicar lote (restam={retries}) err={e} backoff={backoff:.2f}s")
                await asyncio.sleep(backoff)

    logger.info(f"[RETROFEED_DONE] {published} leads reempilhados; skipped={skipped}.")
    return published

# =============================
# Loop contínuo (opcional)
# =============================
async def retrofeed_loop(interval: int = LOOP_INTERVAL):
    """
    Executa retrofeed em loop contínuo a cada X segundos.
    Ideal para processos em background (supervisor/container).
    """
    logger.info(f"[RETROFEED_LOOP] Iniciando com intervalo {interval}s (dry_run={DRY_RUN}).")
    while True:
        try:
            await retrofeed(batch_size=BATCH_SIZE)
        except Exception as e:
            logger.error(f"[RETROFEED_LOOP_ERROR] {e}")
        await asyncio.sleep(interval)

# =============================
# Execução standalone
# =============================
if __name__ == "__main__":
    try:
        asyncio.run(retrofeed(batch_size=BATCH_SIZE))
    except KeyboardInterrupt:
        logger.warning("[RETROFEED_STOPPED] Retrofeed encerrado manualmente.")