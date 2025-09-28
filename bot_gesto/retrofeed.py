# ======================================================
# retrofeed.py — v3.0 full (payload completo + dedupe + robustez)
# ======================================================
import os, json, asyncio, logging, time, random
from typing import Dict, Any, Optional
from redis import Redis

from bot_gesto.db import init_db, get_unsent_leads
from bot_gesto.utils import clamp_event_time, build_event_id

# =============================
# Configurações
# =============================
REDIS_URL   = os.getenv("REDIS_URL")
STREAM      = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE  = int(os.getenv("RETROFEED_BATCH", "100"))
RETRY_MAX   = int(os.getenv("RETROFEED_RETRY_MAX", "3"))
LOOP_INTERVAL = int(os.getenv("RETROFEED_LOOP_INTERVAL", "300"))  # 5 min
DEDUPE_TTL = int(os.getenv("RETROFEED_DEDUPE_TTL", "86400"))      # 24h
DRY_RUN     = os.getenv("RETROFEED_DRY_RUN", "0") == "1"          # não publica, só loga

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
    return base + random.random() * 0.25  # leve jitter anti-thundering herd

def _dedupe_key(event_id: str) -> str:
    return f"retrofeed:seen:{event_id}"

def _coalesce_ids(lead: Dict[str, Any]) -> None:
    """
    Garante que telegram_id/external_id existam no topo e dentro de user_data.
    Não inventa nada além de espelhar o que já existe.
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
    Não cria _fbp/_fbc, não inventa IP/UA: evita derrubar score.
    """
    l = dict(lead)  # cópia defensiva

    # tipo de evento padrão: padrão A (Lead único; Subscribe é derivado pelo sender)
    et = (l.get("event_type") or default_event or "Lead").title()
    l["event_type"] = "Lead" if et.lower() not in ("lead", "subscribe") else et

    # event_time clamp (janela CAPI)
    ts = int(l.get("event_time") or time.time())
    l["event_time"] = clamp_event_time(ts)

    # IDs coerentes
    _coalesce_ids(l)
    # event_id estável (reusa se já existir)
    l["event_id"] = l.get("event_id") or build_event_id(l["event_type"], l, l["event_time"])

    return l

def _already_enqueued(r: Redis, event_id: str) -> bool:
    """
    Dedupe simples via SET/TTL em Redis. Se já visto nas últimas 24h (padrão), pula.
    """
    try:
        key = _dedupe_key(event_id)
        if r.setnx(key, "1"):
            r.expire(key, DEDUPE_TTL)
            return False
        return True
    except Exception:
        # Em caso de falha no Redis, não bloquear o reenvio.
        return False


# =============================
# Função principal de retrofeed
# =============================
async def retrofeed(batch_size: int = BATCH_SIZE) -> int:
    """
    Busca leads não enviados do DB (já com payload completo),
    aplica garantias mínimas (event_time/event_id), dedupe,
    e reempilha no Redis Stream para reprocessamento pelo worker.
    """
    if not redis:
        logger.error("[RETROFEED_ERROR] Redis não configurado (REDIS_URL ausente).")
        return 0

    init_db()
    leads = await get_unsent_leads(limit=batch_size)  # ← já retorna payload completo (db v3.1+)

    if not leads:
        logger.info("[RETROFEED] Nenhum lead pendente.")
        return 0

    count = 0
    for lead in leads:
        enriched = _ensure_core_fields(lead, default_event="Lead")
        ev_id = enriched.get("event_id")
        tg_id = enriched.get("telegram_id")

        if not ev_id:
            # fallback paranoico (não deveria acontecer)
            ev_id = build_event_id(enriched.get("event_type") or "Lead",
                                   enriched, enriched.get("event_time") or int(time.time()))
            enriched["event_id"] = ev_id

        # Dedupe por event_id (evita reempilhar o mesmo item em loops/supervisores)
        if _already_enqueued(redis, ev_id):
            logger.info(f"[RETROFEED_SKIP] Já enfileirado recentemente event_id={ev_id} tg={tg_id}")
            continue

        payload = json.dumps(enriched, ensure_ascii=False)
        retries = RETRY_MAX

        while retries > 0:
            try:
                if DRY_RUN:
                    logger.info(f"[RETROFEED_DRY_RUN] Simulado XADD: tg={tg_id} ev={ev_id}")
                else:
                    # Mantemos 'payload' como campo principal para compatibilidade com o worker atual.
                    # Campos extras ('event_id', 'type') não quebram leitores antigos.
                    redis.xadd(STREAM, {
                        "payload": payload,
                        "event_id": ev_id,
                        "type": enriched.get("event_type") or "Lead",
                    })
                logger.info(f"[RETROFEED_ENQUEUED] tg={tg_id} event_id={ev_id}")
                count += 1
                break

            except Exception as e:
                retries -= 1
                if retries <= 0:
                    logger.error(f"[RETROFEED_FAIL] tg={tg_id} event_id={ev_id} err={e}")
                    break
                backoff = _jitter(1.0) * (RETRY_MAX - retries + 1)
                logger.warning(
                    f"[RETROFEED_RETRY] tg={tg_id} event_id={ev_id} "
                    f"(restam={retries}) err={e} backoff={backoff:.2f}s"
                )
                await asyncio.sleep(backoff)

    logger.info(f"[RETROFEED_DONE] {count} leads reempilhados.")
    return count


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