# ======================================================
# retrofeed.py — v2.2 full (enriquecido + robusto + resiliente)
# ======================================================
import os, json, asyncio, logging, time
from redis import Redis
from bot_gesto.db import init_db, get_unsent_leads
from bot_gesto.utils import clamp_event_time, build_event_id

# =============================
# Configurações
# =============================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE = int(os.getenv("RETROFEED_BATCH", "100"))
RETRY_MAX = int(os.getenv("RETROFEED_RETRY_MAX", "3"))
LOOP_INTERVAL = int(os.getenv("RETROFEED_LOOP_INTERVAL", "300"))  # default 5min

redis = Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# =============================
# Logger padronizado
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [retrofeed] %(message)s"
)
logger = logging.getLogger("retrofeed")


# =============================
# Enriquecimento para retrofeed
# =============================
def enrich_lead_for_retrofeed(lead: dict, default_event: str = "Lead") -> dict:
    """
    Garante que o lead tenha os campos essenciais antes de ser reempilhado no Redis.
    Sem inventar dados sensíveis; apenas complementa os obrigatórios para deduplicação
    e rastreamento pelos pixels.
    """
    lead = dict(lead)  # cópia defensiva

    # Ajusta event_time para estar dentro da janela do pixel
    ts = int(lead.get("event_time") or time.time())
    lead["event_time"] = clamp_event_time(ts)

    # IDs para rastreamento
    tg_id = str(lead.get("telegram_id") or "")
    lead["telegram_id"] = tg_id
    lead["external_id"] = lead.get("external_id") or tg_id

    # Cookies essenciais
    if not lead.get("_fbp") and tg_id:
        lead["_fbp"] = f"fb.1.{int(time.time())}.{tg_id}"

    if not lead.get("_fbc") and tg_id:
        lead["_fbc"] = f"fb.1.{int(time.time())}.retro.{tg_id}"

    # Garantir event_id para deduplicação
    lead["event_id"] = build_event_id(default_event, lead, lead["event_time"])

    return lead


# =============================
# Função principal de retrofeed
# =============================
async def retrofeed(batch_size: int = BATCH_SIZE) -> int:
    """
    Busca leads não enviados do DB, enriquece e reempilha no Redis.
    Retorna a quantidade de leads reempilhados.
    """
    if not redis:
        logger.error("[RETROFEED_ERROR] Redis não configurado.")
        return 0

    init_db()
    leads = await get_unsent_leads(limit=batch_size)

    if not leads:
        logger.info("[RETROFEED] Nenhum lead pendente.")
        return 0

    count = 0
    for lead in leads:
        enriched = enrich_lead_for_retrofeed(lead)
        payload = json.dumps(enriched)
        retries = RETRY_MAX

        while retries > 0:
            try:
                redis.xadd(STREAM, {"payload": payload})
                logger.info(
                    f"[RETROFEED] Lead reempilhado "
                    f"telegram_id={enriched.get('telegram_id')} "
                    f"event_id={enriched.get('event_id')}"
                )
                count += 1
                break

            except Exception as e:
                retries -= 1
                logger.warning(
                    f"[RETROFEED_RETRY] Falha ao reempilhar lead "
                    f"telegram_id={enriched.get('telegram_id')} "
                    f"(tentativas restantes={retries}) err={e}"
                )
                time.sleep(1)

    logger.info(f"[RETROFEED_DONE] {count} leads reempilhados e enriquecidos.")
    return count


# =============================
# Loop contínuo (opcional)
# =============================
async def retrofeed_loop(interval: int = LOOP_INTERVAL):
    """
    Executa retrofeed em loop contínuo a cada X segundos.
    Ideal para processos em background (ex.: supervisor, container).
    """
    logger.info(f"[RETROFEED_LOOP] Iniciando com intervalo {interval}s.")
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