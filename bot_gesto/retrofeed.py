# retrofeed.py — versão 2.1 avançada e robusta
import os, json, asyncio, logging, time
from redis import Redis
from bot_gesto.db import init_db, get_unsent_leads

# =============================
# Configurações
# =============================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")
BATCH_SIZE = int(os.getenv("RETROFEED_BATCH", "100"))
RETRY_MAX = int(os.getenv("RETROFEED_RETRY_MAX", "3"))

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# Logger padronizado (compatível com supervisord)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [retrofeed] %(message)s"
)
logger = logging.getLogger("retrofeed")

# ==============================
# Função de reempilhamento
# ==============================
async def retrofeed(batch_size: int = BATCH_SIZE) -> int:
    """
    Busca leads não enviados no DB e reempilha no Redis para reprocessamento.
    Retorna a quantidade de leads reempilhados.
    """
    init_db()
    leads = await get_unsent_leads(limit=batch_size)
    if not leads:
        logger.info("Nenhum lead pendente para retrofeed.")
        return 0

    count = 0
    for lead in leads:
        payload = json.dumps(lead)
        retries = RETRY_MAX
        while retries > 0:
            try:
                redis.xadd(STREAM, {"payload": payload})
                logger.info(
                    f"[RETROFEED] Lead reempilhado -> "
                    f"telegram_id={lead.get('telegram_id')} event_type={lead.get('event_type')}"
                )
                count += 1
                break
            except Exception as e:
                retries -= 1
                logger.warning(
                    f"[RETROFEED_RETRY] Falha ao reempilhar lead "
                    f"telegram_id={lead.get('telegram_id')} "
                    f"(tentativas restantes={retries}) err={e}"
                )
                time.sleep(1)
    logger.info(f"[RETROFEED_DONE] {count} leads reempilhados.")
    return count

# ==============================
# Loop contínuo opcional
# ==============================
async def retrofeed_loop(interval: int = 300):
    """
    Loop que roda o retrofeed a cada X segundos.
    Ideal para rodar dentro do bot supervisorado.
    """
    while True:
        try:
            await retrofeed(batch_size=BATCH_SIZE)
        except Exception as e:
            logger.error(f"[RETROFEED_LOOP_ERROR] {e}")
        await asyncio.sleep(interval)

# ==============================
# Main (execução standalone)
# ==============================
if __name__ == "__main__":
    try:
        asyncio.run(retrofeed(batch_size=BATCH_SIZE))
    except KeyboardInterrupt:
        logger.warning("Retrofeed encerrado manualmente.")