# retrofeed.py
import os, json, asyncio, logging
from redis import Redis
from db import init_db, get_unsent_leads

# ==============================
# Config
# ==============================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

redis = Redis.from_url(REDIS_URL, decode_responses=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("retrofeed")

# ==============================
# Retrofeed logic
# ==============================
async def retrofeed(batch_size: int = 100):
    """
    Busca leads não enviados no DB e reempilha no Redis para reprocessamento.
    """
    init_db()
    leads = await get_unsent_leads(limit=batch_size)
    if not leads:
        logger.info("Nenhum lead pendente para retrofeed.")
        return 0

    for lead in leads:
        try:
            redis.xadd(STREAM, {"payload": json.dumps(lead)})
        except Exception as e:
            logger.error(f"Erro ao reempilhar lead {lead.get('telegram_id')}: {e}")

    logger.info(f"Retrofeed concluído: {len(leads)} leads reempilhados.")
    return len(leads)

# ==============================
# Main
# ==============================
if __name__ == "__main__":
    asyncio.run(retrofeed(batch_size=100))