# worker.py
import os, asyncio, json, signal
from redis import Redis
from fb_google import send_event
from utils import derive_event_from_route, should_send_event

# =========================
# Configurações
# =========================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")
GROUP = os.getenv("REDIS_GROUP", "botb_group")
CONSUMER = os.getenv("REDIS_CONSUMER", "worker-1")

redis = Redis.from_url(REDIS_URL, decode_responses=True)

running = True

# =========================
# Processamento de batch
# =========================
async def process_entry(entry_id, entry_data):
    """
    Processa 1 lead do Redis Stream.
    Espera que entry_data contenha 'payload' (JSON do lead).
    """
    try:
        ld = json.loads(entry_data.get("payload", "{}"))
    except Exception as e:
        print(f"[ERRO] Parse payload {entry_id}: {e}")
        return

    # Determina qual evento disparar (Lead / Subscribe)
    route_key = ld.get("route_key") or ld.get("link_key") or ""
    event = derive_event_from_route(route_key)

    if not should_send_event(event):
        print(f"[SKIP] {entry_id} evento não permitido ou não reconhecido -> {event}")
        return

    print(f"[EVENT] {entry_id} -> {event} para lead {ld.get('telegram_id') or ld.get('external_id')}")

    # Envia para Facebook + Google
    results = await send_event(event, ld)
    print(f"[RESULT] {entry_id}: {results}")

async def process_batch():
    """
    Loop principal: lê stream do Redis e processa eventos.
    """
    global running
    while running:
        try:
            entries = redis.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=10, block=5000)
            if not entries:
                continue

            for stream_name, msgs in entries:
                for entry_id, entry_data in msgs:
                    await process_entry(entry_id, entry_data)
                    redis.xack(STREAM, GROUP, entry_id)
        except Exception as e:
            print(f"[ERRO LOOP] {e}")
            await asyncio.sleep(2)

# =========================
# Sinais de encerramento
# =========================
def shutdown(sig, frame):
    global running
    print(f"[STOP] Signal {sig}, encerrando...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# =========================
# Main
# =========================
if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(process_batch())
    except KeyboardInterrupt:
        print("Encerrado manualmente.")