# worker.py — versão 3.1 robusta (concorrente + auto-claim + db sync)
import os, asyncio, json, signal, logging, time
from typing import Tuple, Dict, Any, List
from redis import Redis
from bot_gesto.fb_google import send_event_with_retry
from bot_gesto.utils import derive_event_from_route, should_send_event
from bot_gesto.db import save_lead

# =============================
# Logger
# =============================
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("worker")

# =============================
# Configurações
# =============================
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")
GROUP = os.getenv("REDIS_GROUP", "botb_group")
CONSUMER = os.getenv("REDIS_CONSUMER", f"worker-{os.getpid()}")

# Concorrência e resiliência
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "10"))         # nº máx. de eventos processados em paralelo
READ_COUNT = int(os.getenv("WORKER_READ_COUNT", "20"))                   # lote por leitura no stream
READ_BLOCK_MS = int(os.getenv("WORKER_READ_BLOCK_MS", "5000"))           # bloqueio em ms no xreadgroup
AUTOCLAIM_MIN_IDLE_MS = int(os.getenv("AUTOCLAIM_MIN_IDLE_MS", "60000")) # tempo mínimo parado para re-clamar pendentes
AUTOCLAIM_BATCH = int(os.getenv("AUTOCLAIM_BATCH", "50"))                # lote de autoclain por iteração
AUTOCLAIM_INTERVAL = float(os.getenv("AUTOCLAIM_INTERVAL", "30"))        # intervalo (s) entre rodadas de autoclain

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# =============================
# Garante que o grupo de consumidores exista
# =============================
try:
    redis.xgroup_create(name=STREAM, groupname=GROUP, id="$", mkstream=True)
    print(f"[INIT] Grupo {GROUP} criado no stream {STREAM}")
    logger.info(f"[INIT] Grupo {GROUP} criado no stream {STREAM}")
except Exception as e:
    if "BUSYGROUP" in str(e):
        print(f"[INIT] Grupo {GROUP} já existe, seguindo...")
        logger.info(f"[INIT] Grupo {GROUP} já existe, seguindo...")
    else:
        print(f"[INIT] Erro ao criar grupo {GROUP}: {e}")
        logger.error(f"[INIT] Erro ao criar grupo {GROUP}: {e}")
        raise

# Logger básico (para aparecer no supervisord sem Illegal seek)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

running = True

# Semáforo para limitar concorrência
_sem = asyncio.Semaphore(WORKER_CONCURRENCY)

# =========================
# Utilidades de ACK/Log
# =========================
def _safe_ack(entry_id: str):
    try:
        redis.xack(STREAM, GROUP, entry_id)
    except Exception as e:
        logger.error(f"[ACK_ERROR] {entry_id}: {e}")

def _parse_payload(entry_id: str, entry_data: Dict[str, Any]) -> Dict[str, Any] | None:
    try:
        return json.loads(entry_data.get("payload", "{}"))
    except Exception as e:
        print(f"[ERRO] Parse payload {entry_id}: {e}")
        logger.error(f"[ERRO] Parse payload {entry_id}: {e}")
        return None

# =========================
# Processamento de 1 item
# =========================
async def process_entry(entry_id: str, entry_data: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Processa 1 lead do Redis Stream e retorna (entry_id, success).
    success=True => pode ack
    """
    ld = _parse_payload(entry_id, entry_data)
    if not ld:
        # payload inválido -> acka para não travar a fila
        return (entry_id, True)

    # Determina qual evento disparar (Lead / Subscribe)
    route_key = ld.get("route_key") or ld.get("link_key") or ""
    event = derive_event_from_route(route_key)

    if not should_send_event(event):
        msg = f"[SKIP] {entry_id} evento não permitido ou não reconhecido -> {event}"
        print(msg)
        logger.warning(msg)
        # Sem ação de pixels, mas salvamos histórico mínimo e ackamos
        try:
            await save_lead(ld, event_record={"event": event or "unknown", "status": "skipped"})
        except Exception as e:
            logger.error(f"[DB_SKIP_SAVE_ERR] {entry_id}: {e}")
        return (entry_id, True)

    lead_id = ld.get('telegram_id') or ld.get('external_id')
    print(f"[EVENT] {entry_id} -> {event} para lead {lead_id}")
    logger.info(f"[EVENT] {entry_id} -> {event} para lead {lead_id}")

    # Envia para Facebook + Google com retry inteligente
    try:
        results = await send_event_with_retry(event, ld)
        print(f"[RESULT] {entry_id}: {results}")
        logger.info(f"[RESULT] {entry_id}: {results}")

        # Registra no banco com histórico
        status = "success" if results.get("status") == "success" else "failed"
        event_record = {
            "event": event,
            "entry_id": entry_id,
            "results": results,
            "status": status
        }
        try:
            await save_lead(ld, event_record=event_record)
        except Exception as e:
            logger.error(f"[DB_SAVE_ERR] {entry_id}: {e}")

        # Política de ack:
        # - Com sucesso: ACK imediato.
        # - Em falha: NÃO ACK (mantém na pendência para reprocesso/claim posterior),
        #   mas o DB guarda status=failed (retrofeed pode reenviar depois).
        return (entry_id, status == "success")

    except Exception as e:
        msg = f"[ERRO] Falha ao enviar evento {entry_id}: {e}"
        print(msg)
        logger.error(msg)
        try:
            await save_lead(ld, event_record={"event": event, "status": "failed", "error": str(e)})
        except Exception as e2:
            logger.error(f"[DB_SAVE_ON_ERROR] {entry_id}: {e2}")
        return (entry_id, False)

# =========================
# Auto-claim de pendências
# =========================
def _autoclaim_once() -> List[Tuple[str, Dict[str, Any]]]:
    """
    Reivindica mensagens paradas (pendentes) muito tempo no grupo e as retorna
    para reprocessamento por este consumidor.
    """
    try:
        # XAUTOCLAIM <key> <group> <consumer> <min-idle-time> <start> COUNT <n>
        # redis-py retorna (next_start_id, [(id, {fields})...])
        next_id, claimed = redis.xautoclaim(
            STREAM, GROUP, CONSUMER, min_idle_time=AUTOCLAIM_MIN_IDLE_MS, start_id="0-0", count=AUTOCLAIM_BATCH
        )
        out = []
        for msg_id, fields in claimed or []:
            out.append((msg_id, fields))
        if out:
            logger.info(f"[AUTOCLAIM] reclaimed={len(out)} start={next_id}")
        return out
    except Exception as e:
        logger.warning(f"[AUTOCLAIM_ERR] {e}")
        return []

async def _periodic_autoclaim(queue: asyncio.Queue):
    """
    Tarefa periódica que reclama pendências e reenvia para a fila local de processamento.
    """
    while running:
        await asyncio.sleep(AUTOCLAIM_INTERVAL)
        claimed = _autoclaim_once()
        for msg_id, fields in claimed:
            # re-enfileira localmente para processamento concorrente
            await queue.put((msg_id, fields))

# =========================
# Loop principal (concorrente)
# =========================
async def process_batch():
    """
    Loop principal: lê stream do Redis e processa eventos com concorrência controlada.
    Também executa um reclaimer periódico de mensagens pendentes.
    """
    global running

    # Fila local para integrar leituras do stream e de auto-claim
    local_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue()

    # Task periódica de autoclain
    autoclaim_task = asyncio.create_task(_periodic_autoclaim(local_queue))

    async def _spawn_worker(entry_id: str, entry_data: Dict[str, Any]):
        async with _sem:
            eid, ok = await process_entry(entry_id, entry_data)
            if ok:
                _safe_ack(eid)

    in_flight: List[asyncio.Task] = []

    try:
        while running:
            try:
                # 1) Alimenta a fila local com mensagens novas do stream
                entries = redis.xreadgroup(
                    GROUP, CONSUMER, {STREAM: ">"}, count=READ_COUNT, block=READ_BLOCK_MS
                )
                if entries:
                    for _stream_name, msgs in entries:
                        for entry_id, entry_data in msgs:
                            await local_queue.put((entry_id, entry_data))

                # 2) Desenfileira e processa concorrente até o limite
                while not local_queue.empty() and len(in_flight) < WORKER_CONCURRENCY:
                    entry_id, entry_data = await local_queue.get()
                    t = asyncio.create_task(_spawn_worker(entry_id, entry_data))
                    in_flight.append(t)

                # 3) Limpa tasks concluídas
                if in_flight:
                    done, pending = await asyncio.wait(in_flight, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                    in_flight = [t for t in pending if not t.done()]

            except Exception as e:
                print(f"[ERRO LOOP] {e}")
                logger.error(f"[ERRO LOOP] {e}")
                await asyncio.sleep(2)

    finally:
        # Encerramento: espera tarefas pendentes
        logger.warning("[SHUTDOWN] aguardando tarefas em andamento...")
        try:
            await asyncio.wait(in_flight, timeout=10)
        except Exception:
            pass
        # Cancela autoclaim
        try:
            autoclaim_task.cancel()
        except Exception:
            pass
        logger.warning("[SHUTDOWN] finalizado loop principal.")

# =========================
# Sinais de encerramento
# =========================
def shutdown(sig, frame):
    global running
    print(f"[STOP] Signal {sig}, encerrando...")
    logger.warning(f"[STOP] Signal {sig}, encerrando...")
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
        logger.warning("Encerrado manualmente.")