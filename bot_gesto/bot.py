# bot.py — v3.0 (Padrão A: envio direto, sem fila) — alinhado a fb_google v3.0
import os, logging, json, asyncio, time
from datetime import datetime
from typing import Dict, Any, Optional

from aiogram import Bot, Dispatcher, types
import redis
from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram

# =============================
# DB / Pixels
# =============================
from bot_gesto.db import save_lead, init_db, sync_pending_leads
from bot_gesto.fb_google import send_event_with_retry  # <- envio direto, sem fila
from bot_gesto.utils import now_ts

# =============================
# Logging estruturado (JSON)
# =============================
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "time": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log)

logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(JSONFormatter())
logger.addHandler(ch)

# =============================
# ENV
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_CHANNEL = os.getenv("VIP_CHANNEL")  # chat_id do canal VIP
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYNC_INTERVAL_SEC = int(os.getenv("SYNC_INTERVAL_SEC", "60"))
BRIDGE_NS = os.getenv("BRIDGE_NS", "typebot")

# opcional: para construir fallback de URL de origem
VIP_PUBLIC_USERNAME = (os.getenv("VIP_PUBLIC_USERNAME") or "").strip().lstrip("@")

SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())
fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

if not BOT_TOKEN or not VIP_CHANNEL:
    raise RuntimeError("BOT_TOKEN e VIP_CHANNEL são obrigatórios")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# DB init
init_db()

# =============================
# Métricas Prometheus
# =============================
LEADS_TRIGGERED = Counter('bot_leads_triggered_total', 'Leads disparados (envio direto)')
PROCESS_LATENCY = Histogram('bot_process_latency_seconds', 'Latência no processamento')
VIP_LINK_ERRORS = Counter('bot_vip_link_errors_total', 'Falhas ao gerar link VIP')

# =============================
# Segurança
# =============================
def encrypt_data(data: Optional[str]) -> str:
    return fernet.encrypt(data.encode()).decode() if data else ""

# =============================
# VIP Link
# =============================
async def generate_vip_link(event_key: str, member_limit=1, expire_hours=24) -> Optional[str]:
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=int(VIP_CHANNEL),
            member_limit=member_limit,
            expire_date=int(time.time()) + expire_hours * 3600,
            name=f"VIP-{event_key}"
        )
        return invite.invite_link
    except Exception as e:
        VIP_LINK_ERRORS.inc()
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None

# =============================
# Parser de argumentos do /start
# =============================
def parse_start_args(msg: types.Message) -> Dict[str, Any]:
    try:
        raw = msg.get_args() if hasattr(msg, "get_args") else None
        if not raw:
            return {}
        raw = raw.strip()

        # deep-link do Bridge: t_<token>
        if raw.startswith("t_"):
            token = raw[2:]
            blob = redis_client.get(f"{BRIDGE_NS}:{token}")
            if blob:
                try:
                    data = json.loads(blob)
                    redis_client.delete(f"{BRIDGE_NS}:{token}")  # one-shot
                    return data
                except Exception:
                    return {}
            return {}

        # json inline (fallback)
        if raw.startswith("{") and raw.endswith("}"):
            return json.loads(raw)

    except Exception:
        pass
    return {}

# =============================
# Construção do Lead enriquecido
# =============================
def build_lead(user: types.User, msg: types.Message, args: Dict[str, Any]) -> Dict[str, Any]:
    user_id = user.id
    now = int(time.time())

    # Sinais FB (mantém os do Bridge quando vierem)
    fbp = args.get("_fbp") or f"fb.1.{now}.{user_id}"
    fbc = args.get("_fbc") or (f"fb.1.{now}.fbclid.{user_id}" if args.get("fbclid") else f"fbc-{user_id}-{now}")

    # NÃO inventar IP: se o Bridge não trouxe IP real, deixamos ausente
    ip_from_bridge = args.get("ip")
    ua_from_bridge = args.get("user_agent")

    # fonte do evento (para CAPI/GA4)
    event_source_url = (
        args.get("event_source_url")
        or args.get("landing_url")
        or (f"https://t.me/{VIP_PUBLIC_USERNAME}" if VIP_PUBLIC_USERNAME else None)
    )

    # cookies (opcional, útil para auditoria interna)
    cookies = {"_fbp": encrypt_data(fbp), "_fbc": encrypt_data(fbc)}

    device_info = {
        "platform": "telegram",
        "app": "aiogram",
        "device": args.get("device"),
        "os": args.get("os"),
        "browser": args.get("browser"),
        "url": event_source_url,
    }

    lead: Dict[str, Any] = {
        # chaves principais
        "telegram_id": user_id,
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "premium": getattr(user, "is_premium", False),
        "lang": user.language_code or "",
        "origin": "telegram",

        # sinais técnicos
        "user_agent": ua_from_bridge or "TelegramBot/1.0",
        "ip": ip_from_bridge,  # usado por utils.normalize_user_data
        "event_source_url": event_source_url,
        "event_time": now_ts(),
        "event_key": f"tg-{user_id}-{now}",

        # enrichment auxiliar
        "cookies": cookies,
        "device_info": device_info,
        "session_metadata": {"msg_id": msg.message_id, "chat_id": msg.chat.id},

        # UTM e clids (mantém enrichment do Bridge)
        "utm_source": args.get("utm_source") or "telegram",
        "utm_medium": args.get("utm_medium") or "botb",
        "utm_campaign": args.get("utm_campaign") or "vip_access",
        "utm_term": args.get("utm_term"),
        "utm_content": args.get("utm_content"),

        "gclid": args.get("gclid"),
        "gbraid": args.get("gbraid"),
        "wbraid": args.get("wbraid"),
        "cid": args.get("cid"),
        "fbclid": args.get("fbclid"),

        "value": args.get("value") or 0,
        "currency": args.get("currency") or "BRL",

        # Geo (pode vir do Bridge)
        "country": args.get("country"),
        "city": args.get("city"),
        "state": args.get("state"),

        # espelha sinais para utils/FB CAPI (hashing)
        "_fbp": fbp,
        "_fbc": fbc,
        "user_data": {
            "email": args.get("email"),
            "phone": args.get("phone"),
            "first_name": args.get("first_name") or user.first_name,
            "last_name": args.get("last_name") or user.last_name,
            "city": args.get("city"),
            "state": args.get("state"),
            "zip": args.get("zip"),
            "country": args.get("country"),
            "telegram_id": str(user_id),
            "external_id": str(user_id),
            "fbp": fbp,
            "fbc": fbc,
            "ip": ip_from_bridge,
            "ua": ua_from_bridge,
        }
    }
    return lead

# =============================
# Preview helper (invite sozinho)
# =============================
async def send_vip_message_with_preview(msg: types.Message, first_name: str, vip_link: str):
    try:
        await msg.answer(f"✅ <b>{first_name}</b>, seu acesso VIP foi liberado!\nLink exclusivo expira em 24h.")
        await asyncio.sleep(0.3)
        await bot.send_message(msg.chat.id, vip_link)
    except Exception as e:
        logger.error(json.dumps({"event": "PREVIEW_SEND", "error": str(e)}))
        await msg.answer(f"🔑 Acesse aqui: {vip_link}", disable_web_page_preview=False)

# =============================
# Processamento de novo lead (envio direto)
# =============================
async def process_new_lead(msg: types.Message):
    start_t = time.perf_counter()
    args = parse_start_args(msg)
    lead = build_lead(msg.from_user, msg, args)

    # persiste no DB
    await save_lead(lead)

    # gera link VIP (não bloqueia envio do evento)
    vip_link = await generate_vip_link(lead["event_key"])

    # dispara o evento de forma assíncrona (Lead apenas)
    # Subscribe automático acontecerá DENTRO do fb_google, se FB_AUTO_SUBSCRIBE_FROM_LEAD=1
    asyncio.create_task(send_event_with_retry("Lead", lead))
    LEADS_TRIGGERED.inc()

    PROCESS_LATENCY.observe(time.perf_counter() - start_t)
    logger.info(json.dumps({
        "event": "EVENT_TRIGGERED",
        "dispatch_path": "direct",
        "type": "Lead",
        "telegram_id": lead.get("telegram_id")
    }))

    return vip_link, lead

# =============================
# Handlers
# =============================
@dp.message_handler(commands=["start"])
async def start_cmd(msg: types.Message):
    await msg.answer("👋 Validando seu acesso VIP…")
    try:
        vip_link, lead = await process_new_lead(msg)
        if vip_link:
            await send_vip_message_with_preview(msg, lead['first_name'], vip_link)
        else:
            await msg.answer("⚠️ Seu acesso foi registrado, mas não foi possível gerar o link VIP agora.")
    except Exception as e:
        logger.error(json.dumps({"event": "START_HANDLER_ERROR", "error": str(e)}))
        await msg.answer("⚠️ Ocorreu um erro ao validar seu acesso. Tente novamente em alguns instantes.")

@dp.message_handler()
async def fallback(msg: types.Message):
    await msg.answer("Use /start para iniciar o fluxo de acesso VIP.")

# =============================
# Loop de sincronização pendentes (DB)
# =============================
async def _sync_pending_loop():
    while True:
        try:
            count = await sync_pending_leads()
            if count:
                logger.info(json.dumps({"event": "SYNC_PENDING", "processed": count}))
        except Exception as e:
            logger.error(json.dumps({"event": "SYNC_PENDING_ERROR", "error": str(e)}))
        await asyncio.sleep(SYNC_INTERVAL_SEC)

# =============================
# Runner
# =============================
if __name__ == "__main__":
    async def main():
        logger.info(json.dumps({"event": "BOT_START", "dispatch_path": "direct"}))
        asyncio.create_task(_sync_pending_loop())
        await dp.start_polling()
    asyncio.run(main())