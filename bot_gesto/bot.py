# bot.py — v4.0 (envio direto; AM-friendly; sem dados fake; dedupe estável)
# - Parser de /start robusto: t_<token> (Redis), JSON inline, Base64URL (b64:...), e k=v&...
# - Sinais reais: NÃO cria _fbp “do nada” e só deriva _fbc se houver fbclid.
# - login_id → external_id (identidade de login); zip/cep; click_id; consent snapshot
# - event_id calculado já no bot (mantém dedupe consistente em toda a stack)
# - Sem IP/UA inventados (usa apenas os do bridge quando existirem; caso ausente, não envia)
# - Logs estruturados + métricas Prometheus
# - Nenhuma ENV nova obrigatória

import os, logging, json, asyncio, time, base64
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import parse_qsl

from aiogram import Bot, Dispatcher, types
import redis
from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram

# =============================
# DB / Pixels / Utils
# =============================
from bot_gesto.db import save_lead, init_db, sync_pending_leads
from bot_gesto.fb_google import send_event_with_retry  # envio direto, sem fila
from bot_gesto.utils import now_ts, clamp_event_time, build_event_id

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
        return json.dumps(log, ensure_ascii=False)

logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(JSONFormatter())
logger.addHandler(ch)

# =============================
# ENV (nenhuma nova obrigatória)
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_CHANNEL = os.getenv("VIP_CHANNEL")                  # chat_id numérico do canal VIP
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYNC_INTERVAL_SEC = int(os.getenv("SYNC_INTERVAL_SEC", "60"))
BRIDGE_NS = os.getenv("BRIDGE_NS", "typebot")
VIP_PUBLIC_USERNAME = (os.getenv("VIP_PUBLIC_USERNAME") or "").strip().lstrip("@")

# opcional: anti-flood leve no /start (0 = desliga)
LEAD_THROTTLE_SEC = int(os.getenv("LEAD_THROTTLE_SEC", "0"))

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
START_THROTTLED = Counter('bot_start_throttled_total', 'Mensagens /start ignoradas por throttle')

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
def _try_parse_json(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        return None

def _try_parse_b64url(s: str) -> Optional[Dict[str, Any]]:
    """
    Aceita 'b64:<payload>' ou um blob base64url direto; tenta JSON primeiro, depois k=v&...
    """
    raw = s[4:] if s.startswith("b64:") else s
    padding = '=' * (-len(raw) % 4)
    try:
        decoded = base64.urlsafe_b64decode(raw + padding).decode()
        return _try_parse_json(decoded) or dict(parse_qsl(decoded))
    except Exception:
        return None

def _try_parse_kv(s: str) -> Optional[Dict[str, Any]]:
    try:
        pairs = dict(parse_qsl(s, keep_blank_values=False))
        return pairs or None
    except Exception:
        return None

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
                    return data or {}
                except Exception:
                    return {}
            return {}

        # JSON inline
        if raw.startswith("{") and raw.endswith("}"):
            parsed = _try_parse_json(raw)
            if parsed is not None:
                return parsed

        # Base64URL (b64: ou direto)
        b64_parsed = _try_parse_b64url(raw)
        if b64_parsed is not None:
            return b64_parsed

        # k=v&x=y
        kv = _try_parse_kv(raw)
        if kv is not None:
            return kv

    except Exception:
        pass
    return {}

# =============================
# Construção do Lead (enriquecido, sem “fake”)
# =============================
def build_lead(user: types.User, msg: types.Message, args: Dict[str, Any]) -> Dict[str, Any]:
    user_id = user.id
    now = int(time.time())

    # Sinais FB vindos do Bridge/Typebot
    fbclid = args.get("fbclid")
    fbp = args.get("_fbp")                       # NÃO inventa _fbp se não vier
    # fbc real: _fbc do cookie OU “fb.1.<ts>.<fbclid>” somente se houver fbclid
    fbc = args.get("_fbc") or (f"fb.1.{now}.{fbclid}" if fbclid else None)

    # IP/UA: usa apenas se vierem do Bridge (sem default)
    ip_from_bridge = args.get("ip")
    ua_from_bridge = args.get("user_agent")

    # event_source_url (fallback para o @username público do canal)
    event_source_url = (
        args.get("event_source_url")
        or args.get("landing_url")
        or (f"https://t.me/{VIP_PUBLIC_USERNAME}" if VIP_PUBLIC_USERNAME else None)
    )

    # cookies (apenas os que EXISTEM; sem “fake”)
    cookies = {}
    if fbp: cookies["_fbp"] = encrypt_data(fbp)
    if fbc: cookies["_fbc"] = encrypt_data(fbc)

    # CEP/ZIP
    zip_code = args.get("zip") or args.get("postal_code") or args.get("cep")

    # click_id (útil para auditoria/dedupe)
    click_id = (
        args.get("click_id")
        or args.get("gclid")
        or args.get("wbraid")
        or args.get("gbraid")
        or fbclid
    )

    # login_id / external_id (identidade de login); external_id = login_id (se vier) senão telegram_id
    login_id = args.get("login_id")
    external_id = args.get("external_id") or login_id or str(user_id)

    # consent snapshot (opcional, vindo do bridge)
    consent = args.get("consent") if isinstance(args.get("consent"), dict) else None

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
        "external_id": external_id,
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "premium": getattr(user, "is_premium", False),
        "lang": user.language_code or "",
        "origin": "telegram",
        "event_type": "Lead",

        # sinais técnicos (somente se reais)
        # IMPORTANTE: não inventamos UA/IP; só repassamos se existirem
        "user_agent": ua_from_bridge,
        "ip": ip_from_bridge,
        "event_source_url": event_source_url,
        "event_time": now_ts(),
        "event_key": f"tg-{user_id}-{now}",
        "click_id": click_id,

        # enrichment auxiliar
        "cookies": cookies if cookies else None,
        "device_info": device_info,
        "session_metadata": {"msg_id": msg.message_id, "chat_id": msg.chat.id},

        # UTM e clids
        "utm_source": args.get("utm_source") or "telegram",
        "utm_medium": args.get("utm_medium") or "botb",
        "utm_campaign": args.get("utm_campaign") or "vip_access",
        "utm_term": args.get("utm_term"),
        "utm_content": args.get("utm_content"),

        "gclid": args.get("gclid"),
        "gbraid": args.get("gbraid"),
        "wbraid": args.get("wbraid"),
        "cid": args.get("cid"),
        "fbclid": fbclid,

        "value": args.get("value") or 0,
        "currency": args.get("currency") or "BRL",

        # Geo (se vier do bridge)
        "country": args.get("country"),
        "city": args.get("city"),
        "state": args.get("state"),

        # espelho para utils/CAPI (hashing lá dentro)
        "_fbp": fbp,
        "_fbc": fbc,

        # custom_data pré-existente (para preservarmos consent e outras flags)
        "custom_data": {"consent": consent} if consent else None,

        "user_data": {
            "email": args.get("email"),
            "phone": args.get("phone"),
            "first_name": args.get("first_name") or user.first_name,
            "last_name": args.get("last_name") or user.last_name,
            "city": args.get("city"),
            "state": args.get("state"),
            "zip": zip_code,
            "country": args.get("country"),
            "telegram_id": str(user_id),
            "external_id": external_id,
            "login_id": login_id,     # identificação de login (se existir)
            "fbp": fbp,
            "fbc": fbc,
            "ip": ip_from_bridge,
            "ua": ua_from_bridge,
        }
    }

    # event_id estável já no bot (mantém dedupe consistente em toda a stack)
    clamped = clamp_event_time(int(lead["event_time"]))
    lead["event_id"] = build_event_id("Lead", lead, clamped)

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
    # throttle leve anti-flood (opcional)
    if LEAD_THROTTLE_SEC > 0:
        key = f"lead:throttle:{msg.from_user.id}"
        if not redis_client.set(key, "1", nx=True, ex=LEAD_THROTTLE_SEC):
            START_THROTTLED.inc()
            logger.info(json.dumps({"event": "START_THROTTLED", "telegram_id": msg.from_user.id}))
            await msg.answer("⏳ Um instante… já estamos processando seu acesso.")

    start_t = time.perf_counter()
    args = parse_start_args(msg)
    lead = build_lead(msg.from_user, msg, args)

    # persiste no DB
    await save_lead(lead)

    # gera link VIP (não bloqueia envio do evento)
    vip_link = await generate_vip_link(lead["event_key"])

    # dispara o evento (Lead). Subscribe automático ocorre no fb_google se habilitado
    asyncio.create_task(send_event_with_retry("Lead", lead))
    LEADS_TRIGGERED.inc()

    PROCESS_LATENCY.observe(time.perf_counter() - start_t)
    logger.info(json.dumps({
        "event": "EVENT_TRIGGERED",
        "dispatch_path": "direct",
        "type": "Lead",
        "telegram_id": lead.get("telegram_id"),
        "has_fbp": bool(lead.get("_fbp")),
        "has_fbc": bool(lead.get("_fbc")),
        "has_fbclid": bool(lead.get("fbclid")),
        "has_login_id": bool(lead.get("user_data", {}).get("login_id")),
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