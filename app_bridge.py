# =============================
# app_bridge.py — versão 2.4 estável (sincronizado com bot_gesto direto)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from redis import Redis
from cryptography.fernet import Fernet

# =============================
# Logging JSON estruturado
# =============================
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log)

logger = logging.getLogger("bridge")
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(JSONFormatter())
logger.addHandler(_ch)

# =============================
# Import dos módulos do Bot B (pasta local bot_gesto)
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_B_PATH = os.path.join(BASE_DIR, "bot_gesto")
if BOT_B_PATH not in sys.path:
    sys.path.append(BOT_B_PATH)

try:
    from bot_gesto.db import save_lead
    from bot_gesto.fb_google import send_event_to_all
except ImportError as e:
    raise RuntimeError(
        f"❌ Falha ao importar módulos do Bot B (bot_gesto). "
        f"Verifique se db.py e fb_google.py existem em {BOT_B_PATH}. Erro: {e}"
    )

# =============================
# ENV do Bridge
# =============================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
TOKEN_TTL_SEC = int(os.getenv("TOKEN_TTL_SEC", "3600"))   # 1h
BRIDGE_API_KEY = os.getenv("BRIDGE_API_KEY", "")
ALLOWED_ORIGINS = (os.getenv("ALLOWED_ORIGINS", "") or "").split(",")
PORT = int(os.getenv("PORT", "8080"))

# Criptografia opcional
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
fernet = None
if CRYPTO_KEY:
    derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
    fernet = Fernet(derived)
    logger.info("✅ Crypto: Fernet habilitado no Bridge")

if not BOT_USERNAME:
    raise RuntimeError("BOT_USERNAME não configurado (ex.: SeuBotUsername)")

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# =============================
# App & CORS
# =============================
app = FastAPI(title="Typebot Bridge", version="2.4.0")

if ALLOWED_ORIGINS and ALLOWED_ORIGINS != ['']:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in ALLOWED_ORIGINS if o.strip()],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# =============================
# Schemas
# =============================
class TBPayload(BaseModel):
    # IDs/cookies
    _fbp: Optional[str] = None
    _fbc: Optional[str] = None
    fbclid: Optional[str] = None
    gclid: Optional[str] = None
    gbraid: Optional[str] = None
    wbraid: Optional[str] = None
    cid: Optional[str] = None  # GA client_id

    # UTM & origem
    landing_url: Optional[str] = None
    event_source_url: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None

    # Device
    device: Optional[str] = None
    os: Optional[str] = None
    user_agent: Optional[str] = Field(default=None, alias="user_agent")

    # Dados de contato
    email: Optional[str] = None
    phone: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None

    # Valor opcional
    value: Optional[float] = None
    currency: Optional[str] = None

    # Extras livres
    extra: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True

# =============================
# Helpers
# =============================
def _make_token(nbytes: int = 16) -> str:
    return secrets.token_urlsafe(nbytes)

def _key(token: str) -> str:
    return f"typebot:{token}"

def _deep_link(token: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=t_{token}"

def _auth_guard(x_api_key: Optional[str]):
    if BRIDGE_API_KEY and (not x_api_key or x_api_key != BRIDGE_API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")

def _enrich_payload(data: dict, client_ip: str, client_ua: str) -> dict:
    """Aplica enriquecimento e normalização no payload recebido"""
    data.setdefault("ip", client_ip)
    data.setdefault("user_agent", data.get("user_agent") or client_ua)
    data.setdefault("ts", int(time.time()))

    # cookies FB
    fbclid = data.get("fbclid")
    if fbclid and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{fbclid}"
    if not data.get("_fbp"):
        data["_fbp"] = f"fb.1.{int(time.time())}.{secrets.randbelow(999_999_999)}"

    # GA identifiers fallback
    if not data.get("cid") and data.get("gclid"):
        data["cid"] = f"gclid.{data['gclid']}"

    # Criptografia opcional
    if fernet:
        try:
            raw = json.dumps(data).encode()
            enc = fernet.encrypt(raw).decode()
            data["_encrypted"] = enc
        except Exception as e:
            logger.warning(f"⚠️ Encryption failed: {e}")

    return data

# =============================
# Rotas
# =============================
@app.get("/health")
def health():
    try:
        redis.ping()
        status = {"status": "ok", "redis": "ok"}
    except Exception as e:
        status = {"status": "degraded", "redis_error": str(e)}

    status.update({
        "bot_dir": BOT_B_PATH,
        "db_present": os.path.isfile(os.path.join(BOT_B_PATH, "db.py")),
        "fb_present": os.path.isfile(os.path.join(BOT_B_PATH, "fb_google.py")),
    })
    return status

@app.post("/tb/link")
async def create_deeplink(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False)
):
    _auth_guard(x_api_key)

    client_ip = req.client.host if req.client else None
    client_ua = req.headers.get("user-agent")

    data = body.dict(by_alias=True, exclude_none=True)
    enriched = _enrich_payload(data, client_ip, client_ua)

    token = _make_token()
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(enriched))
    deep_link = _deep_link(token)

    logger.info(json.dumps({
        "event": "TB_LINK_CREATED",
        "token": token,
        "expires_in": TOKEN_TTL_SEC,
        "payload_keys": list(enriched.keys())
    }))

    return {"deep_link": deep_link, "token": token, "expires_in": TOKEN_TTL_SEC}

@app.post("/webhook")
async def webhook(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False)
):
    """
    Integração direta: Typebot → Bridge → DB + Pixels (FB/GA4)
    """
    _auth_guard(x_api_key)

    client_ip = req.client.host if req.client else None
    client_ua = req.headers.get("user-agent")

    data = body.dict(by_alias=True, exclude_none=True)
    enriched = _enrich_payload(data, client_ip, client_ua)

    # Salva no DB
    asyncio.create_task(save_lead(enriched))

    # Dispara eventos (Lead padrão)
    asyncio.create_task(send_event_to_all(enriched, et="Lead"))

    logger.info(json.dumps({
        "event": "WEBHOOK_RECEIVED",
        "payload_keys": list(enriched.keys())
    }))

    return {"status": "ok", "saved": True, "events": ["Lead"]}

@app.get("/tb/peek/{token}")
def peek_token(token: str, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key)
    blob = redis.get(_key(token))
    if not blob:
        raise HTTPException(status_code=404, detail="token not found/expired")
    return {"token": token, "payload": json.loads(blob)}

@app.delete("/tb/del/{token}")
def delete_token(token: str, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key)
    redis.delete(_key(token))
    return {"deleted": True, "token": token}