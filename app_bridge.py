# =============================
# app_bridge.py — versão 2.6 estável (auto-sync com bot_gesto)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib, importlib.util
from typing import Optional, Dict, Any, Tuple
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
# Descoberta e import dinâmico do Bot B
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))    # /app (no container)
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
CWD_DIR = os.getcwd()

def _exists(p: Optional[str]) -> bool:
    try:
        return bool(p) and os.path.isdir(p)
    except Exception:
        return False

def _files_ok(d: str) -> bool:
    return os.path.isfile(os.path.join(d, "db.py")) and os.path.isfile(os.path.join(d, "fb_google.py"))

def _import_module_from_file(modname: str, filepath: str):
    spec = importlib.util.spec_from_file_location(modname, filepath)
    if not spec or not spec.loader:
        raise ImportError(f"spec loader inválido para {filepath}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module

IMPORT_DEBUG: Dict[str, Any] = {
    "strategy": None,
    "base_dir": BASE_DIR,
    "parent_dir": PARENT_DIR,
    "cwd": CWD_DIR,
    "candidates": [],
    "chosen_dir": None,
    "db_exists": None,
    "fb_exists": None,
    "errors": [],
}

# 1) Tenta como pacote: from bot_gesto... (correto quando /app está no sys.path e bot_gesto é pacote)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from bot_gesto.db import save_lead  # type: ignore
    from bot_gesto.fb_google import send_event_to_all  # type: ignore
    IMPORT_DEBUG.update({
        "strategy": "package_import_bot_gesto",
        "chosen_dir": os.path.join(BASE_DIR, "bot_gesto"),
        "db_exists": os.path.isfile(os.path.join(BASE_DIR, "bot_gesto", "db.py")),
        "fb_exists": os.path.isfile(os.path.join(BASE_DIR, "bot_gesto", "fb_google.py")),
    })
except Exception as e_pkg1:
    IMPORT_DEBUG["errors"].append(f"package bot_gesto: {e_pkg1}")

    # 2) Tenta como pacote: from typebot_conection.bot_gesto...
    try:
        from typebot_conection.bot_gesto.db import save_lead  # type: ignore
        from typebot_conection.bot_gesto.fb_google import send_event_to_all  # type: ignore
        IMPORT_DEBUG.update({
            "strategy": "package_import_typebot_conection.bot_gesto",
            "chosen_dir": os.path.join(BASE_DIR, "typebot_conection", "bot_gesto"),
            "db_exists": os.path.isfile(os.path.join(BASE_DIR, "typebot_conection", "bot_gesto", "db.py")),
            "fb_exists": os.path.isfile(os.path.join(BASE_DIR, "typebot_conection", "bot_gesto", "fb_google.py")),
        })
    except Exception as e_pkg2:
        IMPORT_DEBUG["errors"].append(f"package typebot_conection.bot_gesto: {e_pkg2}")

        # 3) Fallback: importar por caminho absoluto (aceita qualquer layout)
        candidates = [
            os.path.join(BASE_DIR, "bot_gesto"),
            os.path.join(BASE_DIR, "typebot_conection", "bot_gesto"),
            os.path.join(PARENT_DIR, "bot_gesto"),
            os.path.join(PARENT_DIR, "typebot_conection", "bot_gesto"),
            os.path.join(CWD_DIR, "bot_gesto"),
            os.path.join(CWD_DIR, "typebot_conection", "bot_gesto"),
        ]
        IMPORT_DEBUG["candidates"] = candidates

        chosen_dir: Optional[str] = None
        for d in candidates:
            if _exists(d) and _files_ok(d):
                chosen_dir = d
                break

        if not chosen_dir:
            logger.error(json.dumps({
                "event": "IMPORT_FAIL",
                **IMPORT_DEBUG,
            }))
            raise RuntimeError(
                "❌ Não foi possível localizar a pasta com db.py e fb_google.py "
                "(tente garantir que 'bot_gesto' fique ao lado do app_bridge.py, "
                "ou dentro de 'typebot_conection/bot_gesto')."
            )

        try:
            db_mod = _import_module_from_file("_bridge_db", os.path.join(chosen_dir, "db.py"))
            fb_mod = _import_module_from_file("_bridge_fb_google", os.path.join(chosen_dir, "fb_google.py"))
            save_lead = getattr(db_mod, "save_lead")
            send_event_to_all = getattr(fb_mod, "send_event_to_all")
            IMPORT_DEBUG.update({
                "strategy": "file_import",
                "chosen_dir": chosen_dir,
                "db_exists": True,
                "fb_exists": True,
            })
        except Exception as e_file:
            IMPORT_DEBUG["errors"].append(f"file_import: {e_file}")
            logger.error(json.dumps({
                "event": "IMPORT_FAIL",
                **IMPORT_DEBUG,
            }))
            raise

logger.info(json.dumps({
    "event": "IMPORT_OK",
    **{k: v for k, v in IMPORT_DEBUG.items() if k != "errors"},
}))

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
app = FastAPI(title="Typebot Bridge", version="2.6.0")

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

def _enrich_payload(data: dict, client_ip: Optional[str], client_ua: Optional[str]) -> dict:
    """Aplica enriquecimento e normalização no payload recebido"""
    if client_ip:
        data.setdefault("ip", client_ip)
    if client_ua:
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
        "import_strategy": IMPORT_DEBUG.get("strategy"),
        "bot_dir": IMPORT_DEBUG.get("chosen_dir"),
        "db_present": IMPORT_DEBUG.get("db_exists"),
        "fb_present": IMPORT_DEBUG.get("fb_exists"),
        "errors": IMPORT_DEBUG.get("errors"),
    })
    return status

@app.post("/tb/link")
async def create_deeplink(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
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
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
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