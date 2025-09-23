# =============================
# app_bridge.py — versão 2.7 estável (auto-sync com bot_gesto)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib, importlib.util
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from redis import Redis
from cryptography.fernet import Fernet

# =============================
# Logging JSON
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
# Descoberta/import do bot_gesto
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))    # geralmente /app no container
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
CWD_DIR = os.getcwd()
ENV_DIR = os.getenv("BRIDGE_BOT_DIR")  # opcional: caminho absoluto de bot_gesto
PKG_NAME = "bot_gesto"

def _ls(path: str) -> Dict[str, Any]:
    out = {"path": path, "exists": os.path.isdir(path)}
    try:
        out["entries"] = sorted(os.listdir(path))[:120]
    except Exception as e:
        out["error"] = str(e)
    return out

def _files_ok(d: str) -> bool:
    return os.path.isfile(os.path.join(d, "db.py")) and os.path.isfile(os.path.join(d, "fb_google.py"))

def _import_by_file(dir_path: str):
    def _imp(modname: str, fp: str):
        spec = importlib.util.spec_from_file_location(modname, fp)
        if not spec or not spec.loader:
            raise ImportError(f"spec loader inválido para {fp}")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)  # type: ignore[attr-defined]
        return m
    db_mod = _imp("_bridge_db", os.path.join(dir_path, "db.py"))
    fb_mod = _imp("_bridge_fb_google", os.path.join(dir_path, "fb_google.py"))
    return db_mod.save_lead, fb_mod.send_event_to_all

IMPORT_DEBUG: Dict[str, Any] = {
    "event": "IMPORT_STATUS",
    "base_dir": BASE_DIR,
    "parent_dir": PARENT_DIR,
    "cwd": CWD_DIR,
    "strategy": None,
    "chosen_dir": None,
    "db_exists": None,
    "fb_exists": None,
    "errors": [],
    "ls_base": _ls(BASE_DIR),
}

# 0) se BRIDGE_BOT_DIR foi setado, tenta direto por arquivo
if ENV_DIR and os.path.isdir(ENV_DIR) and _files_ok(ENV_DIR):
    try:
        save_lead, send_event_to_all = _import_by_file(ENV_DIR)
        IMPORT_DEBUG.update({
            "strategy": "env_dir_file_import",
            "chosen_dir": ENV_DIR,
            "db_exists": True,
            "fb_exists": True
        })
    except Exception as e:
        IMPORT_DEBUG["errors"].append(f"env_dir_file_import: {e}")

# 1) tenta importar como pacote local: from bot_gesto import ...
if "save_lead" not in globals():
    if BASE_DIR not in sys.path:
        sys.path.append(BASE_DIR)
    try:
        from bot_gesto.db import save_lead  # type: ignore
        from bot_gesto.fb_google import send_event_to_all  # type: ignore
        IMPORT_DEBUG.update({
            "strategy": "package_import_bot_gesto",
            "chosen_dir": os.path.join(BASE_DIR, PKG_NAME),
            "db_exists": os.path.isfile(os.path.join(BASE_DIR, PKG_NAME, "db.py")),
            "fb_exists": os.path.isfile(os.path.join(BASE_DIR, PKG_NAME, "fb_google.py"))
        })
    except Exception as e:
        IMPORT_DEBUG["errors"].append(f"package_import_bot_gesto: {e}")

# 2) se falhou, tenta importar via arquivo procurando em alguns lugares comuns
if "save_lead" not in globals():
    candidates = [
        os.path.join(BASE_DIR, PKG_NAME),
        os.path.join(BASE_DIR, "typebot_conection", PKG_NAME),
        os.path.join(PARENT_DIR, PKG_NAME),
        os.path.join(PARENT_DIR, "typebot_conection", PKG_NAME),
        os.path.join(CWD_DIR, PKG_NAME),
        os.path.join(CWD_DIR, "typebot_conection", PKG_NAME),
    ]
    chosen = None
    for d in candidates:
        if os.path.isdir(d) and _files_ok(d):
            chosen = d
            break

    if chosen:
        try:
            save_lead, send_event_to_all = _import_by_file(chosen)
            IMPORT_DEBUG.update({
                "strategy": "file_import_candidates",
                "chosen_dir": chosen,
                "db_exists": True,
                "fb_exists": True
            })
        except Exception as e:
            IMPORT_DEBUG["errors"].append(f"file_import_candidates: {e}")

# 3) se ainda falhou, loga e aborta
if "save_lead" not in globals() or "send_event_to_all" not in globals():
    logger.error(json.dumps({
        **IMPORT_DEBUG,
        "ls_app": _ls("/app"),
        "ls_root": _ls("/"),
    }))
    raise RuntimeError(
        "❌ Não foi possível localizar 'db.py' e 'fb_google.py'. "
        "Garanta que a pasta 'bot_gesto' esteja ao lado do app_bridge.py (mesma pasta) "
        "e que o .dockerignore NÃO a exclui. Alternativamente, defina BRIDGE_BOT_DIR com o caminho da pasta."
    )

logger.info(json.dumps({**IMPORT_DEBUG, "event": "IMPORT_OK"}))

# =============================
# ENV do Bridge
# =============================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
TOKEN_TTL_SEC = int(os.getenv("TOKEN_TTL_SEC", "3600"))
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
app = FastAPI(title="Typebot Bridge", version="2.7.0")

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
    _fbp: Optional[str] = None
    _fbc: Optional[str] = None
    fbclid: Optional[str] = None
    gclid: Optional[str] = None
    gbraid: Optional[str] = None
    wbraid: Optional[str] = None
    cid: Optional[str] = None
    landing_url: Optional[str] = None
    event_source_url: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    device: Optional[str] = None
    os: Optional[str] = None
    user_agent: Optional[str] = Field(default=None, alias="user_agent")
    email: Optional[str] = None
    phone: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None
    value: Optional[float] = None
    currency: Optional[str] = None
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
    if client_ip:
        data.setdefault("ip", client_ip)
    if client_ua:
        data.setdefault("user_agent", data.get("user_agent") or client_ua)
    data.setdefault("ts", int(time.time()))

    fbclid = data.get("fbclid")
    if fbclid and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{fbclid}"
    if not data.get("_fbp"):
        data["_fbp"] = f"fb.1.{int(time.time())}.{secrets.randbelow(999_999_999)}"

    if not data.get("cid") and data.get("gclid"):
        data["cid"] = f"gclid.{data['gclid']}"

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
        "ls_base": IMPORT_DEBUG.get("ls_base"),
        "notes": IMPORT_DEBUG.get("errors"),
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
    _auth_guard(x_api_key)

    client_ip = req.client.host if req.client else None
    client_ua = req.headers.get("user-agent")

    data = body.dict(by_alias=True, exclude_none=True)
    enriched = _enrich_payload(data, client_ip, client_ua)

    asyncio.create_task(save_lead(enriched))
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