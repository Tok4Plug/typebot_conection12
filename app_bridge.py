# =============================
# app_bridge.py — v2.7 estável (auto-sync com bot_gesto, robusto)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib, importlib.util
from typing import Optional, Dict, Any, List
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
# Descoberta + import dinâmico do bot_gesto
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # ex.: /app/typebot_conection
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
CWD_DIR = os.getcwd()
ENV_DIR = os.getenv("BRIDGE_BOT_DIR")  # opcional

def _ls(path: Optional[str]) -> Dict[str, Any]:
    try:
        if not path: 
            return {"path": path, "exists": False}
        return {"path": path, "exists": os.path.isdir(path), "entries": sorted(os.listdir(path))[:80]}
    except Exception as e:
        return {"path": path, "exists": False, "error": str(e)}

def _find_module_file(root: str, name: str) -> Optional[str]:
    candidates = [
        os.path.join(root, f"{name}.py"),
        os.path.join(root, name, "__init__.py"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None

def _import_from_file(modname: str, filepath: str):
    spec = importlib.util.spec_from_file_location(modname, filepath)
    if not spec or not spec.loader:
        raise ImportError(f"spec loader inválido para {filepath}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module

IMPORT_INFO: Dict[str, Any] = {
    "strategy": None,
    "base_dir": BASE_DIR,
    "env_dir": ENV_DIR,
    "candidates": [],
    "chosen_dir": None,
    "db_file": None,
    "fb_file": None,
    "errors": [],
}

# --- Importação ---
save_lead = None
send_event_to_all = None

# 1) Pacote local bot_gesto
try:
    from bot_gesto import db as _db1, fb_google as _fb1
    save_lead = getattr(_db1, "save_lead")
    send_event_to_all = getattr(_fb1, "send_event_to_all")
    IMPORT_INFO.update({
        "strategy": "pkg:bot_gesto",
        "chosen_dir": os.path.join(BASE_DIR, "bot_gesto"),
        "db_file": "package:bot_gesto.db",
        "fb_file": "package:bot_gesto.fb_google",
    })
except Exception as e1:
    IMPORT_INFO["errors"].append(f"pkg bot_gesto: {e1}")

# 2) Pacote typebot_conection.bot_gesto
if not save_lead or not send_event_to_all:
    try:
        if PARENT_DIR not in sys.path:
            sys.path.insert(0, PARENT_DIR)
        from typebot_conection.bot_gesto import db as _db2, fb_google as _fb2
        save_lead = getattr(_db2, "save_lead")
        send_event_to_all = getattr(_fb2, "send_event_to_all")
        IMPORT_INFO.update({
            "strategy": "pkg:typebot_conection.bot_gesto",
            "chosen_dir": os.path.join(BASE_DIR, "bot_gesto"),
            "db_file": "package:typebot_conection.bot_gesto.db",
            "fb_file": "package:typebot_conection.bot_gesto.fb_google",
        })
    except Exception as e2:
        IMPORT_INFO["errors"].append(f"pkg typebot_conection.bot_gesto: {e2}")

# 3) Import direto por arquivo
if not save_lead or not send_event_to_all:
    candidates = [
        ENV_DIR,
        os.path.join(BASE_DIR, "bot_gesto"),
        os.path.join(BASE_DIR, "typebot_conection", "bot_gesto"),
        os.path.join(CWD_DIR, "bot_gesto"),
        os.path.join(CWD_DIR, "typebot_conection", "bot_gesto"),
        os.path.join(PARENT_DIR, "bot_gesto"),
        os.path.join(PARENT_DIR, "typebot_conection", "bot_gesto"),
    ]
    IMPORT_INFO["candidates"] = candidates

    chosen = next((p for p in candidates if p and os.path.isdir(p)), None)
    if chosen:
        db_file = _find_module_file(chosen, "db")
        fb_file = _find_module_file(chosen, "fb_google")
        if db_file and fb_file:
            _db_mod = _import_from_file("_bridge_db", db_file)
            _fb_mod = _import_from_file("_bridge_fb", fb_file)
            save_lead = getattr(_db_mod, "save_lead")
            send_event_to_all = getattr(_fb_mod, "send_event_to_all")
            IMPORT_INFO.update({
                "strategy": "file",
                "chosen_dir": chosen,
                "db_file": db_file,
                "fb_file": fb_file,
            })
        else:
            IMPORT_INFO["errors"].append(f"arquivos não encontrados em {chosen}")
    else:
        IMPORT_INFO["errors"].append("nenhuma pasta candidata encontrada")

if not save_lead or not send_event_to_all:
    logger.error(json.dumps({
        "event": "IMPORT_FAIL",
        **IMPORT_INFO,
        "ls_base": _ls(BASE_DIR),
        "ls_parent": _ls(PARENT_DIR),
    }))
    raise RuntimeError("❌ Não foi possível localizar 'save_lead' e 'send_event_to_all'. "
                       "Garanta que 'bot_gesto' está ao lado do app_bridge.py "
                       "ou defina BRIDGE_BOT_DIR com o caminho correto.")

logger.info(json.dumps({"event": "IMPORT_OK", **{k: v for k,v in IMPORT_INFO.items() if k != "errors"}}))

# =============================
# ENV
# =============================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
TOKEN_TTL_SEC = int(os.getenv("TOKEN_TTL_SEC", "3600"))
BRIDGE_API_KEY = os.getenv("BRIDGE_API_KEY", "")
ALLOWED_ORIGINS = (os.getenv("ALLOWED_ORIGINS", "") or "").split(",")

CRYPTO_KEY = os.getenv("CRYPTO_KEY")
fernet = None
if CRYPTO_KEY:
    derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
    fernet = Fernet(derived)
    logger.info("✅ Crypto habilitado")

if not BOT_USERNAME:
    raise RuntimeError("BOT_USERNAME não configurado")

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# =============================
# App
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
def _make_token(n: int = 16) -> str:
    return secrets.token_urlsafe(n)

def _key(token: str) -> str:
    return f"typebot:{token}"

def _deep_link(token: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=t_{token}"

def _auth_guard(x_api_key: Optional[str]):
    if BRIDGE_API_KEY and (x_api_key != BRIDGE_API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")

def _enrich_payload(data: dict, client_ip: Optional[str], client_ua: Optional[str]) -> dict:
    if client_ip: data.setdefault("ip", client_ip)
    if client_ua: data.setdefault("user_agent", data.get("user_agent") or client_ua)
    data.setdefault("ts", int(time.time()))
    if data.get("fbclid") and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{data['fbclid']}"
    if not data.get("_fbp"):
        data["_fbp"] = f"fb.1.{int(time.time())}.{secrets.randbelow(999999999)}"
    if not data.get("cid") and data.get("gclid"):
        data["cid"] = f"gclid.{data['gclid']}"
    if fernet:
        try:
            raw = json.dumps(data).encode()
            data["_encrypted"] = fernet.encrypt(raw).decode()
        except Exception as e:
            logger.warning(f"⚠️ Encryption failed: {e}")
    return data

# =============================
# Rotas
# =============================
@app.get("/health")
def health():
    try:
        redis.ping(); redis_status = "ok"
    except Exception as e:
        redis_status = f"error: {e}"
    return {
        "status": "ok",
        "redis": redis_status,
        **IMPORT_INFO,
        "ls_bot_dir": _ls(IMPORT_INFO.get("chosen_dir")),
    }

@app.post("/tb/link")
async def create_deeplink(req: Request, body: TBPayload, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key)
    data = _enrich_payload(body.dict(by_alias=True, exclude_none=True), req.client.host if req.client else None, req.headers.get("user-agent"))
    token = _make_token()
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(data))
    return {"deep_link": _deep_link(token), "token": token, "expires_in": TOKEN_TTL_SEC}

@app.post("/webhook")
async def webhook(req: Request, body: TBPayload, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key)
    data = _enrich_payload(body.dict(by_alias=True, exclude_none=True), req.client.host if req.client else None, req.headers.get("user-agent"))
    asyncio.create_task(save_lead(data))
    asyncio.create_task(send_event_to_all(data, et="Lead"))
    return {"status": "ok", "saved": True, "events": ["Lead"]}

@app.get("/tb/peek/{token}")
def peek_token(token: str, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key)
    blob = redis.get(_key(token))
    if not blob: raise HTTPException(status_code=404, detail="token not found/expired")
    return {"token": token, "payload": json.loads(blob)}

@app.delete("/tb/del/{token}")
def delete_token(token: str, x_api_key: Optional[str] = Header(default=None, convert_underscores=False)):
    _auth_guard(x_api_key); redis.delete(_key(token)); return {"deleted": True, "token": token}