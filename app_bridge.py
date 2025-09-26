# =============================
# app_bridge.py — v3.2 (Bridge com enriquecimento + logs claros)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib, importlib.util
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from redis import Redis
from cryptography.fernet import Fernet
from fastapi.responses import RedirectResponse

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
# Descoberta dinâmica do bot_gesto
# =============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
CWD_DIR = os.getcwd()
ENV_DIR = os.getenv("BRIDGE_BOT_DIR")

def _ls(path: Optional[str]) -> Dict[str, Any]:
    try:
        if not path:
            return {"path": path, "exists": False}
        entries = []
        for name in sorted(os.listdir(path))[:80]:
            p = os.path.join(path, name)
            entries.append(("d" if os.path.isdir(p) else "f") + ":" + name)
        return {"path": path, "exists": os.path.isdir(path), "entries": entries}
    except Exception as e:
        return {"path": path, "exists": False, "error": str(e)}

def _find_module_file(root: str, name: str) -> Optional[str]:
    for c in [os.path.join(root, f"{name}.py"), os.path.join(root, name, "__init__.py")]:
        if os.path.isfile(c):
            return c
    return None

def _import_from_file(modname: str, filepath: str):
    spec = importlib.util.spec_from_file_location(modname, filepath)
    if not spec or not spec.loader:
        raise ImportError(f"spec loader inválido para {filepath}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module

def _is_bot_dir(d: str) -> bool:
    return bool(_find_module_file(d, "db") and _find_module_file(d, "fb_google"))

def _walk_find_bot(start: str, max_depth: int = 3) -> Optional[str]:
    start = os.path.abspath(start)
    if not os.path.isdir(start):
        return None
    q: List[Tuple[str, int]] = [(start, 0)]
    seen = set()
    while q:
        d, depth = q.pop(0)
        if d in seen:
            continue
        seen.add(d)
        if _is_bot_dir(d):
            return d
        if depth < max_depth:
            try:
                for n in os.listdir(d):
                    p = os.path.join(d, n)
                    if os.path.isdir(p):
                        q.append((p, depth + 1))
            except Exception:
                pass
    return None

IMPORT_INFO: Dict[str, Any] = {
    "strategy": None,
    "base_dir": BASE_DIR,
    "parent_dir": PARENT_DIR,
    "cwd": CWD_DIR,
    "env_dir": ENV_DIR,
    "candidates": [],
    "chosen_dir": None,
    "db_file": None,
    "fb_file": None,
    "errors": [],
    "ls_base": _ls(BASE_DIR),
    "ls_bot_gesto_at_base": _ls(os.path.join(BASE_DIR, "bot_gesto")),
}

for p in [BASE_DIR, PARENT_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

save_lead = None
send_event_to_all = None

# Import como pacote
try:
    import bot_gesto.db as _db1
    import bot_gesto.fb_google as _fb1
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

# Import alternativo
if save_lead is None or send_event_to_all is None:
    try:
        import typebot_conection.bot_gesto.db as _db2
        import typebot_conection.bot_gesto.fb_google as _fb2
        save_lead = getattr(_db2, "save_lead")
        send_event_to_all = getattr(_fb2, "send_event_to_all")
        IMPORT_INFO.update({
            "strategy": "pkg:typebot_conection.bot_gesto",
            "chosen_dir": os.path.join(BASE_DIR, "typebot_conection", "bot_gesto"),
            "db_file": "package:typebot_conection.bot_gesto.db",
            "fb_file": "package:typebot_conection.bot_gesto.fb_google",
        })
    except Exception as e2:
        IMPORT_INFO["errors"].append(f"pkg typebot_conection.bot_gesto: {e2}")

# Import por caminho
if save_lead is None or send_event_to_all is None:
    candidates = [
        ENV_DIR,
        os.path.join(BASE_DIR, "bot_gesto"),
        os.path.join(BASE_DIR, "typebot_conection", "bot_gesto"),
        os.path.join(CWD_DIR, "bot_gesto"),
        os.path.join(PARENT_DIR, "bot_gesto"),
    ]
    IMPORT_INFO["candidates"] = [c for c in candidates if c]

    chosen = None
    for c in IMPORT_INFO["candidates"]:
        if os.path.isdir(c) and _is_bot_dir(c):
            chosen = c
            break
    if not chosen:
        found = _walk_find_bot(BASE_DIR, max_depth=3)
        if found:
            chosen = found

    if chosen:
        db_file = _find_module_file(chosen, "db")
        fb_file = _find_module_file(chosen, "fb_google")
        IMPORT_INFO["chosen_dir"] = chosen
        IMPORT_INFO["db_file"] = db_file
        IMPORT_INFO["fb_file"] = fb_file
        if db_file and fb_file:
            try:
                _db_mod = _import_from_file("_bridge_db", db_file)
                _fb_mod = _import_from_file("_bridge_fb_google", fb_file)
                save_lead = getattr(_db_mod, "save_lead")
                send_event_to_all = getattr(_fb_mod, "send_event_to_all")
                IMPORT_INFO["strategy"] = "file"
            except Exception as e3:
                IMPORT_INFO["errors"].append(f"file import: {e3}")
        else:
            IMPORT_INFO["errors"].append(
                f"arquivos não encontrados em {chosen}"
            )
    else:
        IMPORT_INFO["errors"].append("nenhuma pasta candidata com db.py e fb_google.py foi encontrada")

if save_lead is None or send_event_to_all is None:
    logger.error(json.dumps({
        "event": "IMPORT_FAIL",
        **IMPORT_INFO,
    }))
    raise RuntimeError("❌ Não foi possível localizar 'save_lead' e 'send_event_to_all'.")

logger.info(json.dumps({"event": "IMPORT_OK", **{k: v for k, v in IMPORT_INFO.items() if k != 'errors'}}))

# =============================
# ENV Bridge
# =============================
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_USERNAME    = os.getenv("BOT_USERNAME", "").lstrip("@")
TOKEN_TTL_SEC   = int(os.getenv("TOKEN_TTL_SEC", "3600"))
BRIDGE_API_KEY  = os.getenv("BRIDGE_API_KEY", "")
BRIDGE_TOKEN    = os.getenv("BRIDGE_TOKEN", "")
ALLOWED_ORIGINS = [o.strip() for o in (os.getenv("ALLOWED_ORIGINS", "") or "").split(",") if o.strip()]
PORT            = int(os.getenv("PORT", "8080"))

GEOIP_DB_PATH   = os.getenv("GEOIP_DB_PATH", "")
USE_USER_AGENTS = os.getenv("USE_USER_AGENTS", "1") == "1"

# Crypto
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
fernet = None
if CRYPTO_KEY:
    derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
    fernet = Fernet(derived)
    logger.info("✅ Cripto: Fernet habilitado")

def _mask(v: str) -> str:
    if not v:
        return ""
    if len(v) <= 6:
        return "***"
    return v[:3] + "***" + v[-3:]

logger.info(json.dumps({
    "event": "BRIDGE_CONFIG",
    "has_bridge_api_key": bool(BRIDGE_API_KEY),
    "has_bridge_token": bool(BRIDGE_TOKEN),
    "bridge_api_key_masked": _mask(BRIDGE_API_KEY),
    "bridge_token_masked": _mask(BRIDGE_TOKEN),
    "bot_username_set": bool(BOT_USERNAME),
    "allowed_origins": ALLOWED_ORIGINS,
}))

if not BOT_USERNAME:
    raise RuntimeError("BOT_USERNAME não configurado")

redis = Redis.from_url(REDIS_URL, decode_responses=True)

# =============================
# GeoIP
# =============================
_geo_reader = None
if GEOIP_DB_PATH and os.path.exists(GEOIP_DB_PATH):
    try:
        import geoip2.database
        _geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)
        logger.info("🌎 GeoIP habilitado")
    except Exception as e:
        logger.warning(f"⚠️ GeoIP indisponível: {e}")

def geo_lookup(ip: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not ip or not _geo_reader:
        return out
    try:
        r = _geo_reader.city(ip)
        out = {
            "ip": ip,
            "country": r.country.iso_code if r.country else None,
            "country_name": r.country.name if r.country else None,
            "region": r.subdivisions[0].name if r.subdivisions else None,
            "city": r.city.name if r.city else None,
            "lat": r.location.latitude if r.location else None,
            "lon": r.location.longitude if r.location else None,
            "timezone": r.location.time_zone if r.location else None,
        }
    except Exception:
        pass
    return out

def parse_ua(ua: Optional[str]) -> Dict[str, Any]:
    if not ua:
        return {}
    if USE_USER_AGENTS:
        try:
            from user_agents import parse as ua_parse
            u = ua_parse(ua)
            return {
                "ua": ua,
                "device": "mobile" if u.is_mobile else "tablet" if u.is_tablet else "pc" if u.is_pc else None,
                "os": str(u.os) or None,
                "browser": str(u.browser) or None,
            }
        except Exception:
            return {"ua": ua}
    return {"ua": ua}

# =============================
# App FastAPI
# =============================
app = FastAPI(title="Typebot Bridge", version="3.2")
if ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
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
    telegram_id: Optional[str] = None
    event: Optional[str] = None
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

def _pick_effective_token() -> Optional[str]:
    return BRIDGE_API_KEY or BRIDGE_TOKEN or None

def _parse_authorization(header_val: Optional[str]) -> Optional[str]:
    if not header_val:
        return None
    parts = header_val.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None

def _auth_guard(
    x_api_key: Optional[str],
    x_bridge_token: Optional[str],
    authorization: Optional[str],
):
    expected = _pick_effective_token()
    if not expected:
        return
    bearer = _parse_authorization(authorization)
    supplied = x_api_key or x_bridge_token or bearer
    if supplied != expected:
        logger.warning(json.dumps({
            "event": "AUTH_FAIL",
            "reason": "token_mismatch",
        }))
        raise HTTPException(status_code=401, detail="Unauthorized")

def _extract_client_ip(req: Request) -> Optional[str]:
    for h in ["CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For"]:
        v = req.headers.get(h)
        if v:
            return v.split(",")[0].strip()
    return req.client.host if req.client else None

def _parse_cookies(header_cookie: Optional[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not header_cookie:
        return out
    try:
        for pair in header_cookie.split(";"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                out[k.strip()] = v.strip()
    except Exception:
        pass
    ga = out.get("_ga")
    if ga and "GA" in ga and not out.get("cid"):
        try:
            parts = ga.split(".")
            if len(parts) >= 4:
                out["cid_hint"] = f"{parts[-2]}.{parts[-1]}"
        except Exception:
            pass
    return out

def _enrich_payload(data: dict, req: Request) -> dict:
    ip = _extract_client_ip(req)
    ua = data.get("user_agent") or req.headers.get("user-agent")
    ck = _parse_cookies(req.headers.get("cookie"))

    if data.get("fbclid") and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{data['fbclid']}"
    if not data.get("_fbp"):
        data["_fbp"] = f"fb.1.{int(time.time())}.{secrets.randbelow(999_999_999)}"
    if ck.get("_fbc") and not data.get("_fbc"):
        data["_fbc"] = ck["_fbc"]
    if ck.get("_fbp") and not data.get("_fbp"):
        data["_fbp"] = ck["_fbp"]

    if not data.get("cid"):
        if data.get("gclid"):
            data["cid"] = f"gclid.{data['gclid']}"
        elif ck.get("cid_hint"):
            data["cid"] = ck["cid_hint"]

    if ip:
        data.setdefault("ip", ip)
        geo = geo_lookup(ip)
        if geo:
            data.setdefault("geo", geo)
            data.setdefault("country", data.get("country") or geo.get("country"))
            data.setdefault("city", data.get("city") or geo.get("city"))
            data.setdefault("state", data.get("state") or geo.get("region"))

    ua_info = parse_ua(ua)
    if ua_info:
        data.setdefault("user_agent", ua_info.get("ua"))
        data.setdefault("device", data.get("device") or ua_info.get("device"))
        data.setdefault("os", data.get("os") or ua_info.get("os"))
        if "browser" in ua_info:
            data.setdefault("browser", ua_info["browser"])

    data.setdefault("ts", int(time.time()))

    if fernet:
        try:
            raw = json.dumps(data).encode()
            data["_encrypted"] = fernet.encrypt(raw).decode()
        except Exception as e:
            logger.warning(f"⚠️ Encryption failed: {e}")

    # LOG DO ENRIQUECIMENTO
    logger.info(json.dumps({
        "event": "ENRICH_OK",
        "ip": data.get("ip"),
        "geo": data.get("geo"),
        "device": data.get("device"),
        "os": data.get("os"),
        "browser": data.get("browser"),
    }))

    return data

async def _maybe_async(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res

# =============================
# Rotas
# =============================
@app.get("/health")
def health():
    try:
        redis.ping(); rstatus = "ok"
    except Exception as e:
        rstatus = f"error: {e}"
    return {
        "status": "ok",
        "redis": rstatus,
        "import": {k: v for k, v in IMPORT_INFO.items() if k != "errors"},
        "ls_chosen": _ls(IMPORT_INFO.get("chosen_dir")),
    }

@app.options("/{full_path:path}")
async def options_ok(full_path: str):
    return {}

@app.post("/tb/link")
async def create_deeplink(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    data = _enrich_payload(body.dict(by_alias=True, exclude_none=True), req)
    token = _make_token()
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(data))
    return {"deep_link": _deep_link(token), "token": token, "expires_in": TOKEN_TTL_SEC}

@app.get("/tb/peek/{token}")
def peek_token(
    token: str,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    blob = redis.get(_key(token))
    if not blob:
        raise HTTPException(status_code=404, detail="token not found/expired")
    return {"token": token, "payload": json.loads(blob)}

@app.delete("/tb/del/{token}")
def delete_token(
    token: str,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    redis.delete(_key(token))
    return {"deleted": True, "token": token}

@app.post("/event")
async def ingest_event(
    req: Request,
    body: TBPayload,
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    event_type: Optional[str] = "Lead"
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    data = _enrich_payload(body.dict(by_alias=True, exclude_none=True), req)

    asyncio.create_task(_maybe_async(save_lead, data))
    asyncio.create_task(_maybe_async(send_event_to_all, data, et=event_type or "Lead"))

    logger.info(json.dumps({
        "event": "EVENT_SENT",
        "type": event_type or "Lead",
        "telegram_id": data.get("telegram_id"),
    }))

    return {"status": "ok", "saved": True, "events": [event_type or "Lead"]}

@app.post("/webhook")
async def webhook(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    return await ingest_event(
        req=req,
        body=body,
        x_bridge_token=x_bridge_token,
        x_api_key=x_api_key,
        authorization=authorization,
        event_type="Lead",
    )

@app.post("/bridge")
async def bridge(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    return await ingest_event(
        req=req,
        body=body,
        x_bridge_token=x_bridge_token,
        x_api_key=x_api_key,
        authorization=authorization,
        event_type="Lead",
    )

@app.get("/apply")
async def apply_redirect(req: Request):
    base_payload: Dict[str, Any] = {"source": "apply"}
    try:
        if req.query_params:
            base_payload["qs"] = dict(req.query_params)
    except Exception:
        pass

    data = _enrich_payload(base_payload, req)
    token = _make_token()
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(data))

    logger.info(json.dumps({
        "event": "APPLY_REDIRECT",
        "token": token,
        "ip": data.get("ip"),
        "geo": data.get("geo")
    }))

    return RedirectResponse(url=_deep_link(token))