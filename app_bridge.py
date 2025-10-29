# app_bridge.py — v5.0.0
# Bridge otimizado: timeouts, fallback Redis->in-memory, persistência fbp/fbc, safe background tasks
import os
import sys
import json
import time
import secrets
import logging
import asyncio
import base64
import hashlib
import importlib.util
from typing import Optional, Dict, Any, List, Tuple, Callable
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic import ConfigDict
from redis import Redis, RedisError
from cryptography.fernet import Fernet
from fastapi.responses import RedirectResponse, JSONResponse
from concurrent.futures import ThreadPoolExecutor

# -----------------------
# JSON logger
# -----------------------
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
        return json.dumps(log, ensure_ascii=False)

LOG_LEVEL = os.getenv("BRIDGE_LOG_LEVEL", "INFO")
logger = logging.getLogger("bridge")
logger.setLevel(LOG_LEVEL)
_ch = logging.StreamHandler()
_ch.setFormatter(JSONFormatter())
logger.handlers = []
logger.addHandler(_ch)

# -----------------------
# discovery (mantive tua lógica)
# -----------------------
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

# try package imports first (keeps compatibility)
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
            IMPORT_INFO["errors"].append(f"arquivos não encontrados em {chosen}")
    else:
        IMPORT_INFO["errors"].append("nenhuma pasta candidata com db.py e fb_google.py foi encontrada")

if save_lead is None or send_event_to_all is None:
    logger.error(json.dumps({"event": "IMPORT_FAIL", **IMPORT_INFO}))
    raise RuntimeError("❌ Não foi possível localizar 'save_lead' e 'send_event_to_all'.")

logger.info(json.dumps({"event": "IMPORT_OK", **{k: v for k, v in IMPORT_INFO.items() if k != 'errors'}}))

# -----------------------
# ENV / tunables
# -----------------------
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_USERNAME    = os.getenv("BOT_USERNAME", "").lstrip("@")
TOKEN_TTL_SEC   = int(os.getenv("TOKEN_TTL_SEC", "3600"))
BRIDGE_API_KEY  = os.getenv("BRIDGE_API_KEY", "")
BRIDGE_TOKEN    = os.getenv("BRIDGE_TOKEN", "")
ALLOWED_ORIGINS = [o.strip() for o in (os.getenv("ALLOWED_ORIGINS", "") or "").split(",") if o.strip()]
PORT            = int(os.getenv("PORT", "8080"))

GEOIP_DB_PATH   = os.getenv("GEOIP_DB_PATH", "")
USE_USER_AGENTS = os.getenv("USE_USER_AGENTS", "1") == "1"

CONSENT_HEADER_KEYS = [h.strip() for h in (os.getenv("CONSENT_HEADER_KEYS", "X-Consent,X-TCF-String,TCF-String,GDPR-Consent") or "").split(",") if h.strip()]

# Timeouts / concurrency tunables
BRIDGE_TASK_TIMEOUT = float(os.getenv("BRIDGE_TASK_TIMEOUT", "2"))   # segundos para tarefas background críticas (<=2s recommended)
BRIDGE_TASK_RETRY = int(os.getenv("BRIDGE_TASK_RETRY", "1"))         # retries for background tasks
BRIDGE_MAX_WORKERS = int(os.getenv("BRIDGE_MAX_WORKERS", "8"))       # threadpool workers for to_thread
BRIDGE_SEMAPHORE_CONCURRENCY = int(os.getenv("BRIDGE_SEMAPHORE_CONCURRENCY", "64"))
BRIDGE_PERSIST_FBC_FBP = os.getenv("BRIDGE_PERSIST_FBC_FBP", "1") == "1"  # persist _fbp/_fbc to redis TTL

# Crypto
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
fernet = None
if CRYPTO_KEY:
    derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
    fernet = Fernet(derived)
    logger.info(json.dumps({"event":"CRYPTO_ENABLED"}))

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
    "task_timeout": BRIDGE_TASK_TIMEOUT,
    "task_retry": BRIDGE_TASK_RETRY,
    "persist_fbp_fbc": BRIDGE_PERSIST_FBC_FBP,
}))

if not BOT_USERNAME:
    raise RuntimeError("BOT_USERNAME não configurado")

# -----------------------
# Redis client with cautious wrapper + in-memory fallback
# -----------------------
_redis_client: Optional[Redis] = None
_redis_available = False

try:
    _redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    # quick ping
    _redis_client.ping()
    _redis_available = True
    logger.info(json.dumps({"event": "REDIS_OK", "url_masked": _mask(REDIS_URL)}))
except Exception as e:
    _redis_client = None
    _redis_available = False
    logger.warning(json.dumps({"event": "REDIS_FAIL", "error": str(e)}))

# in-memory fallback store for tokens and persistence hints
_inmem_store: Dict[str, Tuple[float, str]] = {}        # key -> (expiry_ts, blob_json)
_inmem_lock = asyncio.Lock()

async def _inmem_setex(key: str, ttl: int, blob: str):
    async with _inmem_lock:
        _inmem_store[key] = (time.time() + ttl, blob)
    return True

async def _inmem_get(key: str) -> Optional[str]:
    async with _inmem_lock:
        v = _inmem_store.get(key)
        if not v:
            return None
        exp, blob = v
        if time.time() > exp:
            del _inmem_store[key]
            return None
        return blob

async def _inmem_delete(key: str):
    async with _inmem_lock:
        _inmem_store.pop(key, None)

# attempt to flush in-memory entries to redis when redis becomes available
async def _flush_inmem_to_redis():
    global _redis_available, _redis_client
    if not _redis_client:
        return
    async with _inmem_lock:
        keys = list(_inmem_store.keys())
    for k in keys:
        try:
            async with _inmem_lock:
                tup = _inmem_store.get(k)
                if not tup:
                    continue
                exp, blob = tup
                ttl = max(1, int(exp - time.time()))
            # sync call with small timeout
            try:
                await _run_sync_with_timeout(lambda: _redis_client.setex(k, ttl, blob), timeout=1)
                async with _inmem_lock:
                    _inmem_store.pop(k, None)
                logger.info(json.dumps({"event":"FLUSH_INMEM_TO_REDIS","key":k}))
            except Exception as e:
                logger.warning(json.dumps({"event":"FLUSH_REDIS_FAIL","key":k,"error":str(e)}))
        except Exception as e:
            logger.exception(json.dumps({"event":"FLUSH_INMEM_LOOP_ERR","error":str(e)}))

# ThreadPoolExecutor and semaphore for concurrency control
_executor = ThreadPoolExecutor(max_workers=BRIDGE_MAX_WORKERS)
_task_semaphore = asyncio.Semaphore(BRIDGE_SEMAPHORE_CONCURRENCY)

# -----------------------
# GeoIP & UA parsing (kept)
# -----------------------
_geo_reader = None
if GEOIP_DB_PATH and os.path.exists(GEOIP_DB_PATH):
    try:
        import geoip2.database
        _geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)
        logger.info(json.dumps({"event":"GEOIP_ENABLED"}))
    except Exception as e:
        logger.warning(json.dumps({"event":"GEOIP_FAIL","error":str(e)}))

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

# -----------------------
# FastAPI app + CORS
# -----------------------
app = FastAPI(title="Typebot Bridge", version="5.0.0")
if ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# -----------------------
# util helpers
# -----------------------
def _merge_if_empty(dst: Dict[str, Any], src: Dict[str, Any], keys: List[str]) -> None:
    for k in keys:
        v = src.get(k)
        if v is not None and (dst.get(k) in (None, "")):
            dst[k] = v

def _set_if_blank(d: Dict[str, Any], key: str, value: Optional[str]) -> None:
    if value is None:
        return
    cur = d.get(key)
    if cur is None:
        d[key] = value
        return
    if isinstance(cur, str) and cur.strip() == "":
        d[key] = value

# -----------------------
# Pydantic schema (v2)
# -----------------------
class TBPayload(BaseModel):
    model_config = ConfigDict(
        protected_namespaces=(),
        populate_by_name=True,
        extra="allow"
    )

    fbp: Optional[str] = Field(default=None, alias="_fbp")
    fbc: Optional[str] = Field(default=None, alias="_fbc")
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
    user_agent: Optional[str] = None
    browser: Optional[str] = None
    platform: Optional[str] = None

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
    external_id: Optional[str] = None
    login_id: Optional[str] = None

    consent: Optional[Dict[str, Any]] = None

    event: Optional[str] = None
    event_id: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

# -----------------------
# auth / token helpers
# -----------------------
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
        logger.warning(json.dumps({"event": "AUTH_FAIL", "reason": "token_mismatch"}))
        raise HTTPException(status_code=401, detail="Unauthorized")

def _extract_client_ip(req: Request) -> Optional[str]:
    # Prioritize common proxy headers + Railway custom header
    for h in ("CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For", "Railway-Client-IP", "X-Client-IP"):
        v = req.headers.get(h)
        if v:
            return v.split(",")[0].strip()
    # fallback
    try:
        return req.client.host if req.client else None
    except Exception:
        return None

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

    # derive hints
    ga = out.get("_ga")
    if ga and "GA" in ga and not out.get("cid"):
        try:
            parts = ga.split(".")
            if len(parts) >= 4:
                out["cid_hint"] = f"{parts[-2]}.{parts[-1]}"
        except Exception:
            pass

    if not out.get("gclid"):
        for k in ("_gcl_au", "_gcl_aw", "_gcl_dc"):
            g = out.get(k)
            if g and "." in g:
                try:
                    segs = [s for s in g.split(".") if len(s) >= 8]
                    if segs:
                        out["gclid_hint"] = segs[-1]
                        break
                except Exception:
                    pass

    for lk in ("login_id", "uid", "user_id"):
        if out.get(lk) and not out.get("login_id"):
            out["login_id"] = out[lk]
    return out

# dedupe TTL
_BRIDGE_DEDUPE_TTL = 600

def _bridge_dedupe_mark(event_id: Optional[str]) -> bool:
    if not event_id or (isinstance(event_id, str) and event_id.strip() == ""):
        return True
    try:
        if _redis_client and _redis_available:
            # prefer redis SET NX EX
            ok = _redis_client.set(f"bridge:evid:{event_id}", "1", nx=True, ex=_BRIDGE_DEDUPE_TTL)
            return bool(ok)
        else:
            # in-memory dedupe simple (use redis-like keys)
            key = f"bridge:evid:{event_id}"
            # reuse inmem store
            # store small TTL in in-mem map
            loop = asyncio.get_event_loop()
            # synchronous path: create entry via to_thread? but quick in-memory
            # use _inmem_store
            exp = int(time.time() + _BRIDGE_DEDUPE_TTL)
            # direct write (not awaited) — safe since single-threaded
            _inmem_store[key] = (exp, "1")
            return True
    except Exception:
        return True

# -----------------------
# enrichment
# -----------------------
def _extract_consent(req: Request, data: Dict[str, Any]) -> Dict[str, Any]:
    consent: Dict[str, Any] = dict(data.get("consent") or {})
    for hk in CONSENT_HEADER_KEYS:
        hv = req.headers.get(hk)
        if hv and not consent.get(hk):
            consent[hk] = hv
    ck = _parse_cookies(req.headers.get("cookie"))
    for ck_key in ("euconsent-v2", "OptanonConsent", "CookieConsent", "CONSENT"):
        if ck.get(ck_key) and not consent.get(ck_key):
            consent[ck_key] = ck.get(ck_key)
    dnt = req.headers.get("DNT") or req.headers.get("dnt")
    if dnt is not None and consent.get("dnt") is None:
        consent["dnt"] = dnt
    return consent

def _enrich_payload(data: dict, req: Request) -> dict:
    data = dict(data or {})

    # normalize fbp/fbc
    co_fbp = data.get("_fbp") or data.get("fbp")
    co_fbc = data.get("_fbc") or data.get("fbc")
    if co_fbp:
        data["_fbp"] = co_fbp
        data["fbp"] = co_fbp
    if co_fbc:
        data["_fbc"] = co_fbc
        data["fbc"] = co_fbc

    extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
    for k in ("login_id", "external_id"):
        if not data.get(k) and extra.get(k):
            data[k] = extra[k]

    referer = req.headers.get("Referer") or req.headers.get("referer")
    if not data.get("event_source_url") and referer:
        data["event_source_url"] = referer

    ip = _extract_client_ip(req)
    ua = data.get("user_agent") or req.headers.get("user-agent")
    ck = _parse_cookies(req.headers.get("cookie"))

    _merge_if_empty(data, ck, ["_fbp", "_fbc", "cid", "login_id"])
    if ck.get("cid_hint") and not data.get("cid"):
        data["cid"] = ck["cid_hint"]
    if ck.get("gclid_hint") and not data.get("gclid"):
        data["gclid"] = ck["gclid_hint"]

    if data.get("fbclid") and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{data['fbclid']}"
        data["fbc"] = data["_fbc"]

    if not data.get("cid") and data.get("gclid"):
        data["cid"] = f"gclid.{data['gclid']}"

    _set_if_blank(data, "ip", ip)
    if data.get("ip"):
        geo = geo_lookup(data["ip"])
        if geo:
            data.setdefault("geo", geo)
            data.setdefault("country", data.get("country") or geo.get("country"))
            data.setdefault("city", data.get("city") or geo.get("city"))
            data.setdefault("state", data.get("state") or geo.get("region"))

    ua_info = parse_ua(ua)
    if ua_info:
        _set_if_blank(data, "user_agent", ua_info.get("ua"))
        _merge_if_empty(data, ua_info, ["device", "os", "browser"])

    hdr_login = req.headers.get("X-Login-Id") or req.headers.get("x-login-id")
    if hdr_login and not data.get("login_id"):
        data["login_id"] = hdr_login
    qs = dict(req.query_params) if req.query_params else {}
    if qs.get("login_id") and not data.get("login_id"):
        data["login_id"] = qs.get("login_id")

    if data.get("login_id") and not data.get("external_id"):
        data["external_id"] = data["login_id"]

    data["consent"] = _extract_consent(req, data)
    data.setdefault("ts", int(time.time()))

    logger.info(json.dumps({
        "event": data.get("event"),
        "ip": data.get("ip") or "",
        "has_fbp": bool(data.get("_fbp") or data.get("fbp")),
        "has_fbc": bool(data.get("_fbc") or data.get("fbc")),
        "has_fbclid": bool(data.get("fbclid")),
        "has_gclid": bool(data.get("gclid")),
        "device": data.get("device"),
        "os": data.get("os"),
        "browser": data.get("browser"),
        "consent_keys": list((data.get("consent") or {}).keys()) if data.get("consent") else [],
        "event_id": data.get("event_id"),
    }))

    return data

# -----------------------
# safe running utils (timeouts + retry)
# -----------------------
async def _run_sync_with_timeout(fn: Callable, *args, timeout: float = BRIDGE_TASK_TIMEOUT, **kwargs):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(_executor, lambda: fn(*args, **kwargs)), timeout=timeout)
    except asyncio.TimeoutError:
        raise

async def _run_coro_with_timeout(coro, timeout: float = BRIDGE_TASK_TIMEOUT):
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise

async def _safe_background_runner(fn: Callable, *args, timeout: float = BRIDGE_TASK_TIMEOUT, retries: int = BRIDGE_TASK_RETRY, tag: str = "task"):
    attempt = 0
    exc = None
    while attempt <= retries:
        try:
            attempt += 1
            async with _task_semaphore:
                if asyncio.iscoroutinefunction(fn):
                    coro = fn(*args)
                    return await _run_coro_with_timeout(coro, timeout=timeout)
                else:
                    return await _run_sync_with_timeout(fn, *args, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(json.dumps({"event": "TASK_TIMEOUT", "tag": tag, "attempt": attempt, "timeout": timeout}))
            exc = asyncio.TimeoutError()
        except Exception as e:
            logger.exception(json.dumps({"event": "TASK_ERROR", "tag": tag, "attempt": attempt, "error": str(e)}))
            exc = e
        if attempt <= retries:
            await asyncio.sleep(0.25 * attempt)
    logger.error(json.dumps({"event": "TASK_FAILED", "tag": tag, "attempts": attempt, "error": str(exc)}))
    return None

# -----------------------
# App routes
# -----------------------
@app.get("/health")
async def health():
    # test redis quickly
    rstatus = "disabled"
    try:
        if _redis_client:
            ok = await _run_sync_with_timeout(_redis_client.ping, timeout=1)
            rstatus = "ok" if ok else "error"
        else:
            rstatus = "unavailable"
    except Exception as e:
        rstatus = f"error: {str(e)}"
    return {
        "status": "ok",
        "redis": rstatus,
        "inmem_queue_size": len(_inmem_store),
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
    payload = body.model_dump(by_alias=True, exclude_none=True)
    data = _enrich_payload(payload, req)
    token = _make_token()
    key = _key(token)
    blob = json.dumps(data, ensure_ascii=False)

    # Prefer Redis, else in-memory store; both guarded by timeout
    try:
        if _redis_client and _redis_available:
            try:
                await _run_sync_with_timeout(lambda: _redis_client.setex(key, TOKEN_TTL_SEC, blob), timeout=1.5)
            except Exception as e:
                logger.warning(json.dumps({"event": "REDIS_SETEX_FAIL", "error": str(e)}))
                # fallback to in-memory
                await _inmem_setex(key, TOKEN_TTL_SEC, blob)
        else:
            await _inmem_setex(key, TOKEN_TTL_SEC, blob)
    except Exception as e:
        logger.exception(json.dumps({"event": "REDIS_SETEX_UNHANDLED", "error": str(e)}))
        # still return token (bridge will log)
    # Optionally persist fbp/fbc to redis (quick TTL) to help later reads
    try:
        if BRIDGE_PERSIST_FBC_FBP:
            fbp = data.get("_fbp") or data.get("fbp")
            fbc = data.get("_fbc") or data.get("fbc")
            if (fbp or fbc) and (_redis_client and _redis_available):
                # persist in separate keys (no blocking main flow)
                async def _persist_fbkeys():
                    try:
                        if fbp:
                            _redis_client.setex(f"fbp:{fbp}", 3600, json.dumps({"ts": int(time.time())}))
                        if fbc:
                            _redis_client.setex(f"fbc:{fbc}", 3600, json.dumps({"ts": int(time.time())}))
                    except Exception as e:
                        logger.warning(json.dumps({"event":"PERSIST_FB_FAIL","error":str(e)}))
                asyncio.create_task(_safe_background_runner(_persist_fbkeys, timeout=1.0, tag="persist_fbkeys"))
    except Exception:
        pass

    return {"deep_link": _deep_link(token), "token": token, "expires_in": TOKEN_TTL_SEC}

@app.get("/tb/peek/{token}")
async def peek_token(
    token: str,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    key = _key(token)
    blob = None
    try:
        if _redis_client and _redis_available:
            try:
                blob = await _run_sync_with_timeout(lambda: _redis_client.get(key), timeout=1.0)
            except Exception as e:
                logger.warning(json.dumps({"event":"REDIS_GET_FAIL","error":str(e)}))
                blob = None
        if not blob:
            blob = await _inmem_get(key)
    except Exception as e:
        logger.exception(json.dumps({"event":"PEEK_FAIL","error":str(e)}))
        raise HTTPException(status_code=500, detail="internal error")
    if not blob:
        raise HTTPException(status_code=404, detail="token not found/expired")
    return {"token": token, "payload": json.loads(blob)}

@app.delete("/tb/del/{token}")
async def delete_token(
    token: str,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    _auth_guard(x_api_key, x_bridge_token, authorization)
    key = _key(token)
    try:
        if _redis_client and _redis_available:
            try:
                await _run_sync_with_timeout(lambda: _redis_client.delete(key), timeout=1.0)
            except Exception as e:
                logger.warning(json.dumps({"event":"REDIS_DEL_FAIL","error":str(e)}))
                await _inmem_delete(key)
        else:
            await _inmem_delete(key)
    except Exception as e:
        logger.exception(json.dumps({"event":"DEL_FAIL","error":str(e)}))
        raise HTTPException(status_code=500, detail="internal error")
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
    payload = body.model_dump(by_alias=True, exclude_none=True)
    data = _enrich_payload(payload, req)
    et = (event_type or data.get("event") or "Lead")
    data["event_type"] = et

    if not _bridge_dedupe_mark(data.get("event_id")):
        logger.info(json.dumps({"event":"BRIDGE_DEDUP_SKIP","type":et,"event_id":data.get("event_id")}))
        return {"status":"duplicate","event_id":data.get("event_id")}

    # Fire-and-forget background tasks with safe wrapper
    asyncio.create_task(_safe_background_runner(save_lead, data, timeout=BRIDGE_TASK_TIMEOUT, retries=BRIDGE_TASK_RETRY, tag="save_lead"))
    asyncio.create_task(_safe_background_runner(send_event_to_all, data, timeout=BRIDGE_TASK_TIMEOUT, retries=BRIDGE_TASK_RETRY, tag="send_event_to_all"))

    logger.info(json.dumps({
        "event": "EVENT_SENT",
        "type": et,
        "event_id": data.get("event_id"),
        "telegram_id": data.get("telegram_id"),
        "has_fbp": bool(data.get("_fbp") or data.get("fbp")),
        "has_fbc": bool(data.get("_fbc") or data.get("fbc")),
        "source_url": data.get("event_source_url"),
    }))

    # immediate response to avoid client timeouts
    return {"status": "ok", "saved": True, "event_type": et, "event_id": data.get("event_id")}

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
            for k in (
                "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                "gclid","gbraid","wbraid","fbclid","cid",
                "city","state","zip","country",
                "login_id","external_id","email","phone","event_source_url"
            ):
                if k in req.query_params and base_payload.get(k) in (None, ""):
                    base_payload[k] = req.query_params.get(k)
    except Exception:
        pass

    data = _enrich_payload(base_payload, req)
    token = _make_token()
    key = _key(token)
    blob = json.dumps(data, ensure_ascii=False)

    try:
        if _redis_client and _redis_available:
            try:
                await _run_sync_with_timeout(lambda: _redis_client.setex(key, TOKEN_TTL_SEC, blob), timeout=1.5)
            except Exception as e:
                logger.warning(json.dumps({"event":"REDIS_SETEX_FAIL","error":str(e)}))
                await _inmem_setex(key, TOKEN_TTL_SEC, blob)
        else:
            await _inmem_setex(key, TOKEN_TTL_SEC, blob)
    except Exception as e:
        logger.exception(json.dumps({"event":"REDIS_SETEX_UNHANDLED","error":str(e)}))

    # log minimal
    logger.info(json.dumps({
        "event": "APPLY_REDIRECT",
        "token": token,
        "ip": data.get("ip"),
        "geo": data.get("geo"),
        "has_fbp": bool(data.get("_fbp") or data.get("fbp")),
        "has_fbc": bool(data.get("_fbc") or data.get("fbc")),
    }))

    # redirect to deep link (Telegram)
    return RedirectResponse(url=_deep_link(token))

# -----------------------
# periodic background: try to flush inmem -> redis periodically
# -----------------------
async def _periodic_maintenance():
    while True:
        if _redis_client and not _redis_available:
            try:
                _redis_client.ping()
                # if ping ok, mark available
                logger.info(json.dumps({"event":"REDIS_RECOVERED"}))
                global _redis_available
                _redis_available = True
            except Exception:
                pass
        if _redis_client and _redis_available and _inmem_store:
            try:
                await _flush_inmem_to_redis()
            except Exception:
                pass
        await asyncio.sleep(5)

@app.on_event("startup")
async def _on_startup():
    # spawn maintenance task
    asyncio.create_task(_periodic_maintenance())
    logger.info(json.dumps({"event":"BRIDGE_STARTUP"}))

# End of file