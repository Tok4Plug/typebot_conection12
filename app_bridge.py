# =============================
# app_bridge.py — v4.2 (Bridge first-party; AM-friendly; dedupe por event_id; Pydantic v2 fix; sem dados fake)
# =============================
import os, sys, json, time, secrets, logging, asyncio, base64, hashlib, importlib.util
from typing import Optional, Dict, Any, List, Tuple
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic import ConfigDict
from pydantic.alias_generators import AliasChoices
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
        return json.dumps(log, ensure_ascii=False)

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
            IMPORT_INFO["errors"].append(f"arquivos não encontrados em {chosen}")
    else:
        IMPORT_INFO["errors"].append("nenhuma pasta candidata com db.py e fb_google.py foi encontrada")

if save_lead is None or send_event_to_all is None:
    logger.error(json.dumps({"event": "IMPORT_FAIL", **IMPORT_INFO}))
    raise RuntimeError("❌ Não foi possível localizar 'save_lead' e 'send_event_to_all'.")

logger.info(json.dumps({"event": "IMPORT_OK", **{k: v for k, v in IMPORT_INFO.items() if k != 'errors'}}))

# =============================
# ENV Bridge (sem novas ENVs obrigatórias)
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

# Consent (captura apenas; decisão de uso fica a jusante)
CONSENT_HEADER_KEYS = [h.strip() for h in (os.getenv("CONSENT_HEADER_KEYS", "X-Consent,X-TCF-String,TCF-String,GDPR-Consent") or "").split(",") if h.strip()]

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
# GeoIP / UA
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
app = FastAPI(title="Typebot Bridge", version="4.2")
if ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# =============================
# Helpers utilitários locais
# =============================
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

# =============================
# Schemas (compat + sinais AM)
# =============================
class TBPayload(BaseModel):
    # Pydantic v2: aceitar _fbp/_fbc e também fbp/fbc; serializar como _fbp/_fbc
    model_config = ConfigDict(
        protected_namespaces=(),          # permite nomes iniciando com "_"
        populate_by_name=True,
        extra="allow"
    )

    # IDs/cookies/click-ids
    fbp: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("_fbp", "fbp"),
        serialization_alias="_fbp",
    )
    fbc: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("_fbc", "fbc"),
        serialization_alias="_fbc",
    )
    fbclid: Optional[str] = None
    gclid: Optional[str] = None
    gbraid: Optional[str] = None
    wbraid: Optional[str] = None
    cid: Optional[str] = None

    # URLs/tráfego
    landing_url: Optional[str] = None
    event_source_url: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None

    # Device/UA
    device: Optional[str] = None
    os: Optional[str] = None
    user_agent: Optional[str] = None
    browser: Optional[str] = None
    platform: Optional[str] = None

    # User fields
    email: Optional[str] = None
    phone: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None

    # Valores
    value: Optional[float] = None
    currency: Optional[str] = None

    # IDs de usuário
    telegram_id: Optional[str] = None
    external_id: Optional[str] = None
    login_id: Optional[str] = None

    # Consent
    consent: Optional[Dict[str, Any]] = None

    # Controle / Evento
    event: Optional[str] = None
    event_id: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

# =============================
# Helpers (auth, tokens, consent)
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
        logger.warning(json.dumps({"event": "AUTH_FAIL", "reason": "token_mismatch"}))
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

    # Hints úteis (_ga → cid_hint)
    ga = out.get("_ga")
    if ga and "GA" in ga and not out.get("cid"):
        try:
            parts = ga.split(".")
            if len(parts) >= 4:
                out["cid_hint"] = f"{parts[-2]}.{parts[-1]}"
        except Exception:
            pass

    # GCLID em _gcl_*
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

    # login id em cookie (se a página setar)
    for lk in ("login_id", "uid", "user_id"):
        if out.get(lk) and not out.get("login_id"):
            out["login_id"] = out[lk]

    return out

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

# ===== Dedupe leve (bridge) por event_id — evita reprocesso em retries (TTL 10 min) =====
_BRIDGE_DEDUPE_TTL = 600  # 10 minutos

def _bridge_dedupe_mark(event_id: Optional[str]) -> bool:
    """
    True => pode seguir (não visto). False => duplicado recente.
    Sem Redis, sempre retorna True (não bloqueia).
    """
    if not event_id or (isinstance(event_id, str) and event_id.strip() == ""):
        return True
    try:
        if redis.set(f"bridge:evid:{event_id}", "1", nx=True, ex=_BRIDGE_DEDUPE_TTL):
            return True
        return False
    except Exception:
        return True

# =============================
# Enriquecimento first-party
# =============================
def _enrich_payload(data: dict, req: Request) -> dict:
    """
    - IP real + Geo (quando disponível) [não sobrescreve se já veio válido]
    - UA -> device/os/browser (do UA real) [não sobrescreve se já veio válido]
    - NÃO cria _fbp; _fbc só é derivado se houver fbclid
    - Aceita fbp/fbc sem underscore e normaliza para _fbp/_fbc
    - cid/cid_hint/gclid_hint
    - login_id: header/QS/cookies; external_id ← login_id (quando ausente)
    - event_source_url: body > Referer
    - consent snapshot
    - preserva event_id / event (se fornecidos)
    """
    data = dict(data or {})

    # Normalização/coalesce fbp/fbc -> _fbp/_fbc
    co_fbp = data.get("_fbp") or data.get("fbp")
    co_fbc = data.get("_fbc") or data.get("fbc")
    if co_fbp:
        data["_fbp"] = co_fbp
        data["fbp"] = co_fbp
    if co_fbc:
        data["_fbc"] = co_fbc
        data["fbc"] = co_fbc

    # Fold-in do "extra"
    extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
    for k in ("login_id", "external_id"):
        if not data.get(k) and extra.get(k):
            data[k] = extra[k]

    # event_source_url: body > Referer
    referer = req.headers.get("Referer") or req.headers.get("referer")
    if not data.get("event_source_url") and referer:
        data["event_source_url"] = referer

    ip = _extract_client_ip(req)
    ua = data.get("user_agent") or req.headers.get("user-agent")
    ck = _parse_cookies(req.headers.get("cookie"))

    # Cookies -> corpo (sem sobrescrever valores já presentes)
    _merge_if_empty(data, ck, ["_fbp", "_fbc", "cid", "login_id"])
    if ck.get("cid_hint") and not data.get("cid"):
        data["cid"] = ck["cid_hint"]
    if ck.get("gclid_hint") and not data.get("gclid"):
        data["gclid"] = ck["gclid_hint"]

    # _fbc a partir de fbclid (permitido pela Meta)
    if data.get("fbclid") and not data.get("_fbc"):
        data["_fbc"] = f"fb.1.{int(time.time())}.{data['fbclid']}"
        data["fbc"] = data["_fbc"]

    # NÃO criar _fbp se ausente (somente ecoa se já existe)
    if data.get("_fbp") and not data.get("fbp"):
        data["fbp"] = data["_fbp"]

    # CID fallback (GA4)
    if not data.get("cid") and data.get("gclid"):
        data["cid"] = f"gclid.{data['gclid']}"

    # IP/Geo reais — só grava se faltando ou vazio
    _set_if_blank(data, "ip", ip)
    if data.get("ip"):
        geo = geo_lookup(data["ip"])
        if geo:
            data.setdefault("geo", geo)
            data.setdefault("country", data.get("country") or geo.get("country"))
            data.setdefault("city", data.get("city") or geo.get("city"))
            data.setdefault("state", data.get("state") or geo.get("region"))

    # UA/Device reais — só grava se faltando ou vazio
    ua_info = parse_ua(ua)
    if ua_info:
        _set_if_blank(data, "user_agent", ua_info.get("ua"))
        _merge_if_empty(data, ua_info, ["device", "os", "browser"])

    # login_id de header/QS
    hdr_login = req.headers.get("X-Login-Id") or req.headers.get("x-login-id")
    if hdr_login and not data.get("login_id"):
        data["login_id"] = hdr_login
    qs = dict(req.query_params) if req.query_params else {}
    if qs.get("login_id") and not data.get("login_id"):
        data["login_id"] = qs.get("login_id")

    # external_id ← login_id (quando ausente)
    if data.get("login_id") and not data.get("external_id"):
        data["external_id"] = data["login_id"]

    # consent snapshot
    data["consent"] = _extract_consent(req, data)

    # timestamp de recepção
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

async def _maybe_async(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res

# =============================
# App & Rotas
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
    # by_alias=True preserva _fbp/_fbc quando enviados do Typebot
    data = _enrich_payload(body.dict(by_alias=True, exclude_none=True), req)
    token = _make_token()
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(data, ensure_ascii=False))
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
    event_type: Optional[str] = "Lead"   # query override: ?event_type=PageView|ViewContent|Lead|Subscribe...
):
    """
    Recebe eventos do Typebot/web com cookies 1P reais e `event_id` (dedupe CAPI).
    Prioridade de nome do evento: query ?event_type=  > body.event > "Lead".
    Preserva `event_id` para deduplicação com o Pixel.
    """
    _auth_guard(x_api_key, x_bridge_token, authorization)
    payload = body.dict(by_alias=True, exclude_none=True)
    data = _enrich_payload(payload, req)

    # Nome do evento
    et = (event_type or data.get("event") or "Lead")
    data["event_type"] = et  # para downstream

    # Dedupe leve no bridge por event_id (evita reprocesso em retries de rede)
    if not _bridge_dedupe_mark(data.get("event_id")):
        logger.info(json.dumps({
            "event": "BRIDGE_DEDUP_SKIP",
            "type": et,
            "event_id": data.get("event_id")
        }))
        return {"status": "duplicate", "event_id": data.get("event_id")}

    # Persistência + envio (assíncronos)
    asyncio.create_task(_maybe_async(save_lead, data))
    asyncio.create_task(_maybe_async(send_event_to_all, data, et=et))

    logger.info(json.dumps({
        "event": "EVENT_SENT",
        "type": et,
        "event_id": data.get("event_id"),
        "telegram_id": data.get("telegram_id"),
        "has_fbp": bool(data.get("_fbp") or data.get("fbp")),
        "has_fbc": bool(data.get("_fbc") or data.get("fbc")),
        "source_url": data.get("event_source_url"),
    }))

    return {"status": "ok", "saved": True, "event_type": et, "event_id": data.get("event_id")}

@app.post("/webhook")
async def webhook(
    req: Request,
    body: TBPayload,
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False),
    authorization: Optional[str] = Header(default=None),
    x_bridge_token: Optional[str] = Header(default=None, alias="X-Bridge-Token", convert_underscores=False),
):
    # Compat: trata como Lead (ou chame /event?event_type=Lead do Typebot)
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
    # Compat: trata como Lead
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
    """
    Endpoint de conversão direta (sem corpo).
    Extrai QS/cookies/UA/IP → gera deep link Telegram com token efêmero no Redis.
    """
    base_payload: Dict[str, Any] = {"source": "apply"}

    # Espelha QS de forma segura (sem sobrescrever)
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
    redis.setex(_key(token), TOKEN_TTL_SEC, json.dumps(data, ensure_ascii=False))

    logger.info(json.dumps({
        "event": "APPLY_REDIRECT",
        "token": token,
        "ip": data.get("ip"),
        "geo": data.get("geo"),
        "has_fbp": bool(data.get("_fbp") or data.get("fbp")),
        "has_fbc": bool(data.get("_fbc") or data.get("fbc")),
    }))

    return RedirectResponse(url=_deep_link(token))