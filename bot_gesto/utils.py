# utils.py — v3.3 (dedupe sólido; user_data avançado; sem dados fake)
# - NÃO cria _fbp nem _fbc "do nada": só usa os que vierem do Bridge/Typebot.
# - fbc só é derivado de fbclid se fbclid existir (formato fb.1.<ts>.<fbclid>).
# - external_id prioriza login_id > external_id > telegram_id > user_id.
# - CEP/ZIP normalizado por país (BR → só dígitos).
# - event_id determinístico e estável em toda a stack.
# - GA4 payload com UTM, device, clids e page_location; client_id consistente.

import os, re, time, hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from urllib.parse import urlparse

# ==============================
# Config (reutiliza ENVs existentes; nada novo obrigatório)
# ==============================
DROP_OLD_DAYS = int(os.getenv("FB_DROP_OLDER_THAN_DAYS", "7"))   # janela máx aceita pelo FB
ACTION_SOURCE = os.getenv("FB_ACTION_SOURCE", "website")         # sug.: "chat" p/ Telegram
EVENT_ID_SALT = os.getenv("EVENT_ID_SALT", "change_me")

SEND_LEAD_ON = (os.getenv("SEND_LEAD_ON", "botb") or "").lower()
SEND_SUBSCRIBE_ON = (os.getenv("SEND_SUBSCRIBE_ON", "vip") or "").lower()

GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
GA4_CLIENT_ID_FALLBACK_PREFIX = os.getenv("GA4_CLIENT_ID_FALLBACK_PREFIX", "tlgrm-")

# ==============================
# Helpers básicos
# ==============================
def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D+", "", s or "")

def now_ts() -> int:
    return int(time.time())

def clamp_event_time(ts: Optional[int]) -> int:
    """
    Mantém event_time dentro da janela aceita pelo Facebook CAPI.
    - Máx. passado: DROP_OLD_DAYS
    - Futuro: corta em "agora"
    """
    now = now_ts()
    base = int(ts) if ts is not None else now
    min_ts = int((datetime.now(timezone.utc) - timedelta(days=DROP_OLD_DAYS)).timestamp())
    if base < min_ts:
        return min_ts
    if base > now:
        return now
    return base

def _sanitize_action_source(src: str) -> str:
    allowed = {
        "website", "chat", "app", "phone_call",
        "email", "physical_store", "system_generated", "other"
    }
    s = _norm(src)
    return s if s in allowed else src

def _first_non_empty(*vals) -> Optional[str]:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def _strip_empty(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out

def _url_or_none(u: Optional[str]) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
    s = u.strip()
    if not s:
        return None
    p = urlparse(s)
    # aceita URL completa (com esquema+host) ou caminhos relativos/absolutos
    if (p.scheme and p.netloc) or s.startswith(("/", "./", "../")):
        return s
    # pode ser algo como "example.com/..." sem esquema — ainda útil para logging
    return s

# ==============================
# Normalizações específicas
# ==============================
def _norm_zip_or_cep(s: Optional[str], country: Optional[str]) -> str:
    """
    Normaliza ZIP/CEP antes do hashing:
    - BR: mantém só dígitos (não inventa CEP).
    - Outros: trim + lowercase.
    """
    if not s:
        return ""
    ctry = _norm(country)
    if ctry in ("br", "brazil"):
        return _only_digits(s)
    return _norm(s)

def _compute_fbc_from_fbclid(fbclid: Optional[str], ts: Optional[int]) -> Optional[str]:
    """
    Constrói fbc válido a partir de fbclid, se fbclid existir.
    Formato: fb.1.<timestamp>.<fbclid>
    """
    if not fbclid:
        return None
    t = int(ts or now_ts())
    return f"fb.1.{t}.{fbclid}"

# ==============================
# Deduplicação
# ==============================
def build_event_id(event_name: str, lead: Dict[str, Any], event_time: int) -> str:
    """
    ID determinístico para dedup (igual na stack toda).
    Considera: nome do evento, IDs de usuário, clids, cookies e time.
    """
    keys = [
        _norm(str(event_name)),
        _norm(str(lead.get("telegram_id") or "")),
        _norm(str(lead.get("external_id") or "")),
        _norm(str(lead.get("login_id") or "")),
        _norm(str(lead.get("click_id") or "")),

        _norm(str(lead.get("fbclid") or "")),
        _norm(str(lead.get("fbp") or lead.get("_fbp") or "")),
        _norm(str(lead.get("fbc") or lead.get("_fbc") or "")),

        _norm(str(lead.get("gclid") or "")),
        _norm(str(lead.get("gbraid") or "")),
        _norm(str(lead.get("wbraid") or "")),

        str(event_time),
        EVENT_ID_SALT
    ]
    return _sha256("|".join(keys))

def get_or_build_event_id(event_name: str, lead: Dict[str, Any], event_time: int) -> str:
    existing = _norm(str(lead.get("event_id") or ""))
    return existing if existing else build_event_id(event_name, lead, event_time)

# ==============================
# User Data para Facebook (sem dados fake)
# ==============================
def normalize_user_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara user_data para o Facebook CAPI.
    - Hash (SHA256) para campos sensíveis: email, phone, first/last name, city/state/country/zip, external_id.
    - NÃO inventa fbp/fbc/IP/UA. Usa os que vierem no lead/cookies.
    - Se fbc ausente mas houver fbclid, deriva fbc = fb.1.<ts>.<fbclid>.
    - external_id: login_id > external_id > telegram_id > user_id.
    Aceita 'raw' sendo o lead completo ou o próprio bloco 'user_data'.
    """
    if not raw:
        return {}

    ud_src = raw.get("user_data") if isinstance(raw, dict) else None

    def pick(*paths):
        # procura no user_data e depois no topo do lead
        for p in paths:
            if isinstance(p, tuple) and len(p) == 2:
                top, sub = p
                if isinstance(raw.get(top), dict):
                    v = raw[top].get(sub)
                    if v not in (None, ""):
                        return v
            else:
                if ud_src and ud_src.get(p) not in (None, ""):
                    return ud_src.get(p)
                if raw.get(p) not in (None, ""):
                    return raw.get(p)
        return None

    # Contato e nome
    email = _norm(pick("email"))
    phone = _only_digits(pick("phone"))
    fn = _norm(pick("first_name"))
    ln = _norm(pick("last_name"))

    # Localização
    country = _norm(_first_non_empty(pick("country"), (raw.get("geo") or {}).get("country")))
    st = _norm(_first_non_empty(pick("state"), (raw.get("geo") or {}).get("region")))
    ct = _norm(_first_non_empty(pick("city"), (raw.get("geo") or {}).get("city")))
    zip_raw = _first_non_empty(pick("zip"), pick("postal_code"), pick("cep"))
    zp = _norm_zip_or_cep(zip_raw, country)

    # Identidade
    external_id_src = _first_non_empty(
        pick("login_id"),
        pick("external_id"),
        pick("telegram_id"),
        pick("user_id"),
    )
    external_id = _norm(str(external_id_src or ""))

    # clids / cookies
    fbclid = pick("fbclid")
    fbp_val = _first_non_empty(pick("fbp"), raw.get("_fbp"))
    # fbc: usa se fornecido; senão, deriva de fbclid (se houver)
    event_time = clamp_event_time(pick("event_time"))  # robusto para string/int/None
    provided_fbc = _first_non_empty(pick("fbc"), raw.get("_fbc"))
    fbc_val = provided_fbc if provided_fbc else _compute_fbc_from_fbclid(fbclid, event_time)

    # IP & UA (somente reais)
    ip_val = _first_non_empty(pick("ip"), pick("ip_address"))
    ua_val = _first_non_empty(pick("ua"), pick("user_agent"))

    ud: Dict[str, Any] = {}
    if email:      ud["em"] = _sha256(email)
    if phone:      ud["ph"] = _sha256(phone)
    if fn:         ud["fn"] = _sha256(fn)
    if ln:         ud["ln"] = _sha256(ln)
    if country:    ud["country"] = _sha256(country)
    if st:         ud["st"] = _sha256(st)
    if ct:         ud["ct"] = _sha256(ct)
    if zp:         ud["zp"] = _sha256(zp)
    if external_id:ud["external_id"] = _sha256(external_id)

    # Identificadores diretos aceitos pela CAPI (sem hash)
    if fbp_val: ud["fbp"] = fbp_val
    if fbc_val: ud["fbc"] = fbc_val

    # Dados técnicos
    if ip_val: ud["client_ip_address"] = ip_val
    if ua_val: ud["client_user_agent"] = ua_val

    return ud

# ==============================
# Escolha do evento (Lead/Subscribe)
# ==============================
def derive_event_from_route(route_key: Optional[str]) -> Optional[str]:
    """
    Usa SEND_LEAD_ON / SEND_SUBSCRIBE_ON para decidir Lead/Subscribe
    quando a rota possuir essas marcas no nome. Opcional.
    """
    r = (route_key or "").lower()
    if SEND_SUBSCRIBE_ON and SEND_SUBSCRIBE_ON in r:
        return "Subscribe"
    if SEND_LEAD_ON and SEND_LEAD_ON in r:
        return "Lead"
    return None

def should_send_event(event_name: Optional[str]) -> bool:
    if not event_name:
        return False
    return event_name.lower() in ("lead", "subscribe")

# ==============================
# Payload Facebook
# ==============================
def build_fb_payload(pixel_id: str, event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta payload para Facebook CAPI (sem inventar dados).
    - event_time clampado
    - event_id estável (usa existente ou gera determinístico)
    - user_data normalizado (hash sensível; fbc só se real ou derivado de fbclid)
    - custom_data com UTM/device/geo/clids (campos livres para BI)
    """
    raw_time = int(lead.get("event_time") or now_ts())
    etime = clamp_event_time(raw_time)
    evid = get_or_build_event_id(event_name, lead, etime)

    user_data = normalize_user_data(lead.get("user_data") or lead)

    device_info = lead.get("device_info") or {}
    custom_data = {
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,

        # UTM (úteis para BI; FB ignora se não reconhecer)
        "utm_source": lead.get("utm_source"),
        "utm_medium": lead.get("utm_medium"),
        "utm_campaign": lead.get("utm_campaign"),
        "utm_term": lead.get("utm_term"),
        "utm_content": lead.get("utm_content"),

        # Device/Plataforma
        "device": device_info.get("device") or lead.get("device"),
        "os": device_info.get("os") or lead.get("os"),
        "browser": device_info.get("browser") or lead.get("browser"),
        "platform": device_info.get("platform") or lead.get("platform"),

        # Localidade (para BI; o FB usa os hashes em user_data)
        "city": lead.get("city") or (lead.get("geo") or {}).get("city"),
        "state": lead.get("state") or (lead.get("geo") or {}).get("region"),
        "country": lead.get("country") or (lead.get("geo") or {}).get("country"),

        # IDs de clique para análise
        "fbclid": lead.get("fbclid"),
        "gclid": lead.get("gclid"),
        "gbraid": lead.get("gbraid"),
        "wbraid": lead.get("wbraid"),
        "click_id": lead.get("click_id"),

        # login_id (custom, para BI; o oficial é external_id no user_data)
        "login_id": lead.get("login_id"),

        # CEP/ZIP bruto (para auditoria/BI)
        "zip_raw": _first_non_empty(lead.get("zip"), lead.get("postal_code"), lead.get("cep")),
    }
    custom_data = _strip_empty(custom_data)

    event_source_url = _first_non_empty(
        lead.get("event_source_url"),
        lead.get("src_url"),
        lead.get("landing_url"),
        device_info.get("url"),
    )
    event_source_url = _url_or_none(event_source_url)

    action_source = _sanitize_action_source(ACTION_SOURCE)

    return {
        "data": [{
            "event_name": event_name,
            "event_time": etime,
            "event_id": evid,
            "action_source": action_source,
            "event_source_url": event_source_url,
            "user_data": user_data,
            "custom_data": custom_data
        }]
    }

# ==============================
# Payload Google GA4 (Measurement Protocol)
# ==============================
def to_ga4_event_name(event_name: str) -> str:
    e = (event_name or "").lower()
    if e == "lead":
        return "generate_lead"
    if e == "subscribe":
        return "subscribe"
    return e

def build_ga4_payload(event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Payload GA4 (sem dados fake).
    - client_id: gclid > client_id > cid > fallback prefix + telegram_id/external_id
    - user_id: telegram_id > external_id > login_id (quando existir)
    - page_location: event_source_url/landing_url/device_info.url
    - inclui UTM, device, clids e engagement_time_msec=1
    """
    device_info = lead.get("device_info") or {}

    client_id = (
        lead.get("gclid")
        or lead.get("client_id")
        or lead.get("cid")
        or (GA4_CLIENT_ID_FALLBACK_PREFIX + str(lead.get("telegram_id") or lead.get("external_id") or "anon"))
    )
    user_id = str(lead.get("telegram_id") or lead.get("external_id") or lead.get("login_id") or "")

    page_location = _first_non_empty(
        lead.get("event_source_url"),
        lead.get("landing_url"),
        device_info.get("url"),
    )

    params = {
        # UTM
        "source": lead.get("utm_source"),
        "medium": lead.get("utm_medium"),
        "campaign": lead.get("utm_campaign"),
        "term": lead.get("utm_term"),
        "content": lead.get("utm_content"),

        # Página / origem
        "page_location": page_location,

        # Valor
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,

        # Device
        "device": device_info.get("device") or lead.get("device"),
        "os": device_info.get("os") or lead.get("os"),
        "browser": device_info.get("browser") or lead.get("browser"),
        "platform": device_info.get("platform") or lead.get("platform"),

        # Clids para análise
        "fbclid": lead.get("fbclid"),
        "gclid": lead.get("gclid"),
        "gbraid": lead.get("gbraid"),
        "wbraid": lead.get("wbraid"),

        # Localização (custom)
        "city": lead.get("city") or (lead.get("geo") or {}).get("city"),
        "region": lead.get("state") or (lead.get("geo") or {}).get("region"),
        "country": lead.get("country") or (lead.get("geo") or {}).get("country"),
        "postal_code": _first_non_empty(lead.get("zip"), lead.get("postal_code"), lead.get("cep")),

        # boa prática: mínimo tempo de engajamento
        "engagement_time_msec": 1,
    }
    params = _strip_empty(params)

    payload: Dict[str, Any] = {
        "client_id": str(client_id),
        "events": [{
            "name": to_ga4_event_name(event_name),
            "params": params
        }]
    }
    if user_id:
        payload["user_id"] = user_id

    return payload