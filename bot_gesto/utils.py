# utils.py — versão 2.1 avançada (sincronizado com bridge/bot/fb_google, enriquecimento Lead+Subscribe)
import os, re, time, hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

# ==============================
# Config
# ==============================
DROP_OLD_DAYS = int(os.getenv("FB_DROP_OLDER_THAN_DAYS", "7"))
ACTION_SOURCE = os.getenv("FB_ACTION_SOURCE", "website")
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

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def now_ts() -> int:
    return int(time.time())

def clamp_event_time(ts: int) -> int:
    """Mantém event_time dentro da janela de 7 dias aceita pelo Facebook."""
    if not ts:
        return now_ts()
    min_ts = int((datetime.now(timezone.utc) - timedelta(days=DROP_OLD_DAYS - 1)).timestamp())
    return max(ts, min_ts)

# ==============================
# Deduplicação
# ==============================
def build_event_id(event_name: str, lead: Dict[str, Any], event_time: int) -> str:
    """
    Cria um ID único e determinístico para deduplicar eventos no Facebook.
    Usa salt fixo para evitar colisões.
    """
    keys = [
        _norm(str(event_name)),
        _norm(str(lead.get("telegram_id") or "")),
        _norm(str(lead.get("external_id") or "")),
        _norm(str(lead.get("click_id") or "")),
        _norm(str(lead.get("fbp") or "")),
        _norm(str(lead.get("fbc") or "")),
        _norm(str(lead.get("gclid") or "")),
        _norm(str(lead.get("gbraid") or "")),
        _norm(str(lead.get("wbraid") or "")),
        str(event_time),
        EVENT_ID_SALT
    ]
    return _sha256("|".join(keys))

# ==============================
# User Data para Facebook
# ==============================
def normalize_user_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara user_data para o Facebook CAPI (hashing SHA256 quando requerido).
    Enriquecido: cobre email, telefone, nome, localização, IDs técnicos.
    """
    if not raw:
        return {}

    email = _norm(raw.get("email"))
    phone = _only_digits(raw.get("phone"))
    fn = _norm(raw.get("first_name"))
    ln = _norm(raw.get("last_name"))
    country = _norm(raw.get("country"))
    st = _norm(raw.get("state"))
    ct = _norm(raw.get("city"))
    zp = _norm(raw.get("zip"))
    external_id = _norm(str(raw.get("external_id") or raw.get("telegram_id") or ""))

    ud = {}
    if email: ud["em"] = _sha256(email)
    if phone: ud["ph"] = _sha256(phone)
    if fn:    ud["fn"] = _sha256(fn)
    if ln:    ud["ln"] = _sha256(ln)
    if country: ud["country"] = _sha256(country)
    if st:      ud["st"] = _sha256(st)
    if ct:      ud["ct"] = _sha256(ct)
    if zp:      ud["zp"] = _sha256(zp)
    if external_id: ud["external_id"] = _sha256(external_id)

    # Identificadores diretos
    if raw.get("fbp"): ud["fbp"] = raw.get("fbp")
    if raw.get("fbc"): ud["fbc"] = raw.get("fbc")

    # Dados técnicos
    if raw.get("ip"): ud["client_ip_address"] = raw.get("ip")
    if raw.get("ua"): ud["client_user_agent"] = raw.get("ua")

    return ud

# ==============================
# Escolha do evento (Lead/Subscribe)
# ==============================
def derive_event_from_route(route_key: str) -> str | None:
    """
    Decide dinamicamente se a rota representa Lead ou Subscribe,
    com base em SEND_LEAD_ON / SEND_SUBSCRIBE_ON.
    """
    r = (route_key or "").lower()
    if SEND_SUBSCRIBE_ON and SEND_SUBSCRIBE_ON in r:
        return "Subscribe"
    if SEND_LEAD_ON and SEND_LEAD_ON in r:
        return "Lead"
    return None

def should_send_event(event_name: str) -> bool:
    """
    Define se evento deve ser enviado ao pixel.
    """
    if not event_name:
        return False
    e = event_name.lower()
    return e in ("lead", "subscribe")

# ==============================
# Payload Facebook
# ==============================
def build_fb_payload(pixel_id: str, event_name: str, lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta payload completo para envio ao Facebook CAPI.
    Enriquecimento avançado: inclui dados UTM, device_info e deduplicação.
    """
    raw_time = int(lead.get("event_time") or now_ts())
    etime = clamp_event_time(raw_time)
    evid = build_event_id(event_name, lead, etime)

    # Normalização user_data
    user_data = normalize_user_data(lead.get("user_data") or lead)

    # Enriquecimento custom_data
    custom_data = {
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,
        "utm_source": lead.get("utm_source"),
        "utm_medium": lead.get("utm_medium"),
        "utm_campaign": lead.get("utm_campaign"),
        "utm_term": lead.get("utm_term"),
        "utm_content": lead.get("utm_content"),
        "device": (lead.get("device_info") or {}).get("device") or lead.get("device"),
        "os": (lead.get("device_info") or {}).get("os") or lead.get("os"),
    }
    custom_data = {k: v for k, v in custom_data.items() if v}

    # event_source_url real
    event_source_url = (
        lead.get("event_source_url")
        or lead.get("src_url")
        or lead.get("landing_url")
        or (lead.get("device_info") or {}).get("url")
    )

    # payload final
    return {
        "data": [{
            "event_name": event_name,
            "event_time": etime,
            "event_id": evid,
            "action_source": ACTION_SOURCE,
            "event_source_url": event_source_url,
            "user_data": user_data,
            "custom_data": custom_data
        }]
    }

# ==============================
# Payload Google GA4
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
    Monta payload para envio ao GA4 (Measurement Protocol).
    Enriquecido com UTM, device e suporte a client_id/telegram_id.
    """
    client_id = (
        lead.get("gclid")
        or lead.get("client_id")
        or lead.get("cid")
        or (GA4_CLIENT_ID_FALLBACK_PREFIX + str(lead.get("telegram_id") or lead.get("external_id") or "anon"))
    )
    user_id = str(lead.get("telegram_id") or lead.get("external_id") or "")

    params = {
        "source": lead.get("utm_source"),
        "medium": lead.get("utm_medium"),
        "campaign": lead.get("utm_campaign"),
        "term": lead.get("utm_term"),
        "content": lead.get("utm_content"),
        "event_source_url": lead.get("event_source_url") or lead.get("landing_url"),
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,
        "device": (lead.get("device_info") or {}).get("device") or lead.get("device"),
        "os": (lead.get("device_info") or {}).get("os") or lead.get("os"),
    }
    params = {k: v for k, v in params.items() if v}

    payload = {
        "client_id": str(client_id),
        "events": [{
            "name": to_ga4_event_name(event_name),
            "params": params
        }]
    }
    if user_id:
        payload["user_id"] = user_id

    return payload