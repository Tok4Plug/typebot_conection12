# utils.py — versão 3.1 (dedupe sólido, user_data avançado, fbc correto, login_id, CEP/ZIP, GA4 refinado)
import os, re, time, hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from urllib.parse import urlparse

# ==============================
# Config
# ==============================
DROP_OLD_DAYS = int(os.getenv("FB_DROP_OLDER_THAN_DAYS", "7"))  # janela máx aceita pelo FB
ACTION_SOURCE = os.getenv("FB_ACTION_SOURCE", "website")        # sug.: "chat" p/ Telegram
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
    Mantém event_time dentro da janela aceita pelo Facebook CAPI (<= 7 dias).
    Se vazio, usa agora.
    """
    base = int(ts) if ts else now_ts()
    # mínimo = agora - (DROP_OLD_DAYS-1) dias, para evitar rejeição por "too old"
    min_ts = int((datetime.now(timezone.utc) - timedelta(days=DROP_OLD_DAYS - 1)).timestamp())
    return max(base, min_ts)

def _sanitize_action_source(src: str) -> str:
    """
    Normaliza action_source para valores aceitos pelo FB.
    Mantém padrão caso diferente (não quebra compat).
    """
    allowed = {
        "website", "chat", "app", "phone_call",
        "email", "physical_store", "system_generated", "other"
    }
    s = _norm(src)
    return s if s in allowed else src

def _first_non_empty(*vals) -> Optional[str]:
    """
    Retorna o primeiro valor não vazio/None/'' dentre os argumentos.
    """
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def _strip_empty(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove chaves com None ou strings vazias. Mantém zeros/False.
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out

def _url_or_none(u: Optional[str]) -> Optional[str]:
    """
    Retorna a URL se parecer válida (tem netloc quando há esquema) ou se for um caminho absoluto/relativo utilizável.
    Não tenta corrigir agressivamente; apenas evita mandar lixo.
    """
    if not u or not isinstance(u, str):
        return None
    s = u.strip()
    if not s:
        return None
    p = urlparse(s)
    # aceita: tem esquema + netloc, ou caminho relativo/absoluto
    if (p.scheme and p.netloc) or s.startswith(("/", "./", "../")):
        return s
    # pode ser algo como "example.com/..." sem esquema — ainda útil para logging/atribuição
    return s

# ==============================
# Normalizações específicas
# ==============================
def _norm_zip_or_cep(s: Optional[str], country: Optional[str]) -> str:
    """
    Normaliza ZIP/CEP antes do hashing:
    - BR: mantém só dígitos (idealmente 8, mas não forçamos), sem criar dado fictício.
    - Outros: remove espaços e deixa minúsculo.
    """
    if not s:
        return ""
    ctry = _norm(country)
    if ctry in ("br", "brazil"):
        return _only_digits(s)
    return _norm(s)

def _compute_fbc_from_fbclid(fbclid: Optional[str], ts: Optional[int]) -> Optional[str]:
    """
    Constrói fbc válido a partir de fbclid, se possível.
    Formato: fb.1.<timestamp>.<fbclid>
    """
    if not fbclid:
        return None
    # usa o timestamp do evento (já "clampado") para estabilidade
    t = int(ts or now_ts())
    return f"fb.1.{t}.{fbclid}"

# ==============================
# Deduplicação
# ==============================
def build_event_id(event_name: str, lead: Dict[str, Any], event_time: int) -> str:
    """
    Cria um ID único e determinístico para deduplicar eventos no Facebook.
    Usa salt fixo para evitar colisões.
    Chaves consideradas incluem fbp/fbc/gclid/gbraid/wbraid e click_id.
    """
    keys = [
        _norm(str(event_name)),
        _norm(str(lead.get("telegram_id") or "")),
        _norm(str(lead.get("external_id") or "")),
        _norm(str(lead.get("login_id") or "")),      # novo: login_id entra no dedupe
        _norm(str(lead.get("click_id") or "")),
        _norm(str(lead.get("fbclid") or "")),        # útil para estabilidade
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
    """
    Se já houver event_id persistido/sugerido no lead, usa-o (estabiliza dedupe).
    Caso contrário, gera via build_event_id (lógica original + aprimoramentos).
    """
    existing = _norm(str(lead.get("event_id") or ""))
    return existing if existing else build_event_id(event_name, lead, event_time)

# ==============================
# User Data para Facebook
# ==============================
def normalize_user_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara user_data para o Facebook CAPI (hashing SHA256 quando requerido).
    Enriquecido: cobre email, telefone, nome, localização, IDs técnicos.
    Aceita 'raw' sendo o próprio user_data OU o lead completo.
    - fbc correto a partir de fbclid quando fbc ausente.
    - external_id prioriza: raw.external_id > raw.login_id > raw.telegram_id > raw.user_id
    - CEP/ZIP normalizado por país.
    """
    if not raw:
        return {}

    # Se 'raw' for o lead completo, pode existir um sub-bloco 'user_data'
    ud_src = raw.get("user_data") if isinstance(raw, dict) else None

    def pick(*paths):
        """
        Procura o primeiro valor existente entre chaves simples ou tuplas (para nested):
        Ex.: pick("email", ("user_data","email"))
        """
        for p in paths:
            if isinstance(p, tuple) and len(p) == 2:
                top, sub = p
                if isinstance(raw.get(top), dict):
                    v = raw[top].get(sub)
                    if v not in (None, ""):
                        return v
            else:
                # tenta no user_data primeiro, depois no raw
                if ud_src and ud_src.get(p) not in (None, ""):
                    return ud_src.get(p)
                if raw.get(p) not in (None, ""):
                    return raw.get(p)
        return None

    # Contato / nome
    email = _norm(pick("email"))
    phone = _only_digits(pick("phone"))
    fn = _norm(pick("first_name"))
    ln = _norm(pick("last_name"))

    # Localização (direto ou via geo)
    country = _norm(_first_non_empty(pick("country"), raw.get("geo", {}).get("country")))
    st = _norm(_first_non_empty(pick("state"), raw.get("geo", {}).get("region")))
    ct = _norm(_first_non_empty(pick("city"), raw.get("geo", {}).get("city")))

    # CEP/ZIP pode vir como "zip", "postal_code" ou "cep"
    zip_raw = _first_non_empty(pick("zip"), pick("postal_code"), pick("cep"))
    zp = _norm_zip_or_cep(zip_raw, country)

    # IDs de usuário/login
    external_id_src = _first_non_empty(
        pick("external_id"),
        pick("login_id"),            # novo: login_id mapeado para external_id
        pick("telegram_id"),
        pick("user_id"),
    )
    external_id = _norm(str(external_id_src or ""))

    # clids / cookies
    fbclid = pick("fbclid")
    fbp_val = _first_non_empty(pick("fbp"), raw.get("_fbp"))
    # fbc: usa o fornecido; se ausente, deriva de fbclid
    fbc_val = _first_non_empty(pick("fbc"), raw.get("_fbc")) or _compute_fbc_from_fbclid(fbclid, clamp_event_time(pick("event_time")))

    # IP & UA (variações: ip/ip_address e ua/user_agent)
    ip_val = _first_non_empty(pick("ip"), pick("ip_address"))
    ua_val = _first_non_empty(pick("ua"), pick("user_agent"))

    ud: Dict[str, Any] = {}
    if email: ud["em"] = _sha256(email)
    if phone: ud["ph"] = _sha256(phone)
    if fn:    ud["fn"] = _sha256(fn)
    if ln:    ud["ln"] = _sha256(ln)
    if country: ud["country"] = _sha256(country)
    if st:      ud["st"] = _sha256(st)
    if ct:      ud["ct"] = _sha256(ct)
    if zp:      ud["zp"] = _sha256(zp)
    if external_id: ud["external_id"] = _sha256(external_id)

    # Identificadores diretos aceitos pelo CAPI
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
    Decide dinamicamente se a rota representa Lead ou Subscribe,
    com base em SEND_LEAD_ON / SEND_SUBSCRIBE_ON.
    """
    r = (route_key or "").lower()
    if SEND_SUBSCRIBE_ON and SEND_SUBSCRIBE_ON in r:
        return "Subscribe"
    if SEND_LEAD_ON and SEND_LEAD_ON in r:
        return "Lead"
    return None

def should_send_event(event_name: Optional[str]) -> bool:
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
    Tolerante a variações de chaves; não inventa dados sensíveis.
    """
    # event_time estável (clamp)
    raw_time = int(lead.get("event_time") or now_ts())
    etime = clamp_event_time(raw_time)

    # event_id estável: usa o existente ou gera determinístico
    evid = get_or_build_event_id(event_name, lead, etime)

    # Normalização user_data (aceita lead inteiro ou bloco user_data)
    user_data = normalize_user_data(lead.get("user_data") or lead)

    # Enriquecimento custom_data
    device_info = lead.get("device_info") or {}
    custom_data = {
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,

        # UTM
        "utm_source": lead.get("utm_source"),
        "utm_medium": lead.get("utm_medium"),
        "utm_campaign": lead.get("utm_campaign"),
        "utm_term": lead.get("utm_term"),
        "utm_content": lead.get("utm_content"),

        # Device/Plataforma (não sensível; FB ignora o que não reconhecer)
        "device": device_info.get("device") or lead.get("device"),
        "os": device_info.get("os") or lead.get("os"),
        "browser": device_info.get("browser") or lead.get("browser"),
        "platform": device_info.get("platform") or lead.get("platform"),

        # Localidade (não sensível porque já tipicamente pública no lead/geo)
        "city": lead.get("city") or (lead.get("geo") or {}).get("city"),
        "state": lead.get("state") or (lead.get("geo") or {}).get("region"),
        "country": lead.get("country") or (lead.get("geo") or {}).get("country"),

        # IDs de clique úteis para debug/atribuição (custom fields)
        "fbclid": lead.get("fbclid"),
        "gclid": lead.get("gclid"),
        "gbraid": lead.get("gbraid"),
        "wbraid": lead.get("wbraid"),
        "click_id": lead.get("click_id"),

        # Identificação de login (campo custom; o ID oficial para CAPI é external_id já no user_data)
        "login_id": lead.get("login_id"),

        # CEP/ZIP bruto (para debug/BI; o FB usa o hash no user_data['zp'])
        "zip_raw": _first_non_empty(lead.get("zip"), lead.get("postal_code"), lead.get("cep")),
    }
    custom_data = _strip_empty(custom_data)

    # event_source_url
    event_source_url = _first_non_empty(
        lead.get("event_source_url"),
        lead.get("src_url"),
        lead.get("landing_url"),
        device_info.get("url"),
    )
    event_source_url = _url_or_none(event_source_url)

    # action_source normalizado
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
    Inclui page_location e engagement_time_msec (>0) como boa prática.
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

    # Permite parametros custom seguro; GA4 aceita chaves arbitrárias
    params = {
        # tráfego/utm
        "source": lead.get("utm_source"),
        "medium": lead.get("utm_medium"),
        "campaign": lead.get("utm_campaign"),
        "term": lead.get("utm_term"),
        "content": lead.get("utm_content"),

        # recomendação de página/origem
        "page_location": page_location,

        # valor/currency
        "currency": lead.get("currency") or "BRL",
        "value": lead.get("value") or 0,

        # device
        "device": device_info.get("device") or lead.get("device"),
        "os": device_info.get("os") or lead.get("os"),
        "browser": device_info.get("browser") or lead.get("browser"),
        "platform": device_info.get("platform") or lead.get("platform"),

        # clids úteis p/ análise no GA4
        "fbclid": lead.get("fbclid"),
        "gclid": lead.get("gclid"),
        "gbraid": lead.get("gbraid"),
        "wbraid": lead.get("wbraid"),

        # localização (param custom no GA4 — opcional)
        "city": lead.get("city") or (lead.get("geo") or {}).get("city"),
        "region": lead.get("state") or (lead.get("geo") or {}).get("region"),
        "country": lead.get("country") or (lead.get("geo") or {}).get("country"),
        "postal_code": _first_non_empty(lead.get("zip"), lead.get("postal_code"), lead.get("cep")),

        # mínima duração de engajamento
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