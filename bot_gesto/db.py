# db.py — v4.0
# (enriquecimento persistido; sem dados fake; dedupe estável; AM coverage)
# - NÃO inventa _fbp/_fbc, IP ou UA (apenas persiste o que veio do Bridge/Typebot/Bot).
# - Idempotência por event_key + event_id (histórico sem duplicatas).
# - Espelhamento de UTM/CLIDs/GEO/Device/URL/IDs para custom_data (sem schema novo).
# - Histórico consistente: [{event_type,event_id,event_time,status,ts,delivery,am}].
# - Payload completo no sync/retrofeed (inclui event_id), sem fabricar cookies.
# - Criptografia de cookies (Fernet ou base64).
# - Índices opcionais (Postgres) para performance.
# - Sem ENVs novos obrigatórios.

import os, asyncio, json, time, hashlib, base64, logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import sys
sys.path.append(os.path.dirname(__file__))
import utils  # normalize/dedupe/clamp/helpers

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean,
    DateTime, Float, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import text as _sql_text

# ==============================
# Logging
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [db] %(message)s"
)
logger = logging.getLogger("db")

# ==============================
# Criptografia (cookies)
# ==============================
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
_use_fernet, _fernet = False, None
try:
    if CRYPTO_KEY:
        from cryptography.fernet import Fernet
        derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
        _fernet = Fernet(derived)
        _use_fernet = True
        logger.info("✅ Crypto: Fernet habilitado")
except Exception as e:
    logger.warning(f"⚠️ Fernet indisponível, fallback base64: {e}")

def _encrypt_value(s: Any) -> Any:
    if s is None:
        return s
    try:
        return _fernet.encrypt(str(s).encode()).decode() if _use_fernet else base64.b64encode(str(s).encode()).decode()
    except Exception:
        return base64.b64encode(str(s).encode()).decode()

def _decrypt_value(s: Any) -> Any:
    if s is None:
        return s
    try:
        return _fernet.decrypt(str(s).encode()).decode() if _use_fernet else base64.b64decode(str(s).encode()).decode()
    except Exception:
        return s

def _safe_dict(d: Any, decrypt: bool = False) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in d.items():
        try:
            if decrypt and isinstance(v, str):
                out[k] = _decrypt_value(v)
            else:
                out[k] = v
        except Exception:
            out[k] = v
    return out

# ==============================
# Config DB
# ==============================
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRESQL_URL")
)
engine = create_engine(
    DATABASE_URL,
    pool_size=int(os.getenv("DB_POOL_SIZE", 50)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 150)),
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
) if DATABASE_URL else None

Base = declarative_base()
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False) if engine else None

# ==============================
# Modelo Lead
# ==============================
class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    event_key = Column(String(128), unique=True, nullable=False, index=True)
    telegram_id = Column(String(128), index=True, nullable=False)

    event_type = Column(String(50), index=True)   # "Lead" principal; "Subscribe" apenas histórico
    route_key = Column(String(50), index=True)    # ex.: "botb", "vip"

    src_url = Column(Text, nullable=True)
    value = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)

    user_data = Column(JSONB, nullable=False, default=dict)   # plain (email/phone/ip/ua/etc.)
    custom_data = Column(JSONB, nullable=True, default=dict)  # UTM/CLIDs/GEO/Device/URL/IDs

    cookies = Column(JSONB, nullable=True)        # cifrados
    device_info = Column(JSONB, nullable=True)    # device/os/browser/platform/url
    session_metadata = Column(JSONB, nullable=True)

    sent = Column(Boolean, default=False, index=True)
    sent_pixels = Column(JSONB, nullable=True, default=list)
    event_history = Column(JSONB, nullable=True, default=list)  # [{event_type,event_id,event_time,status,ts,delivery,am}]

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

# ==============================
# Init + Índices opcionais
# ==============================
def _ensure_indexes():
    """Cria índices opcionais (PostgreSQL) para melhorar consultas críticas."""
    if not engine or engine.dialect.name != "postgresql":
        return
    try:
        with engine.begin() as conn:
            conn.execute(_sql_text(
                "CREATE INDEX IF NOT EXISTS idx_leads_telegram_created "
                "ON leads (telegram_id, created_at DESC)"
            ))
            conn.execute(_sql_text(
                "CREATE INDEX IF NOT EXISTS idx_leads_sent_false "
                "ON leads (created_at) WHERE sent = false"
            ))
        logger.info("🗃️ Índices opcionais verificados/criados (PostgreSQL).")
    except Exception as e:
        logger.warning(f"⚠️ Falha ao criar índices opcionais: {e}")

def init_db():
    if not engine:
        return
    try:
        Base.metadata.create_all(bind=engine)
        _ensure_indexes()
        logger.info("✅ DB inicializado e tabelas sincronizadas")
    except SQLAlchemyError as e:
        logger.error(f"Erro init DB: {e}")

# ==============================
# Helpers de espelhamento/merge
# ==============================
_UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
_CLID_KEYS = ("gclid", "wbraid", "gbraid", "fbclid", "cid", "_fbp", "_fbc", "click_id")
_GEO_KEYS = ("country", "state", "city", "zip")
_DEVICE_TOP_KEYS = ("device", "os", "browser", "platform")
_URL_KEYS = ("event_source_url", "landing_url", "src_url")
_ID_KEYS = ("login_id", "external_id")  # IDs auxiliares

def _mirror_into_custom(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Copia UTMs, CLIDs, GEO, Device extras, URL e IDs auxiliares para custom_data.
    Não sobrescreve valores já existentes. Não cria _fbp/_fbc/IP/UA.
    """
    base = dict(data.get("custom_data") or {})

    # UTMs
    for k in _UTM_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # Click IDs / cookies existentes
    for k in _CLID_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # IDs auxiliares
    ud = data.get("user_data") or {}
    for k in _ID_KEYS:
        v = data.get(k) or ud.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # GEO (aceita nested geo)
    geo = data.get("geo") or {}
    for k in _GEO_KEYS:
        v = data.get(k, None)
        if v is None:
            v = geo.get("region" if k == "state" else k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # Device extras
    for k in _DEVICE_TOP_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # URLs
    for k in _URL_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    return base

def _best_src_url(data: Dict[str, Any]) -> Optional[str]:
    """Resolve melhor origem de URL: event_source_url > src_url > landing_url > device_info.url."""
    di = data.get("device_info") or {}
    return (
        data.get("event_source_url")
        or data.get("src_url")
        or data.get("landing_url")
        or di.get("url")
    )

def _latest_event_time(row: Lead) -> int:
    try:
        hist = row.event_history or []
        if hist:
            for ev in reversed(hist):
                et = ev.get("event_time")
                if isinstance(et, (int, float)) and et > 0:
                    return int(et)
        return int(row.created_at.timestamp())
    except Exception:
        return int(row.created_at.timestamp())

def _latest_event_id(row: Lead) -> Optional[str]:
    try:
        hist = row.event_history or []
        for ev in reversed(hist):
            evid = ev.get("event_id")
            if evid:
                return str(evid)
    except Exception:
        pass
    return None

# ------------------------------
# Advanced Matching coverage (snapshot por evento)
# ------------------------------
def _am_coverage_from_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula cobertura de AM (sem logar dados sensíveis)."""
    ud = utils.normalize_user_data(lead.get("user_data") or lead) or {}
    hashed_keys = ["em", "ph", "fn", "ln", "ct", "st", "country", "zp", "external_id"]
    direct_keys = ["fbp", "fbc", "client_ip_address", "client_user_agent"]

    present_hashed = [k for k in hashed_keys if ud.get(k)]
    present_direct = [k for k in direct_keys if ud.get(k)]
    missing_hashed = [k for k in hashed_keys if k not in present_hashed]
    missing_direct = [k for k in direct_keys if k not in present_direct]

    total = len(hashed_keys) + len(direct_keys)
    present_total = len(present_hashed) + len(present_direct)
    coverage = round(100.0 * present_total / total, 2) if total else 0.0

    return {
        "pct": coverage,
        "present_hashed": present_hashed,
        "present_direct": present_direct,
        "missing_hashed": missing_hashed,
        "missing_direct": missing_direct,
        "total_fields": total,
        "present_total": present_total
    }

# ------------------------------
# Compactação de resultados de entrega (FB/GA4)
# ------------------------------
def _compact_result(res: Any) -> Dict[str, Any]:
    """Mantém metadados de entrega não sensíveis."""
    if not isinstance(res, dict):
        return {"ok": False}
    out = {
        "platform": res.get("platform"),
        "event": res.get("event"),
        "ok": res.get("ok"),
        "status": res.get("status"),
    }
    body = res.get("body")
    if isinstance(body, str):
        try:
            j = json.loads(body)
            if isinstance(j, dict):
                if "events_received" in j:
                    out["events_received"] = j.get("events_received")
                if "fbtrace_id" in j:
                    out["fbtrace_id"] = j.get("fbtrace_id")
        except Exception:
            pass
    return out

def _trim_list(lst: List[Any], max_len: int = 20) -> List[Any]:
    if not isinstance(lst, list):
        return []
    if len(lst) <= max_len:
        return lst
    return lst[-max_len:]

# ==============================
# Montagem do payload completo (para sync/retrofeed)
# ==============================
def _assemble_lead_payload(row: Lead) -> Dict[str, Any]:
    """
    Reconstrói o LEAD COMPLETO para envio aos pixels (com event_id).
    NÃO fabrica _fbp/_fbc: apenas repassa se existirem; se só houver fbclid,
    mantemos fbclid e deixamos utils.normalize_user_data derivar fbc no envio.
    """
    user = row.user_data or {}
    custom = row.custom_data or {}
    cookies = _safe_dict(row.cookies or {}, decrypt=True)
    di = row.device_info or {}

    _fbp = custom.get("_fbp") or user.get("fbp")
    _fbc = custom.get("_fbc") or user.get("fbc")

    event_time = _latest_event_time(row)

    lead: Dict[str, Any] = {
        "telegram_id": row.telegram_id,
        "event_key": row.event_key,
        "event_type": row.event_type or "Lead",
        "event_time": event_time,

        "value": row.value,
        "currency": row.currency,

        "cookies": cookies,
        "device_info": {
            "platform": di.get("platform"),
            "app": di.get("app"),
            "device": di.get("device") or custom.get("device"),
            "os": di.get("os") or custom.get("os"),
            "browser": di.get("browser") or custom.get("browser"),
            "url": di.get("url") or custom.get("event_source_url") or custom.get("landing_url"),
        },
        "session_metadata": row.session_metadata or {},

        # URLs/UTMs
        "event_source_url": custom.get("event_source_url") or row.src_url,
        "landing_url": custom.get("landing_url"),
        "utm_source": custom.get("utm_source"),
        "utm_medium": custom.get("utm_medium"),
        "utm_campaign": custom.get("utm_campaign"),
        "utm_term": custom.get("utm_term"),
        "utm_content": custom.get("utm_content"),

        # Click IDs e cookies (só os reais)
        "gclid": custom.get("gclid"),
        "wbraid": custom.get("wbraid"),
        "gbraid": custom.get("gbraid"),
        "fbclid": custom.get("fbclid"),
        "cid": custom.get("cid"),
        "_fbp": _fbp,
        "_fbc": _fbc,
        "click_id": custom.get("click_id"),

        # IDs auxiliares
        "login_id": custom.get("login_id"),
        "external_id": custom.get("external_id") or user.get("external_id"),

        # Geo/Localização
        "country": custom.get("country"),
        "state": custom.get("state"),
        "city": custom.get("city"),
        "zip": custom.get("zip"),
    }

    # Repasse dos campos úteis do user_data (plain)
    ext_id_fallback = (
        user.get("external_id")
        or custom.get("external_id")
        or str(row.telegram_id)
    )
    lead["user_data"] = {
        "email": user.get("email"),
        "phone": user.get("phone"),
        "first_name": user.get("first_name"),
        "last_name": user.get("last_name"),
        "city": user.get("city") or custom.get("city"),
        "state": user.get("state") or custom.get("state"),
        "zip": user.get("zip") or custom.get("zip"),
        "country": user.get("country") or custom.get("country"),
        "telegram_id": user.get("telegram_id") or str(row.telegram_id),
        "external_id": ext_id_fallback,
        "fbp": user.get("fbp") or _fbp,
        "fbc": user.get("fbc") or _fbc,
        "ip": user.get("ip"),
        "ua": user.get("ua"),
        "login_id": user.get("login_id") or custom.get("login_id"),

        # mantemos username/premium se salvos no lead
        "username": user.get("username"),
        "premium": user.get("premium"),
        "first_name_top": user.get("first_name_top"),
        "last_name_top": user.get("last_name_top"),
        "lang": user.get("lang"),
    }

    # user_agent no topo (qualifica parsers a jusante)
    if user.get("ua"):
        lead["user_agent"] = user["ua"]

    # src_url canônico
    lead["src_url"] = row.src_url or _best_src_url(lead)

    # event_id: usa último salvo no histórico; se faltar, gera determinístico
    last_event_id = _latest_event_id(row)
    if last_event_id:
        lead["event_id"] = last_event_id
    else:
        lead["event_id"] = utils.get_or_build_event_id(lead.get("event_type") or "Lead", lead, event_time)

    return lead

# ==============================
# Priority Score (robusto p/ username/premium)
# ==============================
def compute_priority_score(user_data: Dict[str, Any], custom_data: Dict[str, Any]) -> float:
    """
    Pontuação simples para priorização de envios.
    Lê username/premium do user_data (que agora recebe esses campos no save_lead).
    """
    score = 0.0
    if user_data.get("username"): score += 2
    if user_data.get("first_name") or user_data.get("first_name_top"): score += 1
    if user_data.get("premium") is True: score += 3
    if user_data.get("country"): score += 1
    if user_data.get("external_id"): score += 2
    try:
        score += float(custom_data.get("subscribe_count") or 0) * 3
    except Exception:
        pass
    return score

# ==============================
# Save Lead (idempotente e completo)
# ==============================
async def save_lead(data: dict, event_record: Optional[dict] = None, retries: int = 3) -> bool:
    """
    Salva/atualiza um Lead de forma idempotente e persiste TODO o enriquecimento relevante.
    - NÃO cria _fbp/_fbc/IP/UA — só persiste o que veio.
    - Espelha UTMs/CLIDs/GEO/URL/Device/IDs em custom_data.
    - Guarda histórico {event_type,event_id,event_time,status,ts} sem duplicar event_id.
    - Garante user_data com username/premium vindos do topo (para compute_priority_score).
    """
    if not SessionLocal:
        logger.warning("DB desativado - save_lead ignorado")
        return False

    loop = asyncio.get_event_loop()

    def db_sync() -> bool:
        nonlocal retries
        while retries > 0:
            session = SessionLocal()
            try:
                # Identidades
                telegram_id = str(data.get("telegram_id") or "")
                etype = data.get("event_type") or "Lead"
                if not telegram_id:
                    logger.warning("[SAVE_LEAD] evento inválido: telegram_id ausente")
                    return False

                # event_time + event_id estáveis
                event_time = utils.clamp_event_time(int(data.get("event_time") or utils.now_ts()))
                event_id = utils.get_or_build_event_id(etype, data, event_time)

                # event_key (fallback determinístico se não vier)
                ek = data.get("event_key") or f"tg-{telegram_id}-{event_time}"

                # user_data normalizado + inclusão de username/premium do topo
                normalized_ud = dict(data.get("user_data") or {})
                # campos do topo que queremos garantir no user_data:
                for k_top, k_ud in [
                    ("username", "username"),
                    ("premium", "premium"),
                    ("first_name", "first_name_top"),
                    ("last_name", "last_name_top"),
                    ("lang", "lang"),
                ]:
                    v = data.get(k_top)
                    if v not in (None, "") and normalized_ud.get(k_ud) in (None, ""):
                        normalized_ud[k_ud] = v
                # garante telegram_id
                normalized_ud.setdefault("telegram_id", telegram_id)

                # espelhamento para custom_data (somente sinais reais)
                custom = _mirror_into_custom(data)

                # prioridade opcional
                custom["priority_score"] = compute_priority_score(normalized_ud, custom)

                # cifrar cookies (se vierem)
                enc_cookies = None
                if isinstance(data.get("cookies"), dict) and data["cookies"]:
                    enc_cookies = {k: _encrypt_value(v) for k, v in data["cookies"].items()}

                lead = session.query(Lead).filter(Lead.event_key == ek).first()
                if lead:
                    logger.info(f"[DB_UPDATE] ek={ek} tipo={etype}")
                    # merge user_data (sem sobrescrever com vazio)
                    merged_ud = dict(lead.user_data or {})
                    for k, v in (normalized_ud or {}).items():
                        if v not in (None, ""):
                            merged_ud[k] = v
                    lead.user_data = merged_ud

                    # merge custom_data
                    merged_cd = dict(lead.custom_data or {})
                    for k, v in (custom or {}).items():
                        if v not in (None, ""):
                            merged_cd[k] = v
                    lead.custom_data = merged_cd

                    lead.event_type = etype

                    # src_url canônico
                    lead.src_url = _best_src_url({
                        "event_source_url": data.get("event_source_url"),
                        "src_url": data.get("src_url") or lead.src_url,
                        "landing_url": data.get("landing_url"),
                        "device_info": data.get("device_info")
                    })

                    # valor/moeda (apenas se fornecidos)
                    if data.get("value") is not None:
                        lead.value = data.get("value")
                    if data.get("currency"):
                        lead.currency = data.get("currency")

                    # cookies
                    if enc_cookies:
                        ec = lead.cookies or {}
                        ec.update(enc_cookies)
                        lead.cookies = ec

                    # device_info
                    if data.get("device_info"):
                        di = lead.device_info or {}
                        for k, v in (data["device_info"] or {}).items():
                            if v not in (None, ""):
                                di[k] = v
                        lead.device_info = di

                    # session metadata
                    if data.get("session_metadata"):
                        sm = lead.session_metadata or {}
                        for k, v in (data["session_metadata"] or {}).items():
                            if v not in (None, ""):
                                sm[k] = v
                        lead.session_metadata = sm

                    # histórico (dedupe por event_id)
                    eh = lead.event_history or []
                    if not any(ev.get("event_id") == event_id for ev in eh):
                        eh.append({
                            "event_type": etype,
                            "event_id": event_id,
                            "event_time": event_time,
                            "status": (event_record or {}).get("status") or "queued",
                            "ts": datetime.now(timezone.utc).isoformat()
                        })
                        lead.event_history = eh

                else:
                    logger.info(f"[DB_INSERT] Inserindo ek={ek} tipo={etype}")
                    lead = Lead(
                        event_key=ek,
                        telegram_id=telegram_id,
                        event_type=etype,
                        route_key=data.get("route_key"),
                        src_url=_best_src_url(data),
                        value=data.get("value"),
                        currency=data.get("currency"),
                        user_data=normalized_ud,
                        custom_data=custom,
                        cookies=enc_cookies,
                        device_info=data.get("device_info"),
                        session_metadata=data.get("session_metadata"),
                        event_history=[{
                            "event_type": etype,
                            "event_id": event_id,
                            "event_time": event_time,
                            "status": (event_record or {}).get("status") or "queued",
                            "ts": datetime.now(timezone.utc).isoformat()
                        }]
                    )
                    session.add(lead)

                # marcações tentativa/ok
                if event_record:
                    if event_record.get("status") == "success":
                        lead.last_sent_at = datetime.now(timezone.utc)
                        lead.sent = True
                    else:
                        lead.last_attempt_at = datetime.now(timezone.utc)

                session.commit()
                return True

            except OperationalError as e:
                session.rollback()
                retries -= 1
                logger.warning(f"[DB_RETRY] Conexão falhou, tentando novamente ({retries} left) {e}")
                time.sleep(1)
            except Exception as e:
                session.rollback()
                logger.error(f"Erro save_lead: {e}")
                return False
            finally:
                session.close()
        return False

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Recuperar leads não enviados (payload completo!)
# ==============================
async def get_unsent_leads(limit: int = 500) -> List[Dict[str, Any]]:
    if not SessionLocal:
        return []

    loop = asyncio.get_event_loop()

    def db_sync() -> List[Dict[str, Any]]:
        session = SessionLocal()
        try:
            rows = (
                session.query(Lead)
                .filter(Lead.sent == False)
                .order_by(Lead.created_at.asc())
                .limit(limit)
                .all()
            )
            return [_assemble_lead_payload(r) for r in rows]
        except Exception as e:
            logger.error(f"Erro get_unsent_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Recuperar leads históricos (para dashboard/admin)
# ==============================
async def get_historical_leads(limit: int = 50) -> List[Dict[str, Any]]:
    if not SessionLocal:
        return []

    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            rows = (
                session.query(Lead)
                .order_by(Lead.created_at.desc())
                .limit(limit)
                .all()
            )
            out = []
            for r in rows:
                dec_cookies = _safe_dict(r.cookies or {}, decrypt=True) if r.cookies else {}
                out.append({
                    "telegram_id": r.telegram_id,
                    "event_key": r.event_key,
                    "event_type": r.event_type,
                    "route_key": r.route_key,
                    "src_url": r.src_url,
                    "value": r.value,
                    "currency": r.currency,
                    "user_data": r.user_data or {},
                    "custom_data": r.custom_data or {},
                    "cookies": dec_cookies,
                    "device_info": r.device_info or {},
                    "sent": r.sent,
                    "sent_pixels": r.sent_pixels or [],
                    "event_history": r.event_history or [],
                    "created_at": r.created_at.isoformat(),
                    "last_sent_at": r.last_sent_at.isoformat() if r.last_sent_at else None,
                    "last_attempt_at": r.last_attempt_at.isoformat() if r.last_attempt_at else None
                })
            return out
        except Exception as e:
            logger.error(f"Erro get_historical_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Sincronizar leads pendentes (envio real aos pixels)
# ==============================
async def sync_pending_leads(batch_size: int = 20) -> int:
    if not SessionLocal:
        return 0

    loop = asyncio.get_event_loop()

    def fetch_pending() -> List[Lead]:
        session = SessionLocal()
        try:
            return (
                session.query(Lead)
                .filter(Lead.sent == False)
                .order_by(Lead.created_at.asc())
                .limit(batch_size)
                .all()
            )
        except Exception as e:
            logger.error(f"Erro query pending leads: {e}")
            return []
        finally:
            session.close()

    rows = await loop.run_in_executor(None, fetch_pending)
    if not rows:
        return 0

    # Import lazy para evitar ciclos de import
    try:
        from bot_gesto.fb_google import send_event_to_all  # pacote
    except Exception:
        from fb_google import send_event_to_all           # fallback solto

    processed = 0
    for row in rows:
        try:
            lead_payload = _assemble_lead_payload(row)

            # Snapshot de Advanced Matching (antes do envio)
            am = _am_coverage_from_lead(lead_payload)

            results = await send_event_to_all(lead_payload, et=lead_payload.get("event_type") or "Lead")
            ok = any((isinstance(v, dict) and v.get("ok")) for v in (results or {}).values())

            # Compactar resultados por plataforma
            compact_delivery = {k: _compact_result(v) for k, v in (results or {}).items() if isinstance(v, dict)}

            session = SessionLocal()
            try:
                obj = session.query(Lead).filter(Lead.id == row.id).first()
                if not obj:
                    session.close()
                    processed += 1
                    continue

                now_iso = datetime.now(timezone.utc).isoformat()
                if ok:
                    obj.sent = True
                    obj.last_sent_at = datetime.now(timezone.utc)
                else:
                    obj.last_attempt_at = datetime.now(timezone.utc)

                # marca histórico coerente
                ev_id = lead_payload.get("event_id")
                eh = obj.event_history or []
                if ev_id:
                    updated = False
                    for ev in reversed(eh):
                        if ev.get("event_id") == ev_id:
                            ev["status"] = "sent" if ok else "attempted"
                            ev["delivery"] = compact_delivery
                            ev["am"] = am
                            ev["ts_delivered"] = now_iso
                            updated = True
                            break
                    if not updated:
                        eh.append({
                            "event_type": lead_payload.get("event_type") or "Lead",
                            "event_id": ev_id,
                            "event_time": lead_payload.get("event_time"),
                            "status": "sent" if ok else "attempted",
                            "delivery": compact_delivery,
                            "am": am,
                            "ts": now_iso,
                            "ts_delivered": now_iso
                        })
                obj.event_history = eh

                # sent_pixels (append compacto, cap 20)
                sp = obj.sent_pixels or []
                for k, v in compact_delivery.items():
                    sp.append({"platform": v.get("platform") or k, "status": v.get("status"), "ok": v.get("ok"), "ts": now_iso})
                obj.sent_pixels = _trim_list(sp, 20)

                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"[SYNC_PENDING_COMMIT_ERROR] ek={row.event_key} err={e}")
            finally:
                session.close()

            processed += 1

        except Exception as e:
            logger.error(f"[SYNC_PENDING_ERROR] ek={row.event_key} err={e}")

    return processed