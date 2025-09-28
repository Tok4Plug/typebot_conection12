# db.py — versão 3.1 (enriquecimento persistido, dedupe estável, payload completo no sync)
import os, asyncio, json, time, hashlib, base64, logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple

import sys
sys.path.append(os.path.dirname(__file__))
import utils  # mantém compat (normalize, dedupe, clamp, etc.)

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean,
    DateTime, Float, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError

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

    event_type = Column(String(50), index=True)   # "Lead" (principal) ou "Subscribe" (apenas histórico)
    route_key = Column(String(50), index=True)    # ex.: "botb", "vip"

    src_url = Column(Text, nullable=True)         # event_source_url/landing_url canônica
    value = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)

    # Armazenamento principal
    user_data = Column(JSONB, nullable=False, default=dict)   # dados do usuário (plain), inclui ip/ua se vier
    custom_data = Column(JSONB, nullable=True, default=dict)  # espelho de UTMs, clids, geo, device extras

    cookies = Column(JSONB, nullable=True)        # cifrados
    device_info = Column(JSONB, nullable=True)    # device/os/browser/platform/url
    session_metadata = Column(JSONB, nullable=True)

    # Estado de envio
    sent = Column(Boolean, default=False, index=True)
    sent_pixels = Column(JSONB, nullable=True, default=list)
    event_history = Column(JSONB, nullable=True, default=list)  # lista de eventos ({event_type, event_id, event_time, status})

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

# ==============================
# Init
# ==============================
def init_db():
    if not engine:
        return
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ DB inicializado e tabelas sincronizadas")
    except SQLAlchemyError as e:
        logger.error(f"Erro init DB: {e}")

# ==============================
# Helpers de espelhamento/merge
# ==============================
_UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
_CLID_KEYS = ("gclid", "wbraid", "gbraid", "fbclid", "cid", "_fbp", "_fbc")
_GEO_KEYS = ("country", "state", "city", "zip")
_DEVICE_TOP_KEYS = ("device", "os", "browser", "platform")
_URL_KEYS = ("event_source_url", "landing_url", "src_url")

def _mirror_into_custom(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Copia UTMs, clids, geo e device extras para custom_data, para persistir enriquecimento completo
    sem exigir migração de schema. Não sobrescreve valores já existentes em custom_data.
    """
    base = dict(data.get("custom_data") or {})
    # UTMs
    for k in _UTM_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # Click IDs
    for k in _CLID_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # GEO (também aceita nested geo)
    geo = data.get("geo") or {}
    for k in _GEO_KEYS:
        v = data.get(k, None)
        if v is None:
            v = geo.get("region" if k == "state" else k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # Device extras no topo (caso venham fora de device_info)
    for k in _DEVICE_TOP_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    # URLs úteis
    for k in _URL_KEYS:
        v = data.get(k)
        if v is not None and base.get(k) in (None, ""):
            base[k] = v
    return base

def _best_src_url(data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a melhor origem de URL (event_source_url > src_url > landing_url > device_info.url).
    """
    di = data.get("device_info") or {}
    return (
        data.get("event_source_url")
        or data.get("src_url")
        or data.get("landing_url")
        or di.get("url")
    )

def _latest_event_time(row: Lead) -> int:
    """
    Recupera o event_time mais recente do histórico; fallback created_at.
    """
    try:
        hist = row.event_history or []
        if hist:
            # pega o último com event_time, senão usa created_at
            for ev in reversed(hist):
                et = ev.get("event_time")
                if isinstance(et, (int, float)) and et > 0:
                    return int(et)
        return int(row.created_at.timestamp())
    except Exception:
        return int(row.created_at.timestamp())

def _assemble_lead_payload(row: Lead) -> Dict[str, Any]:
    """
    Reconstrói o LEAD COMPLETO para envio aos pixels (mesma estrutura do bot/bridge).
    Esse era o ponto que derrubava o score quando reprocessava pendentes.
    """
    user = row.user_data or {}
    custom = row.custom_data or {}
    cookies = _safe_dict(row.cookies or {}, decrypt=True)
    di = row.device_info or {}

    # UTMs/CLIDs/geo/url reproduzidos do custom_data
    lead: Dict[str, Any] = {
        "telegram_id": row.telegram_id,
        "event_key": row.event_key,
        "event_type": row.event_type or "Lead",
        "event_time": _latest_event_time(row),

        # valores de negócio
        "value": row.value,
        "currency": row.currency,

        # cookies descriptografados (já prontos pro Bridge/Bot)
        "cookies": cookies,

        # device_info íntegro
        "device_info": {
            "platform": di.get("platform"),
            "app": di.get("app"),
            "device": di.get("device") or custom.get("device"),
            "os": di.get("os") or custom.get("os"),
            "browser": di.get("browser") or custom.get("browser"),
            "url": di.get("url") or custom.get("event_source_url") or custom.get("landing_url"),
        },

        # session
        "session_metadata": row.session_metadata or {},

        # URLs/UTMs
        "event_source_url": custom.get("event_source_url") or row.src_url,
        "landing_url": custom.get("landing_url"),
        "utm_source": custom.get("utm_source"),
        "utm_medium": custom.get("utm_medium"),
        "utm_campaign": custom.get("utm_campaign"),
        "utm_term": custom.get("utm_term"),
        "utm_content": custom.get("utm_content"),

        # Click IDs (inclui cid e _fbp/_fbc se foram espelhados)
        "gclid": custom.get("gclid"),
        "wbraid": custom.get("wbraid"),
        "gbraid": custom.get("gbraid"),
        "fbclid": custom.get("fbclid"),
        "cid": custom.get("cid"),
        "_fbp": custom.get("_fbp"),
        "_fbc": custom.get("_fbc"),

        # Geo/Localização (se foi espelhado)
        "country": custom.get("country"),
        "state": custom.get("state"),
        "city": custom.get("city"),
        "zip": custom.get("zip"),

        # Repasse de alguns campos úteis do user_data (sem hash)
        "user_data": {
            "email": user.get("email"),
            "phone": user.get("phone"),
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "city": user.get("city") or custom.get("city"),
            "state": user.get("state") or custom.get("state"),
            "zip": user.get("zip") or custom.get("zip"),
            "country": user.get("country") or custom.get("country"),
            "telegram_id": user.get("telegram_id") or row.telegram_id,
            "external_id": user.get("external_id") or row.telegram_id,
            "fbp": user.get("fbp") or custom.get("_fbp"),
            "fbc": user.get("fbc") or custom.get("_fbc"),
            # IP/UA reais (se existirem)
            "ip": user.get("ip"),
            "ua": user.get("ua"),
        },
    }

    # Completa user_agent no topo (útil para alguns parsers/clientes)
    if user.get("ua"):
        lead["user_agent"] = user["ua"]

    # src_url canônico
    lead["src_url"] = row.src_url or _best_src_url(lead)

    return lead

# ==============================
# Priority Score (opcional, para relatórios)
# ==============================
def compute_priority_score(user_data: Dict[str, Any], custom_data: Dict[str, Any]) -> float:
    score = 0.0
    if user_data.get("username"): score += 2
    if user_data.get("first_name"): score += 1
    if user_data.get("premium"): score += 3
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
    - Espelha UTMs/CLIDs/GEO/URL/Device em custom_data (sem schema novo).
    - Guarda histórico de eventos com {event_type, event_id, event_time, status}.
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
                ek = data.get("event_key")
                telegram_id = str(data.get("telegram_id"))
                etype = data.get("event_type") or "Lead"    # padrão A: lead principal
                if not ek or not telegram_id:
                    logger.warning(f"[SAVE_LEAD] evento inválido: ek={ek} tg={telegram_id}")
                    return False

                # event_time/event_id estáveis
                event_time = utils.clamp_event_time(int(data.get("event_time") or utils.now_ts()))
                # NOTE: event_id estável (se vier pronto, usamos)
                event_id = utils.get_or_build_event_id(etype, data, event_time)

                # espelhar enriquecimento no custom_data
                custom = _mirror_into_custom(data)
                # score opcional
                normalized_ud = data.get("user_data") or {"telegram_id": telegram_id}
                custom["priority_score"] = compute_priority_score(normalized_ud, custom)

                # cifrar cookies (se vierem)
                enc_cookies = None
                if isinstance(data.get("cookies"), dict) and data["cookies"]:
                    enc_cookies = {k: _encrypt_value(v) for k, v in data["cookies"].items()}

                lead = session.query(Lead).filter(Lead.event_key == ek).first()
                if lead:
                    logger.info(f"[DB_UPDATE] ek={ek} tipo={etype}")
                    # merge user_data (plain)
                    lead.user_data = {**(lead.user_data or {}), **normalized_ud}
                    # merge custom_data (espelhado)
                    lead.custom_data = {**(lead.custom_data or {}), **custom}
                    lead.event_type = etype
                    # src_url canônico (uma vez definido, só melhora se vier melhor)
                    lead.src_url = _best_src_url({"event_source_url": data.get("event_source_url"),
                                                  "src_url": data.get("src_url") or lead.src_url,
                                                  "landing_url": data.get("landing_url"),
                                                  "device_info": data.get("device_info")})
                    lead.value = data.get("value") if data.get("value") is not None else lead.value
                    lead.currency = data.get("currency") or lead.currency
                    # cookies
                    if enc_cookies:
                        ec = lead.cookies or {}
                        ec.update(enc_cookies)
                        lead.cookies = ec
                    # device_info
                    if data.get("device_info"):
                        di = lead.device_info or {}
                        di.update(data["device_info"])
                        lead.device_info = di
                    # session metadata
                    if data.get("session_metadata"):
                        sm = lead.session_metadata or {}
                        sm.update(data["session_metadata"])
                        lead.session_metadata = sm
                    # event history (dedupe por event_id)
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

                # marcações de tentativa/ok (não setamos sent=True aqui; isso é feito após tentar enviar)
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

    # Import lazy para evitar ciclos
    from bot_gesto.fb_google import send_event_to_all

    processed = 0
    for row in rows:
        try:
            lead_payload = _assemble_lead_payload(row)
            results = await send_event_to_all(lead_payload, et=lead_payload.get("event_type") or "Lead")

            ok = any((isinstance(v, dict) and v.get("ok")) for v in (results or {}).values())
            session = SessionLocal()
            try:
                # refresh row preso ao session de gravação
                obj = session.query(Lead).filter(Lead.id == row.id).first()
                if not obj:
                    # edge case: deletado entre fetch e commit
                    session.close()
                    processed += 1
                    continue

                if ok:
                    obj.sent = True
                    obj.last_sent_at = datetime.now(timezone.utc)
                else:
                    obj.last_attempt_at = datetime.now(timezone.utc)

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