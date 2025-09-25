# db.py — versão 3.0 robusta (Lead + Subscribe com redundância e logs detalhados)
import os, asyncio, json, time, hashlib, base64, logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import sys
sys.path.append(os.path.dirname(__file__))
import utils  # mantém compatibilidade

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
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("db")

# ==============================
# Criptografia
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

def _encrypt_value(s: Any) -> str:
    if s is None:
        return s
    try:
        return _fernet.encrypt(str(s).encode()).decode() if _use_fernet else base64.b64encode(str(s).encode()).decode()
    except Exception:
        return base64.b64encode(str(s).encode()).decode()

def _decrypt_value(s: Any) -> str:
    if s is None:
        return s
    try:
        return _fernet.decrypt(str(s).encode()).decode() if _use_fernet else base64.b64decode(str(s).encode()).decode()
    except Exception:
        return s

def _safe_dict(d: Any, decrypt: bool = False) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out = {}
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

    event_type = Column(String(50), index=True)   # Lead ou Subscribe
    route_key = Column(String(50), index=True)    # botb ou vip

    src_url = Column(Text, nullable=True)
    value = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)

    user_data = Column(JSONB, nullable=False, default=dict)
    custom_data = Column(JSONB, nullable=True, default=dict)

    cookies = Column(JSONB, nullable=True)
    device_info = Column(JSONB, nullable=True)
    session_metadata = Column(JSONB, nullable=True)

    sent = Column(Boolean, default=False, index=True)
    sent_pixels = Column(JSONB, nullable=True, default=list)
    event_history = Column(JSONB, nullable=True, default=list)

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
# Priority Score
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
# Save Lead (insert/update explícito para Lead/Subscribe)
# ==============================
async def save_lead(data: dict, event_record: Optional[dict] = None, retries: int = 3) -> bool:
    """
    Salva/atualiza um Lead OU Subscribe de forma idempotente.
    Mantém histórico separado, sem misturar os dois tipos de evento.
    """
    if not SessionLocal:
        logger.warning("DB desativado - save_lead ignorado")
        return False

    loop = asyncio.get_event_loop()

    def db_sync():
        nonlocal retries
        while retries > 0:
            session = SessionLocal()
            try:
                ek = data.get("event_key")
                telegram_id = str(data.get("telegram_id"))
                etype = data.get("event_type") or "Lead"
                if not ek or not telegram_id:
                    logger.warning(f"[SAVE_LEAD] evento inválido: ek={ek} tg={telegram_id}")
                    return False

                lead = session.query(Lead).filter(Lead.event_key == ek).first()

                normalized_ud = data.get("user_data") or {"telegram_id": telegram_id}
                custom = data.get("custom_data") or {}
                custom["priority_score"] = compute_priority_score(normalized_ud, custom)

                enc_cookies = None
                if isinstance(data.get("cookies"), dict) and data["cookies"]:
                    enc_cookies = {k: _encrypt_value(v) for k, v in data["cookies"].items()}

                if lead:
                    logger.info(f"[DB_UPDATE] Atualizando ek={ek} tipo={etype}")
                    lead.user_data = {**(lead.user_data or {}), **normalized_ud}
                    lead.custom_data = {**(lead.custom_data or {}), **custom}
                    lead.event_type = etype
                    lead.src_url = data.get("src_url") or lead.src_url
                    lead.currency = data.get("currency") or lead.currency
                    if enc_cookies:
                        ec = lead.cookies or {}
                        ec.update(enc_cookies)
                        lead.cookies = ec
                    lead.device_info = data.get("device_info") or lead.device_info
                    lead.session_metadata = {**(lead.session_metadata or {}), **(data.get("session_metadata") or {})}
                    if event_record:
                        eh = lead.event_history or []
                        eh.append({**event_record, "ts": datetime.now(timezone.utc).isoformat()})
                        lead.event_history = eh
                else:
                    logger.info(f"[DB_INSERT] Inserindo ek={ek} tipo={etype}")
                    lead = Lead(
                        event_key=ek,
                        telegram_id=telegram_id,
                        event_type=etype,
                        route_key=data.get("route_key"),
                        src_url=data.get("src_url"),
                        value=data.get("value"),
                        currency=data.get("currency"),
                        user_data=normalized_ud,
                        custom_data=custom,
                        cookies=enc_cookies,
                        device_info=data.get("device_info"),
                        session_metadata=data.get("session_metadata"),
                        event_history=[{**event_record, "ts": datetime.now(timezone.utc).isoformat()}] if event_record else []
                    )
                    session.add(lead)

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
                logger.warning(f"Conexão DB falhou, retry... ({retries} left) {e}")
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
# Recuperar leads não enviados
# ==============================
async def get_unsent_leads(limit: int = 500) -> List[Dict[str, Any]]:
    if not SessionLocal:
        return []

    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            rows = (
                session.query(Lead)
                .filter(Lead.sent == False)
                .order_by(Lead.created_at.asc())
                .limit(limit)
                .all()
            )
            return [r.user_data for r in rows]
        except Exception as e:
            logger.error(f"Erro get_unsent_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Recuperar leads históricos
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
            leads = []
            for r in rows:
                ud, cd = r.user_data or {}, r.custom_data or {}
                dec_cookies = _safe_dict(r.cookies or {}, decrypt=True) if r.cookies else {}
                leads.append({
                    "telegram_id": r.telegram_id,
                    "event_key": r.event_key,
                    "event_type": r.event_type,
                    "route_key": r.route_key,
                    "src_url": r.src_url,
                    "value": r.value,
                    "currency": r.currency,
                    "user_data": ud,
                    "custom_data": cd,
                    "cookies": dec_cookies,
                    "sent": r.sent,
                    "sent_pixels": r.sent_pixels or [],
                    "event_history": r.event_history or [],
                    "priority_score": cd.get("priority_score") or 0.0,
                    "created_at": r.created_at.isoformat(),
                    "last_sent_at": r.last_sent_at.isoformat() if r.last_sent_at else None,
                    "last_attempt_at": r.last_attempt_at.isoformat() if r.last_attempt_at else None
                })
            return leads
        except Exception as e:
            logger.error(f"Erro get_historical_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Sincronizar leads pendentes
# ==============================
async def sync_pending_leads(batch_size: int = 20) -> int:
    if not SessionLocal:
        return 0

    loop = asyncio.get_event_loop()

    def fetch_pending():
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

    leads = await loop.run_in_executor(None, fetch_pending)
    if not leads:
        return 0

    from fb_google import send_event_to_all  # lazy import

    processed = 0
    for l in leads:
        try:
            lead_data = {
                "telegram_id": l.telegram_id,
                "event_key": l.event_key,
                "event_type": l.event_type or "Lead",
                "user_data": l.user_data or {},
                "custom_data": l.custom_data or {},
                "cookies": _safe_dict(l.cookies or {}, decrypt=True),
                "src_url": l.src_url,
                "value": l.value,
                "currency": l.currency
            }

            results = await send_event_to_all(lead_data, et=lead_data["event_type"])

            if any((isinstance(v, dict) and v.get("ok")) for v in (results or {}).values()):
                l.sent = True
                l.last_sent_at = datetime.now(timezone.utc)
            else:
                l.last_attempt_at = datetime.now(timezone.utc)

            session = SessionLocal()
            session.merge(l)
            session.commit()
            session.close()

            processed += 1

        except Exception as e:
            logger.error(f"[SYNC_PENDING_ERROR] ek={l.event_key} err={e}")

    return processed