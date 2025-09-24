# =============================
# Framework Web
# =============================
fastapi==0.111.0
uvicorn[standard]==0.30.6

# =============================
# Redis / Cache / Fila
# =============================
redis==5.1.1

# =============================
# Validação / Config
# =============================
pydantic==2.7.4
python-dotenv==1.0.1

# =============================
# HTTP Requests / Async
# =============================
aiohttp==3.9.5
httpx==0.27.0

# =============================
# Segurança / Criptografia
# =============================
cryptography==43.0.1
python-jose==3.3.0

# =============================
# Observabilidade / Métricas
# =============================
prometheus-client==0.20.0
structlog==24.4.0

# =============================
# GeoIP / Enriquecimento
# =============================
geoip2==4.8.0         # Leitura do banco MaxMind GeoLite2
maxminddb==2.6.2      # Driver nativo do mmdb para alta performance

# =============================
# Extras / Typing
# =============================
typing-extensions==4.12.2