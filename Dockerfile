# =============================
# Dockerfile — Bridge + BotGestor + GeoIP + supervisord
# Base: Debian 12 (bookworm) com Python 3.11 via apt
# =============================
FROM debian:bookworm-slim

# -----------------------------
# Variáveis globais
# -----------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    PATH="/usr/local/bin:$PATH" \
    GEOIP_PATH="/app/GeoLite2-City.mmdb" \
    PYTHONPATH="/app"

WORKDIR /app

# -----------------------------
# 1) Sistema + Python 3.11 + build deps
# -----------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-dev python3-pip \
    gcc g++ make libpq-dev \
    curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# 2) Instalar dependências do Bridge
# -----------------------------
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip3 install --break-system-packages --no-cache-dir -r requirements-bridge.txt

# -----------------------------
# 3) Instalar dependências do BotGestor
# -----------------------------
COPY bot_gesto/requirements.txt ./requirements-bot.txt
RUN pip3 install --break-system-packages --no-cache-dir -r requirements-bot.txt

# -----------------------------
# 4) Supervisord (via pip)
# -----------------------------
RUN pip3 install --break-system-packages --no-cache-dir supervisor

# -----------------------------
# 5) Copiar código
# -----------------------------
COPY . .

# -----------------------------
# 6) Baixar GeoLite2 City (GeoIP) - direto, sem tar
# -----------------------------
RUN curl -L -o /app/GeoLite2-City.mmdb \
    https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-City.mmdb

# -----------------------------
# 7) Expor portas
# -----------------------------
EXPOSE 8080
EXPOSE 8000

# -----------------------------
# 8) Entrypoint
# -----------------------------
CMD ["supervisord", "-c", "/app/supervisord.conf"]