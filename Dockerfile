# =============================
# Dockerfile — Bridge + BotGestor com supervisord + GeoIP (sem Docker Hub)
# =============================
FROM gcr.io/google-appengine/python

# -----------------------------
# 1) Variáveis globais
# -----------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    PATH="/usr/local/bin:$PATH" \
    GEOIP_PATH="/app/GeoLite2-City.mmdb"

WORKDIR /app

# -----------------------------
# 2) Instala pacotes básicos do sistema
# -----------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libpq-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# 3) Instala dependências do Bridge
# -----------------------------
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# -----------------------------
# 4) Instala dependências do BotGestor
# -----------------------------
COPY bot_gesto/requirements.txt ./requirements-bot.txt
RUN pip install --no-cache-dir -r requirements-bot.txt

# -----------------------------
# 5) Instala supervisord
# -----------------------------
RUN pip install --no-cache-dir supervisor

# -----------------------------
# 6) Copia todo o código (Bridge + BotGestor + configs)
# -----------------------------
COPY . .

# -----------------------------
# 7) Baixa banco GeoLite2 (GeoIP2)
# -----------------------------
RUN curl -L -o GeoLite2-City.mmdb.tar.gz \
    https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-City.mmdb.tar.gz \
    && tar -xvzf GeoLite2-City.mmdb.tar.gz --strip-components=1 -C /app \
    && rm GeoLite2-City.mmdb.tar.gz

# -----------------------------
# 8) Expõe portas necessárias
# -----------------------------
# Bridge
EXPOSE 8080
# Admin
EXPOSE 8000

# -----------------------------
# 9) Supervisord como entrypoint
# -----------------------------
CMD ["supervisord", "-c", "/app/supervisord.conf"]