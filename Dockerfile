# =============================
# Dockerfile — Bridge + BotGestor com supervisord (corrigido 100%)
# =============================
FROM python:3.11-alpine

# -----------------------------
# 1) Variáveis globais
# -----------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    PATH="/usr/local/bin:$PATH"

WORKDIR /app

# -----------------------------
# 2) Instala pacotes básicos do sistema
# -----------------------------
# Incluímos build-base (gcc, g++, make) + libpq para PostgreSQL
RUN apk add --no-cache \
    build-base \
    libpq-dev \
    musl-dev \
    linux-headers \
    bash \
    curl

# -----------------------------
# 3) Deps do Bridge
# -----------------------------
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# -----------------------------
# 4) Deps do BotGestor
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
# 7) Expõe portas necessárias
# -----------------------------
EXPOSE 8080
EXPOSE 8000

# -----------------------------
# 8) Supervisord como entrypoint
# -----------------------------
CMD ["supervisord", "-c", "/app/supervisord.conf"]