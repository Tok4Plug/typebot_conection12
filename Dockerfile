# =============================
# Dockerfile — Bridge + BotGestor com supervisord
# =============================
FROM python:3.11-slim

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
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

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
EXPOSE 8080   # Bridge
EXPOSE 8000   # Admin Service (se usar Prometheus)

# -----------------------------
# 8) Supervisord como entrypoint
# -----------------------------
CMD ["supervisord", "-c", "/app/supervisord.conf"]