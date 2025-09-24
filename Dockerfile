# =============================
# Dockerfile — Bridge + BotGestor com supervisord
# =============================
FROM python:3.11-slim

# Variáveis globais
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# 1) Deps do Bridge
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# 2) Deps do bot_gesto
COPY bot_gesto/requirements.txt ./requirements-bot.txt
RUN pip install --no-cache-dir -r requirements-bot.txt

# 3) Instala supervisord
RUN pip install --no-cache-dir supervisor

# 4) Copia todo o código (Bridge + BotGestor)
COPY . .

# 5) Expõe porta para o Bridge
EXPOSE 8080

# 6) Usa supervisord para orquestrar bridge + bot + worker
CMD ["supervisord", "-c", "/app/supervisord.conf"]