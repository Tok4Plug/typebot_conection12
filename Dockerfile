# =============================
# Dockerfile — Bridge + bot_gesto
# =============================
FROM python:3.11-slim

WORKDIR /app

# deps do bridge
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# deps do bot_gesto
COPY bot_gesto/requirements.txt ./bot_gesto/requirements.txt
RUN pip install --no-cache-dir -r bot_gesto/requirements.txt

# código
COPY . .

# dica: se quiser fixar a raiz do bot, descomente a linha abaixo
# ENV BRIDGE_BOT_DIR=/app/bot_gesto

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

EXPOSE 8080
CMD ["uvicorn", "app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]