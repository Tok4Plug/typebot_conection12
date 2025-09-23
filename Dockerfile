# =============================
# Dockerfile — Bridge + typebot_conection.bot_gesto
# =============================
FROM python:3.11-slim

WORKDIR /app

# dependências do bridge
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# código inteiro
COPY . .

# garantir que o Python veja typebot_conection
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

EXPOSE 8080
CMD ["uvicorn", "app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]