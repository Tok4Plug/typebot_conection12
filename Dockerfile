# =============================
# Dockerfile — Bridge + bot_gesto
# =============================
FROM python:3.11-slim

WORKDIR /app

# Dependências
COPY requirements-bridge.txt ./requirements-bridge.txt
RUN pip install --no-cache-dir -r requirements-bridge.txt

# Copia TUDO da pasta atual (inclui bot_gesto/)
COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

EXPOSE 8080

CMD ["uvicorn", "app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]