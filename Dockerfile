# =============================
# Dockerfile — Bridge + Bot B (bot_gesto)
# =============================

# Base leve do Python 3.11
FROM python:3.11-slim

# Define diretório de trabalho
WORKDIR /app

# Copia requirements do bridge (se existir)
COPY requirements-bridge.txt ./requirements-bridge.txt

# Instala dependências do bridge
RUN pip install --no-cache-dir -r requirements-bridge.txt

# Copia o app_bridge.py
COPY app_bridge.py ./app_bridge.py

# Copia a pasta do Bot B (db.py, fb_google.py etc.)
COPY bot_gesto ./bot_gesto

# Variáveis de ambiente padrão
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

# Expõe a porta
EXPOSE 8080

# Comando para iniciar
CMD ["uvicorn", "app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]