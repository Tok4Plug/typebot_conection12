# =============================
# Dockerfile — Bridge + Bot B (typebot_conection.bot_gesto)
# =============================

# Base leve do Python 3.11
FROM python:3.11-slim

# Define diretório de trabalho no container
WORKDIR /app

# Copia requirements do bridge (se existir)
COPY requirements-bridge.txt ./requirements-bridge.txt

# Instala dependências do bridge
RUN pip install --no-cache-dir -r requirements-bridge.txt

# Copia toda a pasta typebot_conection (bridge + bot_gesto)
COPY typebot_conection ./typebot_conection

# Variáveis de ambiente padrão
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

# Expõe a porta
EXPOSE 8080

# Comando para iniciar o Bridge
CMD ["uvicorn", "typebot_conection.app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]