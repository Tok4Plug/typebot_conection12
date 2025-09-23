# =============================
# Dockerfile — Bridge + Bot B (bot_gesto)
# =============================

# Base leve do Python 3.11
FROM python:3.11-slim

# Define diretório de trabalho
WORKDIR /app

# Copia requirements do bridge
COPY requirements-bridge.txt ./requirements-bridge.txt

# Instala dependências do bridge
RUN pip install --no-cache-dir -r requirements-bridge.txt

# Copia todos os arquivos da pasta atual (typebot_conection)
COPY . .

# Variáveis de ambiente padrão
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

# Expõe a porta
EXPOSE 8080

# Comando para iniciar
CMD ["uvicorn", "app_bridge:app", "--host", "0.0.0.0", "--port", "8080"]