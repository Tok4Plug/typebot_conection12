# =============================
# Procfile — Bridge + BotGestor unificado
# =============================

# 🔄 Etapa de migração automática (executa antes de iniciar serviços)
release: alembic upgrade head

# 🤖 Bot principal: captura leads e envia eventos para FB/GA + Typebot
bot: python bot_gesto/bot.py

# ⚙️ Worker: processa filas de eventos, retro-feed e retries
worker: python bot_gesto/worker.py

# 📊 Admin: painel HTTP/Prometheus (monitoramento e métricas)
admin: uvicorn bot_gesto/admin_service:app --host 0.0.0.0 --port 8000 --log-level info

# 🔁 Retro-feed: reenvia leads antigos para novos pixels
retrofeed: python bot_gesto/retrofeed.py

# 🔥 Warmup: reprocessa leads históricos para enriquecer score e priorização
warmup: python bot_gesto/tools/warmup.py

# 📦 DLQ: processa eventos que falharam (dead-letter queue), com retry e logging detalhado
dlq: python bot_gesto/tools/dlq_processor.py

# ⏰ Scheduler: executa tarefas periódicas (limpeza de filas, métricas e atualizações automáticas)
scheduler: python bot_gesto/tools/scheduler.py

# 🌉 Bridge: API FastAPI que gera deep links e dispara eventos
bridge: uvicorn app_bridge:app --host 0.0.0.0 --port 8080 --log-level info

# 🛠️ Migração manual opcional (caso precise rodar forçado)
migrate: alembic upgrade head

# =============================
# Observações:
# - release garante que migrations rodem ANTES do app subir
# - Railway pode escalar cada processo separadamente
# - admin roda em 0.0.0.0:8000 (defina como Healthcheck no Railway)
# - bridge roda em 0.0.0.0:8080 (endpoints do Typebot)
# =============================