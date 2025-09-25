# =============================
# Procfile — Bridge + BotGestor unificado (Railway 3.0)
# =============================

# 🔄 Release: roda migrations ANTES de qualquer processo iniciar
release: alembic upgrade head

# 🤖 Bot principal: captura leads, dispara eventos e integra com Typebot
bot: python -m bot_gesto.bot

# ⚙️ Worker: processa filas (eventos, retro-feed, retries, sync CAPI)
worker: python -m bot_gesto.worker

# 📊 Admin: painel HTTP + métricas Prometheus (monitoramento/healthcheck)
admin: uvicorn bot_gesto.admin_service:app --host 0.0.0.0 --port 8000 --log-level info

# 🔁 Retro-feed: reenvia leads antigos para pixels (manual ou escalado)
retrofeed: python -m bot_gesto.retrofeed

# 🔥 Warmup: reprocessa leads históricos para enriquecer pixels e score
warmup: python -m bot_gesto.tools.warmup

# 📦 DLQ Processor: trata mensagens da dead-letter queue com retry inteligente
dlq: python -m bot_gesto.tools.dlq_processor

# ⏰ Scheduler: tarefas periódicas (limpeza de filas, métricas, sync, heartbeat)
scheduler: python -m bot_gesto.tools.scheduler

# 🌉 Bridge API: FastAPI principal (para Typebot e integrações externas)
bridge: uvicorn app_bridge:app --host 0.0.0.0 --port 8080 --log-level info --proxy-headers --forwarded-allow-ips="*"

# 🛠️ Migrate: comando manual para aplicar migrations (se precisar rodar forçado)
migrate: alembic upgrade head

# =============================
# Observações:
# - Railway entende cada linha como serviço separado (bot, worker, admin, etc.)
# - release garante que migrations rodem ANTES do deploy
# - admin (porta 8000) recomendado como healthcheck no Railway
# - bridge (porta 8080) deve ser exposto ao público (entrada do Typebot)
# - worker/retrofeed/dlq/scheduler podem ser escalados separadamente conforme a carga
# =============================