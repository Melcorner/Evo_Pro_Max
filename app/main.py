import logging

from fastapi import FastAPI

from app.logger import setup_logging
from app.api.tenants import router as tenants_router
from app.api.webhooks import router as webhooks_router
from app.api.events import router as events_router
from app.api.mappings import router as mappings_router
from app.api.errors import router as errors_router
from app.api.evotor import router as evotor_router
from app.api.sync import router as sync_router
from app.api.moysklad_webhooks import router as moysklad_webhooks_router
from dotenv import load_dotenv

load_dotenv()
setup_logging()
log = logging.getLogger("api")

openapi_tags = [
    {"name": "Infrastructure", "description": "Служебные endpoint'ы приложения"},
    {"name": "Tenants", "description": "Tenant'ы и конфигурация интеграции"},
    {"name": "Sync", "description": "Синхронизация товаров и остатков"},
    {"name": "Evotor Webhooks", "description": "Webhook'и от Эвотор"},
    {"name": "MoySklad Webhooks", "description": "Webhook'и от МойСклад"},
    {"name": "Events", "description": "Просмотр и повторная обработка событий"},
    {"name": "Errors", "description": "Журнал ошибок"},
    {"name": "Evotor Service", "description": "Служебные callback'и и endpoint'ы Эвотор"},
    {"name": "Mappings", "description": "Маппинги Evotor ↔ MoySklad"},
]

app = FastAPI(
    title="Evotor ↔ MoySklad Integration Bus",
    openapi_tags=openapi_tags,
)

app.include_router(sync_router)
app.include_router(tenants_router)
app.include_router(webhooks_router)
app.include_router(events_router)
app.include_router(mappings_router)
app.include_router(errors_router)
app.include_router(evotor_router)
app.include_router(moysklad_webhooks_router)

@app.get("/health", tags=["Infrastructure"])
def health():
    return {"status": "ok"}