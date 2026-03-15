import logging

from fastapi import FastAPI

from app.logger import setup_logging
from app.api.tenants import router as tenants_router
from app.api.webhooks import router as webhooks_router
from app.api.events import router as events_router
from app.api.mappings import router as mappings_router
from app.api.errors import router as errors_router

setup_logging()
log = logging.getLogger("api")

app = FastAPI()

app.include_router(tenants_router)
app.include_router(webhooks_router)
app.include_router(events_router)
app.include_router(mappings_router)
app.include_router(errors_router)


@app.get("/health")
def health():
    return {"status": "ok"}