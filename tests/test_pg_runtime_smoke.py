import os

import pytest

from app.db import db_backend
import app.scripts.check_pg_runtime as smoke_module


def _pg_runtime_smoke_enabled() -> bool:
    if os.getenv("RUN_PG_RUNTIME_SMOKE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    return db_backend() == "postgresql"


_PG_RUNTIME_SKIP = pytest.mark.skipif(
    not _pg_runtime_smoke_enabled(),
    reason="Set RUN_PG_RUNTIME_SMOKE=1 and DATABASE_URL=postgresql://... to run PostgreSQL runtime smoke checks.",
)


@_PG_RUNTIME_SKIP
def test_pg_runtime_smoke():
    summary = smoke_module.run_pg_runtime_smoke()

    assert summary["status"] == "ok"
    assert summary["backend"] == "postgresql"
    assert any(check["name"] == "alert_worker_runtime" for check in summary["checks"])


def test_run_pg_runtime_smoke_skips_cleanup_after_primary_connection_failure(monkeypatch):
    cleanup_calls: list[str] = []

    def fail_connection_and_schema():
        raise RuntimeError("primary boom")

    def fail_cleanup(_ctx):
        cleanup_calls.append("called")
        raise RuntimeError("secondary boom")

    monkeypatch.setattr(smoke_module, "_require_postgresql", lambda: None)
    monkeypatch.setattr(smoke_module, "safe_database_config_for_log", lambda *args, **kwargs: {"backend": "postgresql"})
    monkeypatch.setattr(smoke_module, "_check_connection_and_schema", fail_connection_and_schema)
    monkeypatch.setattr(smoke_module, "_delete_smoke_rows", fail_cleanup)

    with pytest.raises(RuntimeError) as exc_info:
        smoke_module.run_pg_runtime_smoke()

    assert "primary boom" in str(exc_info.value)
    assert cleanup_calls == []
