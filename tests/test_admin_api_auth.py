from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.security import require_admin_api_token


def build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/protected", dependencies=[Depends(require_admin_api_token)])
    def protected():
        return {"status": "ok"}

    return app


def test_health_is_public(monkeypatch):
    monkeypatch.setenv("ADMIN_API_TOKEN", "secret-token")
    client = TestClient(build_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_protected_requires_auth_when_token_is_set(monkeypatch):
    monkeypatch.setenv("ADMIN_API_TOKEN", "secret-token")
    client = TestClient(build_app())

    response = client.get("/protected")

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing Authorization header"


def test_protected_rejects_wrong_token(monkeypatch):
    monkeypatch.setenv("ADMIN_API_TOKEN", "secret-token")
    client = TestClient(build_app())

    response = client.get(
        "/protected",
        headers={"Authorization": "Bearer wrong-token"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid admin token"


def test_protected_accepts_valid_token(monkeypatch):
    monkeypatch.setenv("ADMIN_API_TOKEN", "secret-token")
    client = TestClient(build_app())

    response = client.get(
        "/protected",
        headers={"Authorization": "Bearer secret-token"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_protected_is_open_when_admin_token_not_set(monkeypatch):
    monkeypatch.delenv("ADMIN_API_TOKEN", raising=False)
    client = TestClient(build_app())

    response = client.get("/protected")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
