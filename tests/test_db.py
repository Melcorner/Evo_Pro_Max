import pytest

import app.db as db_module


def test_parse_postgres_dsn_valid_url():
    config = db_module.parse_postgres_dsn(
        "postgresql://demo:secret@localhost:5432/evotor_ms?sslmode=require&application_name=integration-bus"
    )

    assert config["host"] == "localhost"
    assert config["port"] == 5432
    assert config["dbname"] == "evotor_ms"
    assert config["user"] == "demo"
    assert config["password"] == "secret"
    assert config["sslmode"] == "require"
    assert config["application_name"] == "integration-bus"


def test_parse_postgres_dsn_decodes_percent_encoded_credentials():
    config = db_module.parse_postgres_dsn(
        "postgresql://demo%40user:p%40ss%3Aword%2F42@localhost/evotor_ms"
    )

    assert config["user"] == "demo@user"
    assert config["password"] == "p@ss:word/42"
    assert config["port"] == 5432


def test_validate_database_url_normalizes_bom_and_spaces():
    normalized = db_module.validate_database_url(
        "\ufeff  postgresql://demo:secret@localhost:5432/evotor_ms  "
    )

    assert normalized == "postgresql://demo:secret@localhost:5432/evotor_ms"


def test_parse_postgres_dsn_rejects_nbsp_and_copy_paste_garbage():
    with pytest.raises(ValueError) as exc_info:
        db_module.parse_postgres_dsn("postgresql://demo:sec\u00a0ret@localhost/evotor_ms")

    message = str(exc_info.value)
    assert "Parsed PostgreSQL password contains suspicious characters" in message
    assert "U+00A0" in message
    assert "Prefer temporary ASCII-only password for smoke-check" in message


def test_parse_postgres_dsn_rejects_smart_quote_in_password():
    with pytest.raises(ValueError) as exc_info:
        db_module.parse_postgres_dsn("postgresql://demo:sec\u201dret@localhost/evotor_ms")

    message = str(exc_info.value)
    assert "Parsed PostgreSQL password contains suspicious characters" in message
    assert "U+201D" in message


def test_inspect_postgres_dsn_components_hides_password_value():
    diagnostic = db_module.inspect_postgres_dsn_components(
        "postgresql://demo:top\u00a0secret@localhost:5432/evotor_ms?sslmode=require"
    )

    password_meta = diagnostic["components"]["password"]
    serialized = str(diagnostic)

    assert "top secret" not in serialized
    assert "top\u00a0secret" not in serialized
    assert password_meta["length"] == len("top\u00a0secret")
    assert password_meta["has_non_ascii"] is True
    assert password_meta["remaining_has_whitespace"] is True
    assert password_meta["suspicious_positions"] == [4]
    assert password_meta["suspicious_codepoints"] == ["U+00A0"]


def test_parse_postgres_dsn_removes_zero_width_chars_from_password():
    database_url = "postgresql://demo:sec%E2%80%8Bret@localhost/evotor_ms"

    config = db_module.parse_postgres_dsn(database_url)
    diagnostic = db_module.inspect_postgres_dsn_components(database_url)

    assert config["password"] == "secret"
    assert diagnostic["components"]["password"]["normalized_changed"] is True
    assert diagnostic["components"]["password"]["suspicious_codepoints"] == ["U+200B"]
    assert diagnostic["components"]["password"]["remaining_suspicious_codepoints"] == []


def test_validate_database_url_rejects_malformed_postgres_like_value():
    with pytest.raises(ValueError) as exc_info:
        db_module.validate_database_url("postgresql:/broken")

    assert "must start with one of" in str(exc_info.value)


def test_db_backend_detects_postgres_and_sqlite(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "\ufeff postgresql://demo:secret@localhost/evotor_ms ")
    assert db_module.db_backend() == "postgresql"

    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert db_module.db_backend() == "sqlite"


def test_get_connection_sqlite_keeps_named_row_access(monkeypatch, tmp_path):
    db_path = tmp_path / "app.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    conn = db_module.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 AS value")
        row = cur.fetchone()
    finally:
        conn.close()

    assert row["value"] == 1


def test_safe_database_config_for_log_hides_password():
    safe_config = db_module.safe_database_config_for_log(
        database_url="postgresql://demo:secret@localhost:5432/evotor_ms?sslmode=require"
    )

    assert safe_config["backend"] == "postgresql"
    assert safe_config["host"] == "localhost"
    assert safe_config["dbname"] == "evotor_ms"
    assert safe_config["user"] == "demo"
    assert safe_config["sslmode"] == "require"
    assert "password" not in safe_config
