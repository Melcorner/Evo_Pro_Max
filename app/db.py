"""
app/db.py

Слой доступа к БД с поддержкой SQLite (dev) и PostgreSQL.

Управляется переменной окружения DATABASE_URL:
  - не задана или начинается с "sqlite" -> SQLite
  - начинается с "postgresql" / "postgres" -> PostgreSQL

Интерфейс get_connection() сохраняет совместимость:
  - SQLite: row["column"] через sqlite3.Row
  - PostgreSQL: row["column"] через psycopg2.extras.RealDictCursor
"""

import logging
import os
from urllib.parse import parse_qsl, unquote, urlsplit

log = logging.getLogger("app.db")

_POSTGRES_PREFIXES = ("postgresql://", "postgres://")
_SQLITE_PREFIXES = ("sqlite://",)
_SUSPICIOUS_CHAR_NAMES = {
    "\u00a0": "non-breaking space",
    "\u1680": "ogham space mark",
    "\u2000": "en quad",
    "\u2001": "em quad",
    "\u2002": "en space",
    "\u2003": "em space",
    "\u2004": "three-per-em space",
    "\u2005": "four-per-em space",
    "\u2006": "six-per-em space",
    "\u2007": "figure space",
    "\u2008": "punctuation space",
    "\u2009": "thin space",
    "\u200a": "hair space",
    "\u200b": "zero-width space",
    "\u200c": "zero-width non-joiner",
    "\u200d": "zero-width joiner",
    "\u2028": "line separator",
    "\u2029": "paragraph separator",
    "\u202f": "narrow non-breaking space",
    "\u205f": "medium mathematical space",
    "\u2060": "word joiner",
    "\u3000": "ideographic space",
    "\u2018": "left single quotation mark",
    "\u2019": "right single quotation mark",
    "\u201c": "left double quotation mark",
    "\u201d": "right double quotation mark",
    "\u00ab": "left angle quote",
    "\u00bb": "right angle quote",
}
_ZERO_WIDTH_CHARS = {
    "\u200b",
    "\u200c",
    "\u200d",
    "\u2060",
}
_NBSP_LIKE_CHARS = {
    "\u00a0",
    "\u1680",
    "\u2000",
    "\u2001",
    "\u2002",
    "\u2003",
    "\u2004",
    "\u2005",
    "\u2006",
    "\u2007",
    "\u2008",
    "\u2009",
    "\u200a",
    "\u2028",
    "\u2029",
    "\u202f",
    "\u205f",
    "\u3000",
}
_SMART_QUOTE_CHARS = {
    "\u2018",
    "\u2019",
    "\u201c",
    "\u201d",
    "\u00ab",
    "\u00bb",
}
_PG_LOG_KEYS = ("host", "port", "dbname", "user", "sslmode", "application_name")
_PG_REQUIRED_KEYS = ("host", "dbname", "user")
_PG_COMPONENT_KEYS = ("host", "dbname", "user", "password", "sslmode", "application_name")
_PG_COMPONENTS_WITHOUT_WHITESPACE = {"host", "dbname", "user", "password", "sslmode"}


def _get_database_url() -> str:
    return str(os.getenv("DATABASE_URL", "") or "")


def _normalize_database_url(database_url: str | None = None) -> str:
    value = _get_database_url() if database_url is None else str(database_url)
    value = value.lstrip("\ufeff").strip()

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()

    return value


def _raw_database_url_length(database_url: str | None = None) -> int:
    value = _get_database_url() if database_url is None else str(database_url)
    return len(value)


def _is_postgres_url(database_url: str) -> bool:
    lowered = database_url.lower()
    return lowered.startswith("postgresql") or lowered.startswith("postgres")


def _is_sqlite_url(database_url: str) -> bool:
    lowered = database_url.lower()
    return lowered.startswith("sqlite")


def _has_postgres_scheme(database_url: str) -> bool:
    lowered = database_url.lower()
    return lowered.startswith(_POSTGRES_PREFIXES)


def _find_suspicious_characters(value: str) -> list[str]:
    found: list[str] = []
    for idx, char in enumerate(value):
        if char in _SUSPICIOUS_CHAR_NAMES:
            found.append(f"{_SUSPICIOUS_CHAR_NAMES[char]} at position {idx + 1}")
        elif ord(char) > 127:
            found.append(f"non-ASCII character U+{ord(char):04X} at position {idx + 1}")
    return found


def _build_database_url_error(message: str) -> ValueError:
    return ValueError(
        f"{message} Rewrite DATABASE_URL in ASCII-safe URL form, for example "
        f"'postgresql://user:password@host:5432/dbname', and URL-encode special characters in credentials."
    )


def _scan_component_characters(value: str) -> dict:
    suspicious_positions: list[int] = []
    suspicious_codepoints: list[str] = []
    suspicious_names: list[str] = []
    has_non_ascii = False
    has_whitespace = False
    has_control_chars = False

    for idx, char in enumerate(value):
        codepoint = ord(char)
        if codepoint > 127:
            has_non_ascii = True
        if char.isspace():
            has_whitespace = True
        if codepoint < 32 or codepoint == 127:
            has_control_chars = True

        reason = None
        if char == "\ufeff":
            reason = "byte order mark"
        elif char in _ZERO_WIDTH_CHARS:
            reason = _SUSPICIOUS_CHAR_NAMES[char]
        elif char in _NBSP_LIKE_CHARS:
            reason = _SUSPICIOUS_CHAR_NAMES[char]
        elif char in _SMART_QUOTE_CHARS:
            reason = _SUSPICIOUS_CHAR_NAMES[char]
        elif codepoint < 32 or codepoint == 127:
            reason = "control character"
        elif codepoint > 127:
            reason = f"non-ASCII character U+{codepoint:04X}"

        if reason:
            suspicious_positions.append(idx + 1)
            suspicious_codepoints.append(f"U+{codepoint:04X}")
            suspicious_names.append(reason)

    return {
        "has_non_ascii": has_non_ascii,
        "has_whitespace": has_whitespace,
        "has_control_chars": has_control_chars,
        "suspicious_positions": suspicious_positions,
        "suspicious_codepoints": suspicious_codepoints,
        "suspicious_names": suspicious_names,
    }


def _normalize_pg_component(value: str | None) -> str:
    normalized = "" if value is None else str(value)
    normalized = normalized.replace("\ufeff", "")

    cleaned: list[str] = []
    for char in normalized:
        if char in _ZERO_WIDTH_CHARS:
            continue
        if char in _NBSP_LIKE_CHARS:
            cleaned.append(" ")
            continue
        cleaned.append(char)

    return "".join(cleaned).strip()


def _inspect_pg_component(value: str | None) -> tuple[str, dict]:
    raw_value = "" if value is None else str(value)
    normalized_value = _normalize_pg_component(raw_value)
    raw_scan = _scan_component_characters(raw_value)
    normalized_scan = _scan_component_characters(normalized_value)

    return normalized_value, {
        "length": len(raw_value),
        "normalized_length": len(normalized_value),
        "normalized_changed": raw_value != normalized_value,
        "has_non_ascii": raw_scan["has_non_ascii"],
        "has_whitespace": raw_scan["has_whitespace"],
        "has_control_chars": raw_scan["has_control_chars"],
        "suspicious_positions": raw_scan["suspicious_positions"],
        "suspicious_codepoints": raw_scan["suspicious_codepoints"],
        "suspicious_names": raw_scan["suspicious_names"],
        "remaining_has_non_ascii": normalized_scan["has_non_ascii"],
        "remaining_has_whitespace": normalized_scan["has_whitespace"],
        "remaining_has_control_chars": normalized_scan["has_control_chars"],
        "remaining_suspicious_positions": normalized_scan["suspicious_positions"],
        "remaining_suspicious_codepoints": normalized_scan["suspicious_codepoints"],
        "remaining_suspicious_names": normalized_scan["suspicious_names"],
    }


def _format_component_issue_details(component_name: str, inspection: dict) -> str:
    detail_parts: list[str] = []

    if inspection["suspicious_positions"]:
        detail_parts.append(
            f"suspicious chars at positions {inspection['suspicious_positions']}, "
            f"codepoints {inspection['suspicious_codepoints']}"
        )
    elif inspection["remaining_suspicious_positions"]:
        detail_parts.append(
            f"remaining suspicious chars at positions {inspection['remaining_suspicious_positions']}, "
            f"codepoints {inspection['remaining_suspicious_codepoints']}"
        )

    if component_name in _PG_COMPONENTS_WITHOUT_WHITESPACE and inspection["remaining_has_whitespace"]:
        detail_parts.append("contains whitespace after normalization")
    if inspection["remaining_has_control_chars"]:
        detail_parts.append("contains control characters after normalization")
    if inspection["remaining_has_non_ascii"]:
        detail_parts.append("contains suspicious non-ASCII characters after normalization")

    return "; ".join(detail_parts)


def _validate_pg_component(component_name: str, value: str, inspection: dict) -> None:
    if component_name in _PG_REQUIRED_KEYS and not value:
        raise _build_database_url_error(
            f"Parsed PostgreSQL {component_name} is empty after normalization. " + _postgres_parse_hint()
        )

    has_invalid_whitespace = (
        component_name in _PG_COMPONENTS_WITHOUT_WHITESPACE and inspection["remaining_has_whitespace"]
    )
    has_invalid_chars = inspection["remaining_has_control_chars"] or inspection["remaining_has_non_ascii"]

    if not has_invalid_whitespace and not has_invalid_chars:
        return

    details = _format_component_issue_details(component_name, inspection)
    guidance = "Rewrite the value manually or URL-encode it."
    if component_name == "password":
        guidance += " Prefer temporary ASCII-only password for smoke-check."

    raise _build_database_url_error(
        f"Parsed PostgreSQL {component_name} contains suspicious characters: {details}. {guidance}"
    )


def _parse_postgres_dsn_raw(database_url: str | None = None) -> dict:
    normalized = _normalize_database_url(database_url)
    raw_length = _raw_database_url_length(database_url)

    if not normalized:
        raise _build_database_url_error(
            "DATABASE_URL is empty but PostgreSQL configuration was requested."
        )

    if not _has_postgres_scheme(normalized):
        raise _build_database_url_error(
            f"DATABASE_URL must start with one of {', '.join(_POSTGRES_PREFIXES)} for PostgreSQL."
        )

    if "#" in normalized:
        raise _build_database_url_error(
            "DATABASE_URL contains '#', which usually means the password or query parameters are not URL-encoded. "
            + _postgres_parse_hint()
        )

    split = urlsplit(normalized)

    if split.fragment:
        raise _build_database_url_error(
            "DATABASE_URL contains a URL fragment. " + _postgres_parse_hint()
        )

    try:
        port = split.port or 5432
    except ValueError as exc:
        raise _build_database_url_error("DATABASE_URL contains an invalid PostgreSQL port.") from exc

    raw_kwargs = {
        "host": split.hostname or "",
        "port": port,
        "dbname": unquote(split.path.lstrip("/")),
        "user": unquote(split.username or ""),
        "password": unquote(split.password) if split.password is not None else "",
    }

    for key, value in parse_qsl(split.query, keep_blank_values=True):
        if not key or key in raw_kwargs:
            continue
        raw_kwargs[key] = value

    raw_kwargs["_raw_length"] = raw_length
    return raw_kwargs


def validate_database_url(database_url: str | None = None) -> str:
    normalized = _normalize_database_url(database_url)

    if not normalized:
        return normalized

    if _is_postgres_url(normalized):
        parse_postgres_dsn(normalized)
        return normalized

    suspicious = _find_suspicious_characters(normalized)
    if suspicious:
        details = ", ".join(suspicious)
        raise _build_database_url_error(
            f"DATABASE_URL contains suspicious characters: {details}."
        )

    if any(char.isspace() for char in normalized):
        raise _build_database_url_error(
            "DATABASE_URL contains whitespace inside the value."
        )

    if not (_is_postgres_url(normalized) or _is_sqlite_url(normalized)):
        return normalized

    return normalized


def _postgres_parse_hint() -> str:
    return (
        "If the password contains special characters such as '/', '?', '#', '@' or '%', "
        "URL-encode it before putting it into DATABASE_URL."
    )


def parse_postgres_dsn(database_url: str | None = None) -> dict:
    raw_kwargs = _parse_postgres_dsn_raw(database_url)

    connect_kwargs = {
        "port": raw_kwargs["port"],
        "_raw_length": raw_kwargs["_raw_length"],
    }
    component_diagnostics: dict[str, dict] = {}

    for key in _PG_COMPONENT_KEYS:
        normalized_value, inspection = _inspect_pg_component(raw_kwargs.get(key, ""))
        component_diagnostics[key] = inspection
        if key in raw_kwargs or key in {"host", "dbname", "user", "password"}:
            connect_kwargs[key] = normalized_value

    for key, value in raw_kwargs.items():
        if key.startswith("_") or key in connect_kwargs:
            continue
        connect_kwargs[key] = value

    for key in _PG_COMPONENT_KEYS:
        _validate_pg_component(key, str(connect_kwargs.get(key, "") or ""), component_diagnostics[key])

    return connect_kwargs


def inspect_postgres_dsn_components(database_url: str | None = None) -> dict:
    normalized = _normalize_database_url(database_url)
    raw_length = _raw_database_url_length(database_url)

    if not normalized:
        return {
            "backend": "unknown",
            "raw_length": raw_length,
            "parse_error": "DATABASE_URL is empty",
        }

    if not _is_postgres_url(normalized):
        return {
            "backend": "sqlite" if _is_sqlite_url(normalized) else "unknown",
            "raw_length": raw_length,
            "parse_error": "DATABASE_URL is not a PostgreSQL URL",
        }

    try:
        raw_kwargs = _parse_postgres_dsn_raw(database_url)
    except ValueError as exc:
        return {
            "backend": "postgresql",
            "raw_length": raw_length,
            "parse_error": str(exc),
        }

    safe_config_input = {
        "port": raw_kwargs["port"],
        "_raw_length": raw_kwargs["_raw_length"],
    }
    components: dict[str, dict] = {}

    for key in _PG_COMPONENT_KEYS:
        normalized_value, inspection = _inspect_pg_component(raw_kwargs.get(key, ""))
        components[key] = inspection
        if key != "password":
            safe_config_input[key] = normalized_value or None

    return {
        "backend": "postgresql",
        "safe_config": safe_database_config_for_log(safe_config_input),
        "components": components,
    }


def safe_database_config_for_log(
    config: dict | None = None,
    *,
    database_url: str | None = None,
) -> dict:
    pg_config = dict(config or parse_postgres_dsn(database_url))
    raw_length = pg_config.pop("_raw_length", _raw_database_url_length(database_url))

    safe_config = {"backend": "postgresql", "raw_length": raw_length}
    for key in _PG_LOG_KEYS:
        safe_config[key] = pg_config.get(key)
    return safe_config


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------


def _get_sqlite_path() -> str:
    url = _normalize_database_url()
    if url.lower().startswith("sqlite:///"):
        path = url[len("sqlite:///") :]
        return path or "data/app.db"
    if url.lower().startswith("sqlite://"):
        path = url[len("sqlite://") :]
        return path or "data/app.db"
    return "data/app.db"


def _get_sqlite_connection():
    import sqlite3

    path = _get_sqlite_path()
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ---------------------------------------------------------------------------
# PostgreSQL backend
# ---------------------------------------------------------------------------


def _get_pg_connection():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2 не установлен. Выполни: pip install psycopg2-binary"
        ) from exc

    try:
        connect_kwargs = parse_postgres_dsn()
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    safe_log_config = safe_database_config_for_log(connect_kwargs)
    log.debug("Opening PostgreSQL connection %s", safe_log_config)

    psycopg_connect_kwargs = {
        key: value
        for key, value in connect_kwargs.items()
        if not key.startswith("_")
    }

    try:
        return psycopg2.connect(
            cursor_factory=psycopg2.extras.RealDictCursor,
            **psycopg_connect_kwargs,
        )
    except UnicodeDecodeError as exc:
        component_diagnostic = inspect_postgres_dsn_components()
        raise RuntimeError(
            "Failed to decode PostgreSQL connection settings during psycopg2.connect(). "
            f"Parsed config={safe_log_config}. "
            f"Component diagnostic={component_diagnostic}. "
            "Rewrite the password manually or URL-encode it. Prefer temporary ASCII-only password for smoke-check."
        ) from exc


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------


def get_connection():
    """
    Возвращает соединение с БД.

    SQLite:     строки доступны как row["column"] (sqlite3.Row)
    PostgreSQL: строки доступны как row["column"] (RealDictCursor)
    """
    if db_backend() == "postgresql":
        return _get_pg_connection()

    log.debug("Opening SQLite connection path=%s", _get_sqlite_path())
    return _get_sqlite_connection()


def adapt_query(sql: str) -> str:
    """
    Конвертирует SQLite-style placeholders (?) в PostgreSQL-style (%s) и обратно.
    """
    if db_backend() == "postgresql":
        return sql.replace("?", "%s")
    return sql.replace("%s", "?")


aq = adapt_query


def db_backend() -> str:
    """Возвращает 'postgresql' или 'sqlite' — для логов и диагностики."""
    normalized = _normalize_database_url()
    return "postgresql" if _is_postgres_url(normalized) else "sqlite"
