import requests


RETRY = "RETRY"
FAILED = "FAILED"


def classify_error(e: Exception) -> str:
    """
    Классифицирует ошибку как RETRY или FAILED.

    Правила:
    - timeout / connection error -> RETRY
    - HTTP 429 -> RETRY
    - HTTP 5xx -> RETRY
    - HTTP 400 / 401 / 403 / 422 -> FAILED
    - всё неизвестное -> RETRY (безопасный дефолт для POC)
    """

    # requests timeout / connection problems
    if isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return RETRY

    # HTTPError с доступным response.status_code
    if isinstance(e, requests.exceptions.HTTPError):
        status_code = None
        if e.response is not None:
            status_code = e.response.status_code
        return _classify_status_code(status_code)

    # Кастомные ошибки, у которых есть status_code
    status_code = getattr(e, "status_code", None)
    if status_code is not None:
        return _classify_status_code(status_code)

    # Иногда статус может лежать в response
    response = getattr(e, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            return _classify_status_code(status_code)

    # На этапе POC неизвестные ошибки лучше считать временными
    return RETRY


def _classify_status_code(status_code: int | None) -> str:
    if status_code is None:
        return RETRY

    if status_code == 429:
        return RETRY

    if 500 <= status_code <= 599:
        return RETRY

    if status_code in (400, 401, 403, 422):
        return FAILED

    # Остальные 4xx по умолчанию считаем фатальными
    if 400 <= status_code <= 499:
        return FAILED

    return RETRY