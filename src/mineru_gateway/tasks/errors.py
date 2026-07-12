"""Error-code taxonomy for dispatch retry decisions."""

from __future__ import annotations

from enum import StrEnum

import httpx


class ErrorClass(StrEnum):
    INFRA = "infra"
    CONTENT = "content"


def classify_error(exc: Exception) -> ErrorClass:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if 400 <= status < 500 and status != 429:
            return ErrorClass.CONTENT
        return ErrorClass.INFRA
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError, OSError)):
        return ErrorClass.INFRA
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        if 400 <= status_code < 500 and status_code != 429:
            return ErrorClass.CONTENT
        return ErrorClass.INFRA
    return ErrorClass.INFRA


def should_retry(exc: Exception) -> bool:
    return classify_error(exc) == ErrorClass.INFRA
