"""API-key authentication middleware."""

from __future__ import annotations

import secrets

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

_PUBLIC_PATHS = frozenset({"/health", "/ready", "/docs", "/openapi.json", "/redoc"})


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Require a valid API key on non-probe endpoints when auth is enabled."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        settings = request.app.state.settings
        if not settings.auth.enabled:
            return await call_next(request)
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        provided = (
            request.headers.get("x-api-key") or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        )
        if not provided:
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
        valid_keys = settings.auth.resolved_keys()
        if not any(secrets.compare_digest(provided, key) for key in valid_keys):
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key"})
        return await call_next(request)
