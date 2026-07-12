"""HTTP request logging middleware."""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log inbound HTTP requests at DEBUG/INFO/WARNING based on method and status."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.perf_counter()
        method = request.method
        path = request.url.path
        client = request.client.host if request.client else "-"

        logger.debug("→ %s %s from %s", method, path, client)
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.exception("← %s %s failed after %.1fms", method, path, elapsed_ms)
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000
        status = response.status_code
        if status >= 500:
            logger.error("← %s %s %d (%.1fms)", method, path, status, elapsed_ms)
        elif status >= 400:
            logger.warning("← %s %s %d (%.1fms)", method, path, status, elapsed_ms)
        elif method in ("POST", "PUT", "PATCH", "DELETE"):
            logger.info("← %s %s %d (%.1fms)", method, path, status, elapsed_ms)
        else:
            logger.debug("← %s %s %d (%.1fms)", method, path, status, elapsed_ms)
        return response
