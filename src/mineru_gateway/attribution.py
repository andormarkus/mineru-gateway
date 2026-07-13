"""Attribution helpers (MinerU LICENSE §2 — online-service attribution obligation).

MinerU is Apache 2.0 *with additional terms*. §2 requires any online service built on MinerU to "clearly and
prominently indicate ... that MinerU is used."

This is a **license obligation**, not an optional credit. The middleware here sets attribution headers on every
response; the API layer adds the ``upstream`` field to ``/health`` and an attribution page at ``GET /``.

See PLAN.md "Attribution" section. Do not remove these.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from mineru_gateway.mineru_compat import MINERU_VERSION

if TYPE_CHECKING:
    from fastapi import FastAPI, Request, Response

UPSTREAM_NAME = "MinerU"
UPSTREAM_HOMEPAGE = "https://github.com/opendatalab/MinerU"
POWERED_BY_HEADER = "X-Powered-By"
VERSION_HEADER_NAME = "MinerU-Version"


def upstream_info() -> dict[str, str]:
    """The ``upstream`` block embedded in ``/health`` and ``GET /``."""
    return {"name": UPSTREAM_NAME, "version": MINERU_VERSION, "homepage": UPSTREAM_HOMEPAGE}


async def attribution_middleware(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """Starlette/FastAPI middleware: stamp attribution headers on every response.

    MinerU §2: an online service built on MinerU must clearly indicate MinerU is used. Doing it in a middleware
    guarantees no route can accidentally drop it.
    """
    response = await call_next(request)
    response.headers[POWERED_BY_HEADER] = UPSTREAM_NAME
    response.headers[VERSION_HEADER_NAME] = MINERU_VERSION
    return response


def install_attribution(app: FastAPI) -> None:
    """Register the attribution middleware on a FastAPI app."""
    app.add_middleware(BaseHTTPMiddleware, dispatch=attribution_middleware)  # type: ignore[arg-type]
