"""Admin API: read-only worker registry and emergency intent updates."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mineru_gateway.config import GatewaySettings
from mineru_gateway.db.models import Worker
from mineru_gateway.scheduler.cache_service import CacheService
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.util.datetime import to_iso

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


class EmergencyCommandRequest(BaseModel):
    reason: str = "manual"
    requester: str | None = None


class DrainWorkerRequest(EmergencyCommandRequest):
    terminate: bool = False


def _workers_repo(request: Request) -> WorkerRepository:
    settings: GatewaySettings = request.app.state.settings
    return WorkerRepository(settings)


@router.get("/workers")
async def list_workers(request: Request) -> JSONResponse:
    rows = await _workers_repo(request).list_workers()
    return JSONResponse(content={"workers": [_worker_dict(w) for w in rows]})


@router.get("/workers/{worker_id}")
async def get_worker(worker_id: str, request: Request) -> JSONResponse:
    row = await _workers_repo(request).get_worker(worker_id)
    if row is None:
        return JSONResponse(status_code=404, content={"detail": "Worker not found"})
    return JSONResponse(content=_worker_dict(row))


@router.post("/workers/{worker_id}/drain")
async def drain_worker(worker_id: str, body: DrainWorkerRequest, request: Request) -> JSONResponse:
    drain_target = "terminated" if body.terminate else "stopped"
    row = await _workers_repo(request).set_drain_intent(
        worker_id, drain_target=drain_target, reason=body.reason, requester=body.requester
    )
    if row is None:
        return JSONResponse(status_code=404, content={"detail": "Worker not found"})
    return JSONResponse(content=_worker_dict(row))


@router.post("/workers/{worker_id}/rotate")
async def rotate_worker(worker_id: str, body: EmergencyCommandRequest, request: Request) -> JSONResponse:
    row = await _workers_repo(request).set_rotation_requested(worker_id, reason=body.reason, requester=body.requester)
    if row is None:
        return JSONResponse(status_code=404, content={"detail": "Worker not found"})
    return JSONResponse(content=_worker_dict(row))


@router.post("/workers/{worker_id}/recover")
async def recover_worker(worker_id: str, body: EmergencyCommandRequest, request: Request) -> JSONResponse:
    row = await _workers_repo(request).recover_worker(worker_id, reason=body.reason, requester=body.requester)
    if row is None:
        return JSONResponse(status_code=404, content={"detail": "Worker not found"})
    return JSONResponse(content=_worker_dict(row))


@router.delete("/cache/{cache_key}")
async def invalidate_cache(cache_key: str, request: Request) -> JSONResponse:
    settings: GatewaySettings = request.app.state.settings
    store = getattr(request.app.state, "object_store", None)
    if store is None:
        return JSONResponse(status_code=503, content={"detail": "Object store not configured"})
    cache = CacheService(settings, store)
    if not await cache.invalidate(cache_key):
        return JSONResponse(status_code=404, content={"detail": "Cache entry not found"})
    return JSONResponse(content={"invalidated": cache_key})


@router.post("/cache/sweep")
async def sweep_cache(request: Request) -> JSONResponse:
    settings: GatewaySettings = request.app.state.settings
    store = getattr(request.app.state, "object_store", None)
    if store is None:
        return JSONResponse(status_code=503, content={"detail": "Object store not configured"})
    removed = await CacheService(settings, store).sweep_expired()
    return JSONResponse(content={"removed": removed})


def _worker_dict(w: Worker) -> dict[str, Any]:
    return {
        "id": w.id,
        "provider": w.provider,
        "deployment_id": w.deployment_id,
        "instance_id": w.instance_id,
        "base_url": w.base_url,
        "desired_state": w.desired_state,
        "cloud_state": w.cloud_state,
        "healthy": w.healthy,
        "draining": w.draining,
        "drain_target": w.drain_target,
        "replacement_for": w.replacement_for,
        "rotation_requested": w.rotation_requested,
        "generation": w.generation,
        "failure_count": w.failure_count,
        "retry_after": to_iso(w.retry_after),
        "stalled_at": to_iso(w.stalled_at),
        "ready_at": to_iso(w.ready_at),
        "last_error": w.last_error,
        "last_active_at": to_iso(w.last_active_at),
        "terminated_at": to_iso(w.terminated_at),
        "created_at": to_iso(w.created_at),
        "updated_at": to_iso(w.updated_at),
    }
