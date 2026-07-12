"""Tier-1 FakeWorker HTTP server.

An in-process async HTTP server that faithfully mimics the ``mineru-api``
worker protocol, so dispatch / load-selection / status-polling /
normalization / dedup-miss can all be tested **in-process, no Docker, no
models**. Per PLAN.md this is the workhorse for ~90% of tests.

The protocol implemented here was read directly from the MinerU source
(``../MinerU/mineru/cli/fast_api.py`` and ``router.py``):

- ``GET /health`` →
    ``{status, version, protocol_version:2, queued_tasks, processing_tasks,
       completed_tasks, failed_tasks, max_concurrent_requests,
       processing_window_size}``
- ``POST /tasks`` (multipart form: ``files``, ``backend``, ``effort``,
  ``parse_method``, ...) → ``202`` with a status payload:
    ``{task_id, status, backend, file_names, created_at, started_at,
       completed_at, error, status_url, result_url}``
- ``GET /tasks/{id}`` → status payload (``404`` if unknown)
- ``GET /tasks/{id}/result`` → ``200`` ZIP, ``202`` if not ready, ``409`` if
  failed, ``404`` if unknown.

Scriptable knobs:
    - ``process_delay``: simulated parse latency (task transitions pending →
      processing → completed after this many seconds).
    - ``fail_next``: make the next N submitted tasks fail.
    - ``max_concurrent_requests`` / ``processing_window_size``: health fields
      that influence load selection in the router.
    - ``result_payload``: bytes returned as the result ZIP body (defaults to a
      minimal valid zip).
"""

from __future__ import annotations

import asyncio
import io
import uuid
import zipfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse, Response
from mineru.cli.api_protocol import API_PROTOCOL_VERSION
from mineru.version import __version__ as MINERU_VERSION

TASK_PENDING = "pending"
TASK_PROCESSING = "processing"
TASK_COMPLETED = "completed"
TASK_FAILED = "failed"


def _now_iso() -> str:
    """RFC3339 timestamp, matching MinerU's ``datetime.now(timezone.utc)``."""
    from datetime import datetime

    return datetime.now(UTC).isoformat()


def _minimal_zip(task_id: str) -> bytes:
    """A valid (if trivial) ZIP for the result endpoint body."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{task_id}/full.md", "# Fake result\n\nParsed content.")
        zf.writestr(f"{task_id}/middle.json", '{"pdf_info": [], "_backend": "fake", "_parse_method": "auto"}')
    return buf.getvalue()


@dataclass
class _FakeTask:
    """Mirrors the fields of MinerU's ``AsyncParseTask.to_status_payload``."""

    task_id: str
    status: str = TASK_PENDING
    backend: str = "pipeline"
    file_names: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_now_iso)
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None

    def to_payload(self, base_url: str = "") -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "backend": self.backend,
            "file_names": self.file_names,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "status_url": f"{base_url}/tasks/{self.task_id}",
            "result_url": f"{base_url}/tasks/{self.task_id}/result",
        }


@dataclass
class FakeWorkerState:
    """Mutable, scriptable state. Tests poke these directly between requests."""

    process_delay: float = 0.0
    """Seconds a task spends pending→processing→completed."""

    fail_next: int = 0
    """Next N submitted tasks will fail."""

    max_concurrent_requests: int = 3
    processing_window_size: int = 64

    # Counters surfaced in /health.
    completed_count: int = 0
    failed_count: int = 0

    result_payload: bytes | None = None
    """Override the result ZIP body. Defaults to a minimal valid zip per task."""

    unhealthy: bool = False
    """If True, /health returns 503."""

    tasks: dict[str, _FakeTask] = field(default_factory=dict)


def create_fake_worker_app(state: FakeWorkerState | None = None) -> FastAPI:
    """Build a FastAPI app implementing the mineru worker protocol.

    Args:
        state: shared mutable state; a fresh one is created if omitted. Tests
            typically hold a reference to script behavior between requests.
    """
    state = state or FakeWorkerState()
    # Track background "processing" tasks so we can cancel them on shutdown.
    bg_tasks: set[asyncio.Task[Any]] = set()

    @asynccontextmanager
    async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            for t in list(bg_tasks):
                t.cancel()
            await asyncio.gather(*bg_tasks, return_exceptions=True)

    app = FastAPI(title="FakeWorker", docs_url=None, redoc_url=None, lifespan=_lifespan)
    app.state.fake = state

    # ---------------------------------------------------------------- health --
    @app.get("/health")
    async def health() -> JSONResponse:
        if state.unhealthy:
            return JSONResponse(status_code=503, content={"status": "unhealthy", "version": MINERU_VERSION})
        queued = sum(1 for t in state.tasks.values() if t.status == TASK_PENDING)
        processing = sum(1 for t in state.tasks.values() if t.status == TASK_PROCESSING)
        return JSONResponse(
            {
                "status": "healthy",
                "version": MINERU_VERSION,
                "protocol_version": API_PROTOCOL_VERSION,
                "queued_tasks": queued,
                "processing_tasks": processing,
                "completed_tasks": state.completed_count,
                "failed_tasks": state.failed_count,
                "max_concurrent_requests": state.max_concurrent_requests,
                "processing_window_size": state.processing_window_size,
            }
        )

    # -------------------------------------------------------------- submit ----
    @app.post("/tasks", status_code=202)
    async def submit_task(
        files: list[UploadFile] = File(...),
        backend: str = Form("pipeline"),
        effort: str = Form("medium"),
        parse_method: str = Form("auto"),
    ) -> dict[str, Any]:
        task_id = str(uuid.uuid4())
        file_names = [f.filename or "upload" for f in files]
        task = _FakeTask(task_id=task_id, backend=backend, file_names=file_names)
        state.tasks[task_id] = task

        if state.fail_next > 0:
            state.fail_next -= 1
            task.status = TASK_FAILED
            task.error = "scripted failure"
            task.completed_at = _now_iso()
            state.failed_count += 1
        else:
            # Kick off the pending → processing → completed lifecycle.
            bg = asyncio.create_task(_advance(task, state))
            bg_tasks.add(bg)
            bg.add_done_callback(bg_tasks.discard)

        return task.to_payload()

    # --------------------------------------------------------------- status ---
    @app.get("/tasks/{task_id}")
    async def get_status(task_id: str) -> dict[str, Any]:
        task = state.tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return task.to_payload()

    # --------------------------------------------------------------- result ---
    @app.get("/tasks/{task_id}/result")
    async def get_result(task_id: str) -> Response:
        task = state.tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status in (TASK_PENDING, TASK_PROCESSING):
            return JSONResponse(
                status_code=202, content={**task.to_payload(), "message": "Task result is not ready yet"}
            )
        if task.status == TASK_FAILED:
            return JSONResponse(status_code=409, content={**task.to_payload(), "message": "Task execution failed"})
        body = state.result_payload or _minimal_zip(task_id)
        return Response(
            content=body,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{task_id}.zip"'},
        )

    async def _advance(task: _FakeTask, st: FakeWorkerState) -> None:
        """Simulate parse progress: pending → processing → completed."""
        try:
            if st.process_delay > 0:
                await asyncio.sleep(st.process_delay)
            task.status = TASK_PROCESSING
            task.started_at = _now_iso()
            if st.process_delay > 0:
                await asyncio.sleep(st.process_delay)
            task.status = TASK_COMPLETED
            task.completed_at = _now_iso()
            st.completed_count += 1
        except asyncio.CancelledError:
            raise

    return app
