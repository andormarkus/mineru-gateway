"""Regression tests for gateway bugbot findings."""

from __future__ import annotations

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from tests.fakes.store import InMemoryStore
from tests.fakes.worker import FakeWorkerState, create_fake_worker_app

from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task
from mineru_gateway.gateway.ingest import _hash_staged_payload, build_payload
from mineru_gateway.mineru_compat import MultipartPayload, StagedUpload
from mineru_gateway.protocol.ocr_models import OCRDocument, OCRRequest
from mineru_gateway.scheduler.cache_service import compute_cache_key, compute_file_records
from mineru_gateway.scheduler.task_repository import TaskRepository
from mineru_gateway.scheduler.worker_repository import WorkerRepository
from mineru_gateway.util.upload_paths import UnsafeUploadNameError


def test_lang_list_preserves_all_values_in_cache_key() -> None:
    """Repeated lang_list fields must all affect the dedup hash."""
    payload = MultipartPayload(
        temp_dir="/tmp",
        fields=[("backend", "pipeline"), ("lang_list", "en"), ("lang_list", "ch")],
        uploads=[
            StagedUpload(field_name="files", upload_name="doc.pdf", content_type="application/pdf", path=__file__)
        ],
    )
    _, options = _hash_staged_payload(payload, settings=get_settings())
    assert options["lang_list"] == ["ch", "en"]

    with open(__file__, "rb") as f:
        file_bytes = f.read()
    records = compute_file_records(["doc.pdf"], [file_bytes])
    key_single, _ = compute_cache_key(records, {"backend": "pipeline", "lang_list": ["en"]})
    key_multi, _ = compute_cache_key(records, {"backend": "pipeline", "lang_list": ["ch", "en"]})
    assert key_single != key_multi


def test_build_payload_uses_internal_upload_path() -> None:
    from pathlib import Path

    body = OCRRequest(document=OCRDocument(type="file", file="JVBERi0=", file_name="doc.pdf"), backend="pipeline")
    payload = build_payload(b"%PDF", "doc.pdf", body)
    try:
        upload = payload.uploads[0]
        assert upload.upload_name == "doc.pdf"
        assert Path(upload.path).resolve().is_relative_to(Path(payload.temp_dir).resolve())
        assert Path(upload.path).name != "doc.pdf"
    finally:
        payload.cleanup()


def test_build_payload_rejects_traversal_name() -> None:
    body = OCRRequest(
        document=OCRDocument(type="file", file="JVBERi0=", file_name="../../evil.pdf"), backend="pipeline"
    )
    with pytest.raises(UnsafeUploadNameError):
        build_payload(b"%PDF", "../../evil.pdf", body)


@pytest.mark.asyncio
async def test_refresh_task_from_upstream_marks_terminal(db_session: AsyncSession) -> None:
    """Dispatched tasks mirror upstream completion into the gateway DB."""
    import asyncio
    import contextlib
    import socket

    import uvicorn

    fake_state = FakeWorkerState(process_delay=0.0)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    base_url = f"http://127.0.0.1:{port}"

    server = uvicorn.Server(
        uvicorn.Config(create_fake_worker_app(fake_state), host="127.0.0.1", port=port, log_level="warning")
    )
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.3)

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            submit = await client.post(
                f"{base_url}/tasks",
                files=[("files", ("doc.pdf", b"%PDF", "application/pdf"))],
                data={"backend": "pipeline"},
            )
            upstream_task_id = submit.json()["task_id"]

            async with get_db_session() as session:
                session.add(
                    Task(
                        task_id="gw-1",
                        status="processing",
                        upstream_task_id=upstream_task_id,
                        upstream_base_url=base_url,
                        backend="pipeline",
                        file_names=["doc.pdf"],
                    )
                )
                await session.commit()

            await asyncio.sleep(0.2)
            repo = TaskRepository(get_settings(), InMemoryStore(), client, WorkerRepository(get_settings()))
            row = await repo.refresh_task_from_upstream("gw-1")
            assert row is not None
            assert row.status == "storing_result"
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


@pytest.mark.asyncio
async def test_startup_fails_without_object_store() -> None:
    """Gateway lifespan rejects startup when durable storage is unavailable."""
    import os
    from unittest.mock import AsyncMock, patch

    from tests.db_helpers import create_all_tables

    from mineru_gateway.config import load_settings, reset_settings_cache
    from mineru_gateway.db.base import init_engine, shutdown_engine
    from mineru_gateway.gateway.app import create_app
    from mineru_gateway.startup_guard import StartupDependencyError

    os.environ["MINERU_GATEWAY_DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    reset_settings_cache()
    load_settings()
    init_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables()

    application = create_app()
    with (
        patch("mineru_gateway.gateway.app.init_store", new=AsyncMock(return_value=None)),
        pytest.raises(StartupDependencyError, match="Object storage is required"),
    ):
        async with application.router.lifespan_context(application):
            pass
    await shutdown_engine()
