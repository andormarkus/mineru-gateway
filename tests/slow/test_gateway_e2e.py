"""Gateway → scheduler → external mineru-api end-to-end tests."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from tests.helpers.scheduler import pre_autoscaling_scheduler_loop
from tests.helpers.tasks import wait_for_task_status

pytestmark = pytest.mark.slow


@pytest.mark.asyncio
async def test_tasks_async_through_gateway(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
    sample_pdf: tuple[str, bytes],
    worker_timeout_seconds: float,
) -> None:
    """POST /tasks → scheduler dispatch → external worker → S3 result."""
    filename, pdf_bytes = sample_pdf
    client, _, store = gateway_with_external_worker
    settings = get_settings()

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        resp = await client.post(
            "/tasks",
            files=[("files", (filename, pdf_bytes, "application/pdf"))],
            data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
        )
        assert resp.status_code == 202, resp.text
        task_id = resp.json()["task_id"]

        row = await wait_for_task_status(task_id, expected="completed", timeout_seconds=worker_timeout_seconds)
        assert row.result_key

        result_resp = await client.get(f"/tasks/{task_id}/result")
        assert result_resp.status_code == 200, result_resp.text
        assert len(result_resp.content) > 0


@pytest.mark.asyncio
async def test_v1_ocr_through_gateway(
    gateway_with_external_worker: tuple[AsyncClient, str, CloudStorageProvider],
    sample_pdf: tuple[str, bytes],
    worker_timeout_seconds: float,
) -> None:
    """POST /v1/ocr blocks until the external worker returns normalized pages."""
    import base64

    filename, pdf_bytes = sample_pdf
    client, _, store = gateway_with_external_worker
    settings = get_settings()

    async with pre_autoscaling_scheduler_loop(settings, store, client_timeout=worker_timeout_seconds):
        resp = await client.post(
            "/v1/ocr",
            json={
                "model": "mineru",
                "document": {"type": "file", "file": base64.b64encode(pdf_bytes).decode(), "file_name": filename},
                "backend": "pipeline",
            },
            timeout=worker_timeout_seconds + 30.0,
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["model"] == "mineru"
    assert isinstance(body["pages"], list)
    assert len(body["pages"]) >= 1
