"""Direct mineru-api protocol checks against an external worker.

These tests hit the worker URL only (no gateway/scheduler). Use them to verify
connectivity and protocol compatibility before running full gateway E2E tests.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from mineru.cli.api_protocol import API_PROTOCOL_VERSION

pytestmark = pytest.mark.slow


@pytest.mark.asyncio
async def test_worker_health(worker_url: str) -> None:
    """External worker exposes /health with protocol_version 2."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{worker_url}/health")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["protocol_version"] == API_PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_worker_submit_and_result(
    worker_url: str,
    sample_pdf: tuple[str, bytes],
    worker_timeout_seconds: float,
) -> None:
    """Submit a PDF to the worker directly and fetch the result ZIP."""
    filename, pdf_bytes = sample_pdf
    async with httpx.AsyncClient(timeout=worker_timeout_seconds) as client:
        submit = await client.post(
            f"{worker_url}/tasks",
            files=[("files", (filename, pdf_bytes, "application/pdf"))],
            data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
        )
        assert submit.status_code == 202, submit.text
        task_id = submit.json()["task_id"]

        deadline = asyncio.get_running_loop().time() + worker_timeout_seconds
        status_body: dict = {}
        while asyncio.get_running_loop().time() < deadline:
            status = await client.get(f"{worker_url}/tasks/{task_id}")
            assert status.status_code == 200, status.text
            status_body = status.json()
            if status_body["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(1.0)
        else:
            pytest.fail(f"task {task_id} did not finish within {worker_timeout_seconds}s: {status_body}")

        assert status_body["status"] == "completed", status_body

        result = await client.get(f"{worker_url}/tasks/{task_id}/result")
        assert result.status_code == 200, result.text
        assert len(result.content) > 0
        assert result.headers.get("content-type", "").startswith("application/")
