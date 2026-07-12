"""Gateway smoke tests — health, attribution, MinerU pin, and FakeWorker protocol."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

# --- gateway app -------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_ok(client: AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    # MinerU §2: upstream attribution must be present on /health.
    assert body["upstream"]["name"] == "MinerU"
    assert body["upstream"]["version"]


@pytest.mark.asyncio
async def test_root_attribution(client: AsyncClient) -> None:
    resp = await client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["service"] == "mineru-gateway"
    assert body["upstream"]["name"] == "MinerU"


@pytest.mark.asyncio
async def test_attribution_headers_on_every_response(client: AsyncClient) -> None:
    """The middleware must stamp headers on every response (§2 obligation)."""
    resp = await client.get("/health")
    assert resp.headers["x-powered-by"] == "MinerU"
    assert resp.headers["mineru-version"]  # present and non-empty


# --- mineru compat / pin -----------------------------------------------------


def test_mineru_version_pin() -> None:
    """Startup validation must accept the pinned MinerU version."""
    from mineru_gateway.mineru_compat import validate_mineru_compat

    assert validate_mineru_compat() == "3.4.4"


# --- FakeWorker (Tier-1) -----------------------------------------------------


@pytest.mark.asyncio
async def test_fake_worker_health(fake_worker_client: AsyncClient) -> None:
    resp = await fake_worker_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    # Exact shape read from ../MinerU/mineru/cli/fast_api.py:health_check.
    assert body["status"] == "healthy"
    assert body["protocol_version"] == 2
    for key in (
        "queued_tasks",
        "processing_tasks",
        "completed_tasks",
        "failed_tasks",
        "max_concurrent_requests",
        "processing_window_size",
    ):
        assert key in body, f"FakeWorker /health missing {key}"


@pytest.mark.asyncio
async def test_fake_worker_submit_status_result(fake_worker_client: AsyncClient, fake_worker_state) -> None:
    # Submit a task via multipart (matches POST /tasks form).
    resp = await fake_worker_client.post(
        "/tasks",
        files=[("files", ("doc.pdf", b"%PDF-1.4 fake", "application/pdf"))],
        data={"backend": "pipeline", "effort": "medium", "parse_method": "auto"},
    )
    assert resp.status_code == 202
    task = resp.json()
    assert task["status"] in ("pending", "processing", "completed")
    assert task["file_names"] == ["doc.pdf"]

    # Give the scripted lifecycle a moment to complete.
    import asyncio

    await asyncio.sleep(0.05)

    status = (await fake_worker_client.get(f"/tasks/{task['task_id']}")).json()
    assert status["status"] == "completed"

    result = await fake_worker_client.get(f"/tasks/{task['task_id']}/result")
    assert result.status_code == 200
    assert result.headers["content-type"] == "application/zip"
    assert result.content[:2] == b"PK"  # ZIP magic


@pytest.mark.asyncio
async def test_fake_worker_scripted_failure(fake_worker_client: AsyncClient, fake_worker_state) -> None:
    fake_worker_state.fail_next = 1
    resp = await fake_worker_client.post("/tasks", files=[("files", ("doc.pdf", b"x", "application/pdf"))])
    assert resp.status_code == 202
    task_id = resp.json()["task_id"]

    status = (await fake_worker_client.get(f"/tasks/{task_id}")).json()
    assert status["status"] == "failed"
    assert status["error"] == "scripted failure"

    result = await fake_worker_client.get(f"/tasks/{task_id}/result")
    assert result.status_code == 409  # MinerU returns 409 for failed tasks.
