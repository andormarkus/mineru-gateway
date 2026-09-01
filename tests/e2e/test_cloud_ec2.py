"""AWS EC2 launch, dispatch, and teardown end-to-end tests."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider
from mineru_gateway.config import get_settings
from tests.e2e.conftest import SAMPLE_PDF
from tests.helpers.cloud import scheduler_poll_interval_seconds
from tests.helpers.scheduler import full_scheduler_loop
from tests.helpers.tasks import wait_for_task_status_api

pytestmark = pytest.mark.e2e

_PDF_BYTES = SAMPLE_PDF.read_bytes()
_PDF_NAME = SAMPLE_PDF.name
_FORM = {"backend": "pipeline", "effort": "medium", "parse_method": "auto"}


@pytest.mark.asyncio
async def test_ec2_worker_is_healthy(gateway_cloud_e2e_session: tuple[AsyncClient, CloudStorageProvider]) -> None:
    """Worker was provisioned once in the session fixture before tests run."""
    client, _ = gateway_cloud_e2e_session
    settings = get_settings()

    workers = await client.get("/admin/workers")
    assert workers.status_code == 200, workers.text
    body = workers.json()
    assert body["workers"], body
    healthy = [w for w in body["workers"] if w.get("healthy")]
    assert healthy, body
    assert healthy[0].get("base_url"), healthy[0]
    assert settings.scaling.min_workers >= 1


@pytest.mark.asyncio
async def test_ec2_submit_task_complete(
    gateway_cloud_e2e_session: tuple[AsyncClient, CloudStorageProvider], e2e_worker_timeout_seconds: float
) -> None:
    """Dispatch to the warm session worker and fetch the S3 result."""
    client, store = gateway_cloud_e2e_session
    settings = get_settings()
    poll = scheduler_poll_interval_seconds()
    scheduler_poll = settings.scheduler.reconcile_poll_interval_seconds

    async with full_scheduler_loop(
        settings, store, interval_seconds=scheduler_poll, client_timeout=e2e_worker_timeout_seconds
    ):
        resp = await client.post("/tasks", files=[("files", (_PDF_NAME, _PDF_BYTES, "application/pdf"))], data=_FORM)
        assert resp.status_code == 202, resp.text
        task_id = resp.json()["task_id"]

        await wait_for_task_status_api(
            client, task_id, expected="completed", timeout_seconds=e2e_worker_timeout_seconds, poll_interval=poll
        )

        result_resp = await client.get(f"/tasks/{task_id}/result")
        assert result_resp.status_code == 200, result_resp.text
        assert len(result_resp.content) > 0
