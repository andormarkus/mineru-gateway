"""Fixtures for Tier-3 tests against an external mineru-api worker.

Prerequisites:
  - ``task up`` — local SeaweedFS (S3 on :8333)
  - ``MINERU_TEST_WORKER_URL`` — reachable mineru-api worker base URL

Uses file-backed SQLite + real SeaweedFS (no moto). EC2 autoscaling is disabled.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from mineru_gateway.cloud.base import CloudStorageProvider

pytestmark = pytest.mark.slow

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
DEFAULT_SEAWEEDFS_ENDPOINT = "http://localhost:8333"
DEFAULT_S3_BUCKET = "mineru-slow-test"

# Real-world PDFs downloaded from public sample URLs (see tests/fixtures/).
SAMPLE_PDFS: list[tuple[str, Path]] = [
    ("pdf_sample_1.pdf", FIXTURES_DIR / "pdf_sample_1.pdf"),
    ("pdf_sample_2.pdf", FIXTURES_DIR / "pdf_sample_2.pdf"),
    ("pdf_sample_3.pdf", FIXTURES_DIR / "pdf_sample_3.pdf"),
]


def pytest_configure(config: pytest.Config) -> None:
    """Register slow marker usage for this package (also declared in pyproject.toml)."""
    config.addinivalue_line("markers", "slow: tests requiring a real mineru-api worker (Tier-3)")


def _seaweedfs_endpoint() -> str:
    return os.environ.get("MINERU_GATEWAY_CLOUD__AWS__ENDPOINT_URL", DEFAULT_SEAWEEDFS_ENDPOINT).strip().rstrip("/")


def _seaweedfs_bucket() -> str:
    return os.environ.get("MINERU_GATEWAY_CLOUD__AWS__BUCKET", DEFAULT_S3_BUCKET).strip()


def _check_seaweedfs_reachable(endpoint: str) -> bool:
    try:
        with httpx.Client(timeout=3.0) as client:
            resp = client.get(f"{endpoint}/status")
            return resp.status_code == 200
    except httpx.HTTPError:
        return False


def _ensure_s3_bucket(endpoint: str, bucket: str) -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def _apply_slow_test_env(
    monkeypatch: pytest.MonkeyPatch, *, database_url: str, s3_endpoint: str, s3_bucket: str
) -> None:
    """Configure gateway for local sqlite + SeaweedFS; disable EC2 autoscaling."""
    monkeypatch.setenv("MINERU_GATEWAY_DATABASE_URL", database_url)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD_WORKERS_ENABLED", "false")
    monkeypatch.setenv("MINERU_GATEWAY_AUTH__ENABLED", "false")
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__ENDPOINT_URL", s3_endpoint)
    monkeypatch.setenv("MINERU_GATEWAY_CLOUD__AWS__BUCKET", s3_bucket)
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__MIN_WORKERS", "0")
    monkeypatch.setenv("MINERU_GATEWAY_SCALING__MAX_WORKERS", "1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


@pytest.fixture(scope="session")
def worker_url() -> str:
    """Base URL of the external mineru-api worker (no trailing slash)."""
    url = os.environ.get("MINERU_TEST_WORKER_URL", "").strip().rstrip("/")
    if not url:
        pytest.skip("MINERU_TEST_WORKER_URL is not set — provide a running mineru-api worker URL")
    return url


@pytest.fixture(scope="session")
def worker_timeout_seconds() -> float:
    """How long to wait for real ML parsing to finish."""
    raw = os.environ.get("MINERU_TEST_WORKER_TIMEOUT", "300")
    return float(raw)


@pytest.fixture(scope="session")
def seaweedfs_endpoint() -> str:
    """Local SeaweedFS S3 endpoint (``task up``)."""
    endpoint = _seaweedfs_endpoint()
    if not _check_seaweedfs_reachable(endpoint):
        pytest.skip(f"SeaweedFS is not reachable at {endpoint} — run `task up` first")
    return endpoint


@pytest.fixture(scope="session")
def slow_s3_bucket(seaweedfs_endpoint: str) -> str:
    """S3 bucket on local SeaweedFS, created once per test session."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    bucket = _seaweedfs_bucket()
    _ensure_s3_bucket(seaweedfs_endpoint, bucket)
    return bucket


@pytest.fixture
def sqlite_database_url(tmp_path: Path) -> str:
    """Per-test file-backed SQLite database URL."""
    db_path = tmp_path / "gateway.db"
    return f"sqlite+aiosqlite:///{db_path}"


@pytest.fixture(params=SAMPLE_PDFS, ids=[name for name, _ in SAMPLE_PDFS])
def sample_pdf(request: pytest.FixtureRequest) -> tuple[str, bytes]:
    """A real-world sample PDF (filename, bytes) for parsing tests."""
    name, path = request.param
    return name, path.read_bytes()


@asynccontextmanager
async def _gateway_stack(
    worker_url: str,
    seaweedfs_endpoint: str,
    slow_s3_bucket: str,
    sqlite_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    worker_healthy: bool,
    worker_ready: bool,
) -> AsyncIterator[tuple[AsyncClient, str, CloudStorageProvider]]:
    from mineru_gateway.config import load_settings, reset_settings_cache
    from mineru_gateway.db.base import get_db_session, init_engine, shutdown_engine
    from mineru_gateway.gateway.app import create_app
    from tests.db_helpers import create_all_tables
    from tests.helpers.worker import seed_worker_row

    _apply_slow_test_env(
        monkeypatch, database_url=sqlite_database_url, s3_endpoint=seaweedfs_endpoint, s3_bucket=slow_s3_bucket
    )
    reset_settings_cache()
    load_settings()

    await shutdown_engine()
    init_engine(sqlite_database_url)
    await create_all_tables()

    worker_id = "external-worker-1"

    application = create_app()
    async with application.router.lifespan_context(application):
        from mineru_gateway.config import get_settings

        store: CloudStorageProvider = application.state.object_store
        settings = get_settings()
        assert not settings.cloud_workers_enabled

        async with get_db_session() as session:
            await seed_worker_row(
                session,
                settings=settings,
                worker_id=worker_id,
                base_url=worker_url,
                healthy=worker_healthy,
                ready=worker_ready,
            )

        transport = ASGITransport(app=application)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, worker_id, store

    await shutdown_engine()


@pytest_asyncio.fixture
async def gateway_with_external_worker(
    worker_url: str,
    seaweedfs_endpoint: str,
    slow_s3_bucket: str,
    sqlite_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[AsyncClient, str, CloudStorageProvider]]:
    """Gateway with a pre-healthy worker row (skips waiting for the health probe)."""
    async with _gateway_stack(
        worker_url,
        seaweedfs_endpoint,
        slow_s3_bucket,
        sqlite_database_url,
        monkeypatch,
        worker_healthy=True,
        worker_ready=True,
    ) as stack:
        yield stack


@pytest_asyncio.fixture
async def gateway_with_unprobed_external_worker(
    worker_url: str,
    seaweedfs_endpoint: str,
    slow_s3_bucket: str,
    sqlite_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[AsyncClient, str, CloudStorageProvider]]:
    """Gateway with worker seeded unhealthy — scheduler must probe /health before dispatch."""
    async with _gateway_stack(
        worker_url,
        seaweedfs_endpoint,
        slow_s3_bucket,
        sqlite_database_url,
        monkeypatch,
        worker_healthy=False,
        worker_ready=False,
    ) as stack:
        yield stack
