"""Cloud provider registry unit tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mineru_gateway.cloud.aws.ec2 import AwsEc2Provider
from mineru_gateway.cloud.aws.s3 import S3ObjectStore
from mineru_gateway.cloud.registry import (
    available_providers,
    available_store_providers,
    get_provider,
    get_store,
    init_provider,
    init_store,
)
from mineru_gateway.config import load_settings, reset_settings_cache


def _create_s3_bucket(endpoint: str, bucket: str, *, region: str = "us-east-1") -> None:
    import boto3

    boto3.client("s3", endpoint_url=endpoint, region_name=region).create_bucket(Bucket=bucket)


def test_available_providers_includes_aws() -> None:
    assert available_providers() == ["aws"]
    assert available_store_providers() == ["aws"]


def test_get_provider_aws() -> None:
    settings = load_settings()
    provider = get_provider("aws", settings=settings)
    assert isinstance(provider, AwsEc2Provider)
    assert provider.name == "aws"


def test_get_store_aws() -> None:
    settings = load_settings()
    store = get_store("aws", settings=settings)
    assert isinstance(store, S3ObjectStore)
    assert store.name == "aws"


def test_get_provider_unknown_raises() -> None:
    settings = load_settings()
    with pytest.raises(ValueError, match="not implemented"):
        get_provider("unknown-vendor", settings=settings)


def test_get_store_unknown_raises() -> None:
    settings = load_settings()
    with pytest.raises(ValueError, match="not implemented"):
        get_store("unknown-vendor", settings=settings)


def test_init_provider_returns_none_when_cloud_workers_disabled() -> None:
    settings = load_settings()
    provider = init_provider(settings)
    assert provider is None


@pytest.mark.asyncio
async def test_init_store_raises_when_bucket_unconfigured() -> None:
    settings = MagicMock()
    settings.cloud.is_object_store_configured.return_value = False
    settings.cloud.provider = "aws"
    with pytest.raises(RuntimeError, match="required"):
        await init_store(settings)


@pytest.mark.asyncio
async def test_init_store_moto(moto_s3_endpoint: str, tmp_path: Path) -> None:
    _create_s3_bucket(moto_s3_endpoint, "registry-test")
    config = tmp_path / "config.yaml"
    config.write_text(
        f"cloud:\n  provider: aws\n  aws:\n    bucket: registry-test\n    endpoint_url: {moto_s3_endpoint}\n",
        encoding="utf-8",
    )
    reset_settings_cache()
    settings = load_settings(config)
    store = await init_store(settings)
    assert store is not None
    assert store.name == "aws"
    await store.put("probe", b"ok")
    assert await store.get("probe") == b"ok"
    reset_settings_cache()
