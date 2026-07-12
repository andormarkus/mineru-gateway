"""Cloud provider registry unit tests."""

from __future__ import annotations

from pathlib import Path

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


def test_available_providers_includes_aws() -> None:
    assert available_providers() == ["aws"]
    assert available_store_providers() == ["aws"]


def test_get_provider_aws() -> None:
    provider = get_provider("aws")
    assert isinstance(provider, AwsEc2Provider)
    assert provider.name == "aws"


def test_get_store_aws() -> None:
    store = get_store("aws")
    assert isinstance(store, S3ObjectStore)
    assert store.name == "aws"


def test_get_provider_unknown_raises() -> None:
    with pytest.raises(ValueError, match="not implemented"):
        get_provider("unknown-vendor")


def test_get_store_unknown_raises() -> None:
    with pytest.raises(ValueError, match="not implemented"):
        get_store("unknown-vendor")


def test_init_provider_returns_aws() -> None:
    provider = init_provider()
    assert isinstance(provider, AwsEc2Provider)


@pytest.mark.asyncio
async def test_init_store_none_when_bucket_unconfigured(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        "cloud:\n  provider: aws\n  aws:\n    bucket: ''\n",
        encoding="utf-8",
    )
    reset_settings_cache()
    load_settings(config)
    assert await init_store() is None
    reset_settings_cache()


@pytest.mark.asyncio
async def test_init_store_moto(moto_s3_endpoint: str, tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"cloud:\n  provider: aws\n  aws:\n    bucket: registry-test\n    endpoint_url: {moto_s3_endpoint}\n",
        encoding="utf-8",
    )
    reset_settings_cache()
    load_settings(config)
    store = await init_store()
    assert store is not None
    assert store.name == "aws"
    await store.put("probe", b"ok")
    assert await store.get("probe") == b"ok"
    reset_settings_cache()
