"""S3ObjectStore unit tests via moto ThreadedMotoServer + aioboto3."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError

from mineru_gateway.cloud.aws.s3 import S3ObjectStore
from mineru_gateway.cloud.errors import CloudError, CloudErrorCategory


def _create_s3_bucket(endpoint: str, bucket: str, *, region: str = "us-east-1") -> None:
    import boto3

    boto3.client("s3", endpoint_url=endpoint, region_name=region).create_bucket(Bucket=bucket)


@pytest.fixture
def s3_store(moto_s3_endpoint: str) -> S3ObjectStore:
    return S3ObjectStore(bucket="unit-test-bucket", endpoint_url=moto_s3_endpoint, region="us-east-1")


@pytest.mark.asyncio
async def test_prepare_validates_existing_bucket(s3_store: S3ObjectStore, moto_s3_endpoint: str) -> None:
    _create_s3_bucket(moto_s3_endpoint, "unit-test-bucket")
    await s3_store.prepare()


@pytest.mark.asyncio
async def test_prepare_missing_bucket_raises(s3_store: S3ObjectStore) -> None:
    exc = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")
    original_client = s3_store._client

    class FakeS3:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        head_bucket = AsyncMock(side_effect=exc)

    s3_store._client = lambda: FakeS3()  # type: ignore[method-assign]
    try:
        with pytest.raises(CloudError) as exc_info:
            await s3_store.prepare()
        assert exc_info.value.category == CloudErrorCategory.NOT_FOUND
    finally:
        s3_store._client = original_client  # type: ignore[method-assign]


def test_prepare_403_is_auth_error() -> None:
    exc = ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadBucket")
    err = S3ObjectStore._classify_s3_prepare_error(exc)
    assert err.category == CloudErrorCategory.AUTH


@pytest.mark.asyncio
async def test_put_get_delete_roundtrip(s3_store: S3ObjectStore, moto_s3_endpoint: str) -> None:
    _create_s3_bucket(moto_s3_endpoint, "unit-test-bucket")
    await s3_store.prepare()
    await s3_store.put("obj-key", b"hello s3", content_type="text/plain")
    assert await s3_store.exists("obj-key")

    data = await s3_store.get("obj-key")
    assert data == b"hello s3"

    head = await s3_store.head("obj-key")
    assert head["size"] == 8
    assert head["content_type"] == "text/plain"

    await s3_store.delete("obj-key")
    assert not await s3_store.exists("obj-key")


@pytest.mark.asyncio
async def test_stream_read(s3_store: S3ObjectStore, moto_s3_endpoint: str) -> None:
    _create_s3_bucket(moto_s3_endpoint, "unit-test-bucket")
    await s3_store.prepare()
    payload = b"x" * 10000
    await s3_store.put("stream-key", payload)

    parts: list[bytes] = []
    async for chunk in s3_store.stream("stream-key", chunk_size=1024):
        parts.append(chunk)
    assert b"".join(parts) == payload


@pytest.mark.asyncio
async def test_missing_key_raises_keyerror(s3_store: S3ObjectStore, moto_s3_endpoint: str) -> None:
    _create_s3_bucket(moto_s3_endpoint, "unit-test-bucket")
    await s3_store.prepare()
    with pytest.raises(KeyError):
        await s3_store.get("missing")
    with pytest.raises(KeyError):
        await s3_store.head("missing")

    parts: list[bytes] = []
    with pytest.raises(KeyError):
        async for chunk in s3_store.stream("missing"):
            parts.append(chunk)
