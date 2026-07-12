"""S3ObjectStore unit tests via moto ThreadedMotoServer + aioboto3."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from mineru_gateway.cloud.aws.s3 import S3ObjectStore


@pytest.fixture
def s3_store(moto_s3_endpoint: str) -> S3ObjectStore:
    return S3ObjectStore(bucket="unit-test-bucket", endpoint_url=moto_s3_endpoint, region="us-east-1")


@pytest.mark.asyncio
async def test_prepare_creates_bucket(s3_store: S3ObjectStore) -> None:
    await s3_store.prepare()


@pytest.mark.asyncio
async def test_put_get_delete_roundtrip(s3_store: S3ObjectStore) -> None:
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
async def test_put_async_iterator(s3_store: S3ObjectStore) -> None:
    await s3_store.prepare()

    async def chunks() -> AsyncIterator[bytes]:
        yield b"part-a"
        yield b"part-b"

    nbytes = await s3_store.put("stream-put", chunks())
    assert nbytes == 12
    assert await s3_store.get("stream-put") == b"part-apart-b"


@pytest.mark.asyncio
async def test_stream_read(s3_store: S3ObjectStore) -> None:
    await s3_store.prepare()
    payload = b"x" * 10000
    await s3_store.put("stream-key", payload)

    parts: list[bytes] = []
    async for chunk in s3_store.stream("stream-key", chunk_size=1024):
        parts.append(chunk)
    assert b"".join(parts) == payload


@pytest.mark.asyncio
async def test_missing_key_raises_keyerror(s3_store: S3ObjectStore) -> None:
    await s3_store.prepare()
    with pytest.raises(KeyError):
        await s3_store.get("missing")
    with pytest.raises(KeyError):
        await s3_store.head("missing")

    parts: list[bytes] = []
    with pytest.raises(KeyError):
        async for chunk in s3_store.stream("missing"):
            parts.append(chunk)
