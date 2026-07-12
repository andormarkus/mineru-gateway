"""AWS S3 object store via aioboto3 (genuinely async).

Works with AWS S3 in prod and SeaweedFS mini mode in dev/test. Credentials resolve via the AWS default credential chain.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

import aioboto3
from botocore.exceptions import ClientError

from mineru_gateway.cloud.base import CloudStorageProvider

logger = logging.getLogger(__name__)

_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey"})


def _s3_error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


class S3ObjectStore(CloudStorageProvider):
    """S3-compatible object store backed by aioboto3."""

    def __init__(self, bucket: str, *, endpoint_url: str | None = None, region: str = "us-east-1") -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region = region
        self._session = aioboto3.Session(region_name=region)

    @property
    def name(self) -> str:
        return "aws"

    def _client(self) -> Any:
        return self._session.client("s3", endpoint_url=self._endpoint_url)

    async def prepare(self) -> None:
        await self.ensure_bucket()

    async def ensure_bucket(self) -> None:
        try:
            async with self._client() as s3:
                await s3.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            err = exc.response.get("Error", {}).get("Code", "")
            if err in ("404", "NoSuchBucket", "403"):
                async with self._client() as s3:
                    await s3.create_bucket(Bucket=self._bucket)
                logger.info("Created S3 bucket %s (endpoint=%s)", self._bucket, self._endpoint_url or "aws")
            else:
                logger.warning(
                    "S3 ensure_bucket failed: bucket=%s code=%s",
                    self._bucket,
                    err,
                    exc_info=True,
                )
                raise

    async def put(
        self,
        key: str,
        data: bytes | AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> int:
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Key": key, "ContentType": content_type}
        if metadata:
            kwargs["Metadata"] = metadata

        if isinstance(data, (bytes, bytearray)):
            kwargs["Body"] = bytes(data)
            async with self._client() as s3:
                await s3.put_object(**kwargs)
            logger.debug("S3 put: bucket=%s key=%s bytes=%d", self._bucket, key, len(data))
            return len(data)

        body = b""
        async for chunk in data:
            body += chunk
        kwargs["Body"] = body
        async with self._client() as s3:
            await s3.put_object(**kwargs)
        logger.debug("S3 put (stream): bucket=%s key=%s bytes=%d", self._bucket, key, len(body))
        return len(body)

    async def get(self, key: str) -> bytes:
        try:
            async with self._client() as s3:
                resp = await s3.get_object(Bucket=self._bucket, Key=key)
                async with resp["Body"] as stream:
                    data = await stream.read()
            logger.debug("S3 get: bucket=%s key=%s bytes=%d", self._bucket, key, len(data))
            return data
        except ClientError as exc:
            if _s3_error_code(exc) in _MISSING_OBJECT_CODES:
                raise KeyError(key) from exc
            logger.warning("S3 get failed: bucket=%s key=%s", self._bucket, key, exc_info=True)
            raise

    async def stream(self, key: str, *, chunk_size: int = 65536) -> AsyncIterator[bytes]:  # type: ignore[override]
        try:
            async with self._client() as s3:
                resp = await s3.get_object(Bucket=self._bucket, Key=key)
                body = resp["Body"]
                if hasattr(body, "iter_chunks"):
                    try:
                        async for chunk in body.iter_chunks(chunk_size):
                            yield chunk
                        return
                    except TypeError:
                        # moto returns aiohttp ClientResponse — no chunked read API.
                        logger.debug(
                            "S3 stream: iter_chunks unsupported for bucket=%s key=%s — buffered read",
                            self._bucket,
                            key,
                        )
                async with body as stream:
                    data = await stream.read()
                for offset in range(0, len(data), chunk_size):
                    yield data[offset : offset + chunk_size]
        except ClientError as exc:
            if _s3_error_code(exc) in _MISSING_OBJECT_CODES:
                raise KeyError(key) from exc
            logger.warning("S3 stream failed: bucket=%s key=%s", self._bucket, key, exc_info=True)
            raise

    async def exists(self, key: str) -> bool:
        try:
            async with self._client() as s3:
                await s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if _s3_error_code(exc) in _MISSING_OBJECT_CODES:
                return False
            logger.warning("S3 exists failed: bucket=%s key=%s", self._bucket, key, exc_info=True)
            raise

    async def delete(self, key: str) -> bool:
        async with self._client() as s3:
            await s3.delete_object(Bucket=self._bucket, Key=key)
        logger.debug("S3 delete: bucket=%s key=%s", self._bucket, key)
        return True

    async def head(self, key: str) -> dict[str, Any]:
        try:
            async with self._client() as s3:
                resp = await s3.head_object(Bucket=self._bucket, Key=key)
            return {
                "size": resp.get("ContentLength"),
                "content_type": resp.get("ContentType"),
                "metadata": resp.get("Metadata", {}),
                "last_modified": resp.get("LastModified"),
            }
        except ClientError as exc:
            if _s3_error_code(exc) in _MISSING_OBJECT_CODES:
                raise KeyError(key) from exc
            logger.warning("S3 head failed: bucket=%s key=%s", self._bucket, key, exc_info=True)
            raise
