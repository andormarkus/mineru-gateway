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
from mineru_gateway.cloud.errors import CloudError, CloudErrorCategory

logger = logging.getLogger(__name__)

_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey"})
_MISSING_BUCKET_CODES = frozenset({"404", "NoSuchBucket"})
_AUTH_ERROR_CODES = frozenset({"403", "AccessDenied"})


def _s3_error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def _classify_s3_prepare_error(exc: ClientError) -> CloudError:
    code = _s3_error_code(exc)
    message = exc.response.get("Error", {}).get("Message", str(exc))
    if code in _AUTH_ERROR_CODES:
        return CloudError(category=CloudErrorCategory.AUTH, code=code, message=message, retryable=False)
    if code in _MISSING_BUCKET_CODES:
        return CloudError(category=CloudErrorCategory.NOT_FOUND, code=code, message=message, retryable=False)
    return CloudError(category=CloudErrorCategory.UNKNOWN, code=code, message=message, retryable=False)


class S3ObjectStore(CloudStorageProvider):
    """S3-compatible object store backed by aioboto3."""

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        region: str = "us-east-1",
        multipart_part_size_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region = region
        self._part_size = max(multipart_part_size_bytes, 5 * 1024 * 1024)
        self._session = aioboto3.Session(region_name=region)

    @property
    def name(self) -> str:
        return "aws"

    def _client(self) -> Any:
        return self._session.client("s3", endpoint_url=self._endpoint_url)

    async def prepare(self) -> None:
        try:
            async with self._client() as s3:
                await s3.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            raise _classify_s3_prepare_error(exc) from exc
        logger.info("S3 bucket validated: bucket=%s endpoint=%s", self._bucket, self._endpoint_url or "aws")

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
            body = bytes(data)
            kwargs["Body"] = body
            async with self._client() as s3:
                await s3.put_object(**kwargs)
            logger.debug("S3 put: bucket=%s key=%s bytes=%d", self._bucket, key, len(body))
            return len(body)

        async with self._client() as s3:
            nbytes = await self._put_multipart(s3, key, data, content_type=content_type, metadata=metadata)
        logger.debug("S3 put (stream): bucket=%s key=%s bytes=%d", self._bucket, key, nbytes)
        return nbytes

    async def _put_multipart(
        self, s3: Any, key: str, data: AsyncIterator[bytes], *, content_type: str, metadata: dict[str, str] | None
    ) -> int:
        upload_id: str | None = None
        parts: list[dict[str, Any]] = []
        total_bytes = 0
        part_number = 1
        buffer = bytearray()

        create_kwargs: dict[str, Any] = {"Bucket": self._bucket, "Key": key, "ContentType": content_type}
        if metadata:
            create_kwargs["Metadata"] = metadata

        async def ensure_upload_started() -> None:
            nonlocal upload_id
            if upload_id is not None:
                return
            create_resp = await s3.create_multipart_upload(**create_kwargs)
            upload_id = create_resp["UploadId"]

        try:
            async for chunk in data:
                if not chunk:
                    continue
                buffer.extend(chunk)
                while len(buffer) >= self._part_size:
                    await ensure_upload_started()
                    part_body = bytes(buffer[: self._part_size])
                    del buffer[: self._part_size]
                    resp = await s3.upload_part(
                        Bucket=self._bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=part_body
                    )
                    parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                    total_bytes += len(part_body)
                    part_number += 1

            if upload_id is None:
                body = bytes(buffer)
                await s3.put_object(**create_kwargs, Body=body)
                return len(body)

            if buffer:
                final_part = bytes(buffer)
                resp = await s3.upload_part(
                    Bucket=self._bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=final_part
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                total_bytes += len(final_part)

            await s3.complete_multipart_upload(
                Bucket=self._bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
            return total_bytes
        except Exception:
            if upload_id is not None:
                try:
                    await s3.abort_multipart_upload(Bucket=self._bucket, Key=key, UploadId=upload_id)
                except Exception:
                    logger.warning(
                        "S3 abort_multipart_upload failed: bucket=%s key=%s upload_id=%s",
                        self._bucket,
                        key,
                        upload_id,
                        exc_info=True,
                    )
            raise

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
                            "S3 stream: iter_chunks unsupported for bucket=%s key=%s — buffered read", self._bucket, key
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

    async def copy(self, source: str, destination: str) -> None:
        copy_source = {"Bucket": self._bucket, "Key": source}
        async with self._client() as s3:
            await s3.copy_object(Bucket=self._bucket, Key=destination, CopySource=copy_source)
        logger.debug("S3 copy: bucket=%s %s -> %s", self._bucket, source, destination)

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
