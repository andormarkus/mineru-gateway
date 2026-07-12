"""Google Cloud Storage object store (not yet implemented).

SDK mapping:
  put/get/delete → google.cloud.storage.Client (async via asyncio.to_thread or gcloud-aio-storage)
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from mineru_gateway.cloud.base import CloudStorageProvider


class GcsObjectStore(CloudStorageProvider):
    """GCS — stub until google-cloud-storage is wired."""

    def __init__(self, *, bucket: str) -> None:
        self._bucket = bucket

    @property
    def name(self) -> str:
        return "gcp"

    async def put(
        self,
        key: str,
        data: bytes | AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> int:
        raise NotImplementedError("GCS object store not yet implemented")

    async def get(self, key: str) -> bytes:
        raise NotImplementedError("GCS object store not yet implemented")

    def stream(self, key: str, *, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        raise NotImplementedError("GCS object store not yet implemented")

    async def exists(self, key: str) -> bool:
        raise NotImplementedError("GCS object store not yet implemented")

    async def delete(self, key: str) -> bool:
        raise NotImplementedError("GCS object store not yet implemented")

    async def head(self, key: str) -> dict[str, Any]:
        raise NotImplementedError("GCS object store not yet implemented")
