"""Azure Blob object store (not yet implemented).

SDK mapping:
  put/get/delete → azure.storage.blob.aio.BlobClient / ContainerClient
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from mineru_gateway.cloud.base import CloudStorageProvider


class AzureBlobStore(CloudStorageProvider):
    """Azure Blob Storage — stub until azure-storage-blob is wired."""

    def __init__(self, *, container: str, account_url: str | None = None) -> None:
        self._container = container
        self._account_url = account_url

    @property
    def name(self) -> str:
        return "azure"

    async def put(
        self,
        key: str,
        data: bytes | AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> int:
        raise NotImplementedError("Azure Blob store not yet implemented")

    async def get(self, key: str) -> bytes:
        raise NotImplementedError("Azure Blob store not yet implemented")

    def stream(self, key: str, *, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        raise NotImplementedError("Azure Blob store not yet implemented")

    async def exists(self, key: str) -> bool:
        raise NotImplementedError("Azure Blob store not yet implemented")

    async def delete(self, key: str) -> bool:
        raise NotImplementedError("Azure Blob store not yet implemented")

    async def head(self, key: str) -> dict[str, Any]:
        raise NotImplementedError("Azure Blob store not yet implemented")
