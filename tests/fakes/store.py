"""In-memory CloudStorageProvider for unit tests (no S3/moto required)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from mineru_gateway.cloud.base import CloudStorageProvider


class InMemoryStore(CloudStorageProvider):
    """Trivial in-memory store implementing the production ABC."""

    def __init__(self, *, name: str = "memory") -> None:
        self._name = name
        self._data: dict[str, bytes] = {}

    @property
    def name(self) -> str:
        return self._name

    async def put(
        self,
        key: str,
        data: bytes | AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> int:
        if isinstance(data, (bytes, bytearray)):
            self._data[key] = bytes(data)
        else:
            body = b""
            async for chunk in data:
                body += chunk
            self._data[key] = body
        return len(self._data[key])

    async def get(self, key: str) -> bytes:
        if key not in self._data:
            raise KeyError(key)
        return self._data[key]

    async def stream(self, key: str, *, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        if key not in self._data:
            raise KeyError(key)
        yield self._data[key]

    async def exists(self, key: str) -> bool:
        return key in self._data

    async def delete(self, key: str) -> bool:
        return self._data.pop(key, None) is not None

    async def head(self, key: str) -> dict[str, object]:
        if key not in self._data:
            raise KeyError(key)
        return {"size": len(self._data[key])}
