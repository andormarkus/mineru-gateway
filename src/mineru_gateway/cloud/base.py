"""Cloud provider ABCs — compute (VM lifecycle) and storage (object/blob)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from mineru_gateway.cloud.types import DiscoveredInstance, InstanceState


class ComputeProvider(ABC):
    """Abstract cloud worker lifecycle manager."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name ('aws', 'azure', 'gcp')."""

    @abstractmethod
    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        """Return controller-managed VMs tagged for ``deployment_id``."""

    @abstractmethod
    async def launch(self, worker_id: str, *, deployment_id: str, generation: int) -> str:
        """Provision a new VM. Returns instance_id. Uses worker_id as idempotency token."""

    @abstractmethod
    async def start(self, instance_id: str) -> None:
        """Power on a stopped VM."""

    @abstractmethod
    async def stop(self, instance_id: str) -> None:
        """Stop a running VM without destroying it."""

    @abstractmethod
    async def terminate(self, instance_id: str) -> None:
        """Permanently destroy a VM."""

    @abstractmethod
    async def get_state(self, instance_id: str) -> InstanceState:
        """Return normalized instance state."""

    @abstractmethod
    async def get_private_ip(self, instance_id: str) -> str | None:
        """Return private IPv4 for worker base_url, or None if not assigned."""


class CloudStorageProvider(ABC):
    """Abstract cloud object/blob store — payloads and results."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name ('aws', 'azure', 'gcp')."""

    async def prepare(self) -> None:
        """Optional startup hook (e.g. validate bucket exists)."""
        return

    async def validate(self) -> None:
        """Validate store connectivity (alias for prepare)."""
        await self.prepare()

    @abstractmethod
    async def put(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> int:
        """Upload data under ``key``. Returns bytes stored."""

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """Download the full object under ``key``. Raises KeyError if missing."""

    @abstractmethod
    def stream(self, key: str, *, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        """Stream an object in chunks. Raises KeyError if missing."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if ``key`` exists."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete ``key``. Returns True if an object was deleted."""

    @abstractmethod
    async def copy(self, source: str, destination: str) -> None:
        """Server-side copy from ``source`` to ``destination``."""

    @abstractmethod
    async def head(self, key: str) -> dict[str, Any]:
        """Return object metadata. Raises KeyError if missing."""
