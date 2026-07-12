"""Cloud provider ABCs — compute (VM lifecycle) and storage (object/blob).

Compute — two-tier, vendor-neutral model (CLOUD_WORKERS.md):
  Tier A: resume_instance / suspend_instance
  Tier B: launch_instance / terminate_instance

Storage — object/blob durability for payloads and results:
  AWS S3, Azure Blob, GCS — register in :mod:`mineru_gateway.cloud.registry`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from mineru_gateway.cloud.types import InstanceState


class CloudWorkerProvider(ABC):
    """Abstract cloud worker lifecycle manager — two tiers, cross-platform."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name ('aws', 'azure', 'gcp')."""

    # --- Tier A: power management (autoscaling uses only these) ---

    @abstractmethod
    async def resume_instance(self, instance_id: str) -> None:
        """Power on a suspended VM (~30-60s boot). Preserves resource id + persistent disk."""

    @abstractmethod
    async def suspend_instance(self, instance_id: str) -> None:
        """Pause compute without destroying the VM resource (NOT terminate)."""

    # --- Tier B: lifecycle (rotation/refresh uses these) ---

    @abstractmethod
    async def launch_instance(self, template_id: str, version: str | None = None) -> str:
        """Provision a new VM from a template/image. Returns the new instance_id."""

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> None:
        """Permanently destroy a VM and ephemeral resources."""

    # --- shared ---

    @abstractmethod
    async def get_state(self, instance_id: str) -> InstanceState:
        """Return normalized instance state."""

    @abstractmethod
    async def get_private_ip(self, instance_id: str) -> str:
        """Return the private IPv4 for building the worker base_url."""


class CloudStorageProvider(ABC):
    """Abstract cloud object/blob store — payloads and results."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name ('aws', 'azure', 'gcp')."""

    async def prepare(self) -> None:
        """Optional startup hook (e.g. ensure bucket/container exists)."""
        return

    @abstractmethod
    async def put(
        self,
        key: str,
        data: bytes | AsyncIterator[bytes],
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
    async def head(self, key: str) -> dict[str, Any]:
        """Return object metadata. Raises KeyError if missing."""
