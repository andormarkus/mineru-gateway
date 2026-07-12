"""GCP Compute Engine worker provider (not yet implemented).

SDK mapping:
  resume_instance   → instances().start()
  suspend_instance  → instances().stop() or instances().suspend()
  launch_instance   → instances().insert() from instance template
  terminate_instance → instances().delete()
  get_state         → instance.status field
"""

from __future__ import annotations

from mineru_gateway.cloud.base import CloudWorkerProvider
from mineru_gateway.cloud.types import InstanceState


class GcpComputeProvider(CloudWorkerProvider):
    """GCP Compute lifecycle — stub until google-cloud-compute is wired."""

    def __init__(self, *, region: str = "us-central1") -> None:
        self._region = region

    @property
    def name(self) -> str:
        return "gcp"

    async def resume_instance(self, instance_id: str) -> None:
        raise NotImplementedError("GCP Compute provider not yet implemented")

    async def suspend_instance(self, instance_id: str) -> None:
        raise NotImplementedError("GCP Compute provider not yet implemented")

    async def launch_instance(self, template_id: str, version: str | None = None) -> str:
        raise NotImplementedError("GCP Compute provider not yet implemented")

    async def terminate_instance(self, instance_id: str) -> None:
        raise NotImplementedError("GCP Compute provider not yet implemented")

    async def get_state(self, instance_id: str) -> InstanceState:
        raise NotImplementedError("GCP Compute provider not yet implemented")

    async def get_private_ip(self, instance_id: str) -> str:
        raise NotImplementedError("GCP Compute provider not yet implemented")
