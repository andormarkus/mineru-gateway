"""Azure VM worker provider (not yet implemented).

SDK mapping:
  resume_instance   → ComputeManagementClient.virtual_machines.begin_start
  suspend_instance  → begin_deallocate or begin_power_off(deallocate=True)
  launch_instance   → create from gallery image / VMSS scale-out
  terminate_instance → begin_delete
  get_state         → instance_view.statuses (PowerState/provisioningState)
"""

from __future__ import annotations

from mineru_gateway.cloud.base import CloudWorkerProvider
from mineru_gateway.cloud.types import InstanceState


class AzureVmProvider(CloudWorkerProvider):
    """Azure VM lifecycle — stub until ComputeManagementClient is wired."""

    def __init__(self, *, region: str = "eastus") -> None:
        self._region = region

    @property
    def name(self) -> str:
        return "azure"

    async def resume_instance(self, instance_id: str) -> None:
        raise NotImplementedError("Azure VM provider not yet implemented")

    async def suspend_instance(self, instance_id: str) -> None:
        raise NotImplementedError("Azure VM provider not yet implemented")

    async def launch_instance(self, template_id: str, version: str | None = None) -> str:
        raise NotImplementedError("Azure VM provider not yet implemented")

    async def terminate_instance(self, instance_id: str) -> None:
        raise NotImplementedError("Azure VM provider not yet implemented")

    async def get_state(self, instance_id: str) -> InstanceState:
        raise NotImplementedError("Azure VM provider not yet implemented")

    async def get_private_ip(self, instance_id: str) -> str:
        raise NotImplementedError("Azure VM provider not yet implemented")
