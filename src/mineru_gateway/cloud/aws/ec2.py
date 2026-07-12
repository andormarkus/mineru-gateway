"""AWS EC2 cloud worker provider — launch-template-backed, two-tier (CLOUD_WORKERS.md).

Tier A: ``start_instances`` / ``stop_instances`` (maps to resume/suspend).
Tier B: ``run_instances`` from launch template / ``terminate_instances``.

All async via aioboto3. Credentials resolve via the AWS default credential chain.
"""

from __future__ import annotations

import logging
from typing import Any

import aioboto3
from botocore.exceptions import ClientError

from mineru_gateway.cloud.base import CloudWorkerProvider
from mineru_gateway.cloud.types import InstanceState

logger = logging.getLogger(__name__)

_EC2_STATE_MAP: dict[str, InstanceState] = {
    "running": InstanceState.RUNNING,
    "stopped": InstanceState.SUSPENDED,
    "pending": InstanceState.STARTING,
    "stopping": InstanceState.STOPPING,
    "shutting-down": InstanceState.STOPPING,
    "terminated": InstanceState.TERMINATED,
}


def _normalize_ec2_state(vendor_state: str) -> InstanceState:
    """Map EC2 ``State.Name`` values to :class:`InstanceState`."""
    return _EC2_STATE_MAP.get(vendor_state, InstanceState.UNKNOWN)


class AwsEc2Provider(CloudWorkerProvider):
    """EC2-backed worker lifecycle via aioboto3 — two tiers, launch-template-backed."""

    def __init__(self, *, region: str = "us-east-1", endpoint_url: str | None = None) -> None:
        self._region = region
        self._endpoint_url = endpoint_url
        self._session = aioboto3.Session(region_name=region)

    @property
    def name(self) -> str:
        return "aws"

    def _client(self) -> Any:
        return self._session.client("ec2", region_name=self._region, endpoint_url=self._endpoint_url)

    # --- Tier A: power management ---

    async def resume_instance(self, instance_id: str) -> None:
        logger.info("EC2 resume: instance_id=%s region=%s", instance_id, self._region)
        async with self._client() as ec2:
            await ec2.start_instances(InstanceIds=[instance_id])

    async def suspend_instance(self, instance_id: str) -> None:
        logger.info("EC2 suspend: instance_id=%s region=%s", instance_id, self._region)
        async with self._client() as ec2:
            await ec2.stop_instances(InstanceIds=[instance_id], Force=False)

    # --- Tier B: lifecycle ---

    async def launch_instance(self, template_id: str, version: str | None = None) -> str:
        launch_template: dict[str, Any] = {"LaunchTemplateId": template_id}
        if version is not None:
            launch_template["Version"] = str(version)

        async with self._client() as ec2:
            resp = await ec2.run_instances(MaxCount=1, MinCount=1, LaunchTemplate=launch_template)
            instance_id = resp["Instances"][0]["InstanceId"]
        logger.info(
            "EC2 launch: instance_id=%s template=%s version=%s region=%s",
            instance_id,
            template_id,
            version,
            self._region,
        )
        return instance_id

    async def terminate_instance(self, instance_id: str) -> None:
        logger.info("EC2 terminate: instance_id=%s region=%s", instance_id, self._region)
        async with self._client() as ec2:
            await ec2.terminate_instances(InstanceIds=[instance_id])

    # --- shared ---

    async def get_state(self, instance_id: str) -> InstanceState:
        try:
            async with self._client() as ec2:
                resp = await ec2.describe_instances(InstanceIds=[instance_id])
        except ClientError as exc:
            if "InvalidInstanceID" in str(exc) or "NotFound" in str(exc):
                logger.debug("EC2 get_state: instance %s not found", instance_id)
                return InstanceState.UNKNOWN
            logger.warning("EC2 get_state failed: instance_id=%s", instance_id, exc_info=True)
            raise
        reservations = resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            return InstanceState.UNKNOWN
        vendor_state = reservations[0]["Instances"][0]["State"]["Name"]
        return _normalize_ec2_state(vendor_state)

    async def get_private_ip(self, instance_id: str) -> str:
        async with self._client() as ec2:
            resp = await ec2.describe_instances(InstanceIds=[instance_id])
        reservations = resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            raise ValueError(f"No instance found: {instance_id}")
        instance = reservations[0]["Instances"][0]
        ip = instance.get("PrivateIpAddress")
        if not ip:
            raise ValueError(f"Instance {instance_id} has no private IP yet")
        return ip
