"""AWS EC2 compute provider — launch-template-backed, idempotent."""

from __future__ import annotations

import logging
from typing import Any, NoReturn

import aioboto3
from botocore.exceptions import ClientError

from mineru_gateway.cloud.base import ComputeProvider
from mineru_gateway.cloud.errors import CloudError, CloudErrorCategory
from mineru_gateway.cloud.types import (
    TAG_DEPLOYMENT,
    TAG_MANAGED,
    TAG_ROLE,
    TAG_ROLE_WORKER,
    TAG_WORKER_ID,
    DiscoveredInstance,
    InstanceState,
    build_worker_tags,
)
from mineru_gateway.config import CloudConfig

logger = logging.getLogger(__name__)

_EC2_STATE_MAP: dict[str, InstanceState] = {
    "running": InstanceState.RUNNING,
    "stopped": InstanceState.SUSPENDED,
    "pending": InstanceState.STARTING,
    "stopping": InstanceState.STOPPING,
    "shutting-down": InstanceState.TERMINATING,
    "terminated": InstanceState.TERMINATED,
}

_EC2_ERROR_CLASSIFICATION: dict[str, tuple[CloudErrorCategory, bool]] = {
    "InvalidInstanceID.NotFound": (CloudErrorCategory.NOT_FOUND, False),
    "InvalidInstanceID.Malformed": (CloudErrorCategory.INVALID_ID, False),
    "InvalidInstanceID": (CloudErrorCategory.INVALID_ID, False),
    "UnauthorizedOperation": (CloudErrorCategory.AUTH, False),
    "AccessDenied": (CloudErrorCategory.AUTH, False),
    "RequestLimitExceeded": (CloudErrorCategory.RETRYABLE, True),
    "Throttling": (CloudErrorCategory.RETRYABLE, True),
    "ThrottlingException": (CloudErrorCategory.RETRYABLE, True),
    "ServiceUnavailable": (CloudErrorCategory.RETRYABLE, True),
    "InternalError": (CloudErrorCategory.RETRYABLE, True),
    "InsufficientInstanceCapacity": (CloudErrorCategory.QUOTA, False),
    "VcpuLimitExceeded": (CloudErrorCategory.QUOTA, False),
    "InstanceLimitExceeded": (CloudErrorCategory.QUOTA, False),
    "VolumeLimitExceeded": (CloudErrorCategory.QUOTA, False),
}


class AwsEc2Provider(ComputeProvider):
    """EC2-backed worker lifecycle via aioboto3."""

    def __init__(
        self,
        *,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        cloud: CloudConfig | None = None,
        deployment_id: str = "dev-local",
    ) -> None:
        self._region = region
        self._endpoint_url = endpoint_url
        self._cloud = cloud
        self._deployment_id = deployment_id
        self._session = aioboto3.Session(region_name=region)

    @property
    def name(self) -> str:
        return "aws"

    def _client(self) -> Any:
        return self._session.client("ec2", region_name=self._region, endpoint_url=self._endpoint_url)

    async def _describe_instance(self, instance_id: str) -> dict[str, Any] | None:
        async with self._client() as ec2:
            try:
                describe_resp = await ec2.describe_instances(InstanceIds=[instance_id])
            except ClientError as exc:
                code = self._ec2_error_code(exc)
                if code in {"InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed", "InvalidInstanceID"}:
                    return None
                self._raise_ec2_error(exc)
            reservations = describe_resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            return None
        return reservations[0]["Instances"][0]

    async def start(self, instance_id: str) -> None:
        state = await self.get_state(instance_id)
        if state in {InstanceState.RUNNING, InstanceState.STARTING}:
            return
        try:
            async with self._client() as ec2:
                await ec2.start_instances(InstanceIds=[instance_id])
        except ClientError as exc:
            if self._ec2_error_code(exc) == "IncorrectInstanceState":
                return
            self._raise_ec2_error(exc)

    async def stop(self, instance_id: str) -> None:
        state = await self.get_state(instance_id)
        if state in {InstanceState.SUSPENDED, InstanceState.STOPPING, InstanceState.TERMINATED}:
            return
        try:
            async with self._client() as ec2:
                await ec2.stop_instances(InstanceIds=[instance_id], Force=False)
        except ClientError as exc:
            if self._ec2_error_code(exc) == "IncorrectInstanceState":
                return
            self._raise_ec2_error(exc)

    async def launch(self, worker_id: str, *, deployment_id: str, generation: int) -> str:
        if self._cloud is None:
            raise CloudError(
                category=CloudErrorCategory.INVALID_ID,
                code="MissingCloudConfig",
                message="AwsEc2Provider requires cloud config at construction",
                retryable=False,
            )
        template_id, version, _ = self._cloud.launch_template()
        if not template_id:
            raise CloudError(
                category=CloudErrorCategory.INVALID_ID,
                code="MissingLaunchTemplate",
                message="cloud.aws.launch_template_id is required",
                retryable=False,
            )

        tags = build_worker_tags(worker_id=worker_id, deployment_id=deployment_id, generation=generation)
        launch_template: dict[str, Any] = {"LaunchTemplateId": template_id}
        if version is not None:
            launch_template["Version"] = str(version)

        params: dict[str, Any] = {
            "MaxCount": 1,
            "MinCount": 1,
            "LaunchTemplate": launch_template,
            "ClientToken": worker_id,
            "TagSpecifications": self._tags_to_specifications(tags),
        }

        async with self._client() as ec2:
            try:
                launch_resp = await ec2.run_instances(**params)
            except ClientError as exc:
                self._raise_ec2_error(exc)
            instance_id = launch_resp["Instances"][0]["InstanceId"]

        logger.info("EC2 launch: instance_id=%s worker_id=%s", instance_id, worker_id)
        return instance_id

    async def terminate(self, instance_id: str) -> None:
        state = await self.get_state(instance_id)
        if state == InstanceState.TERMINATED:
            return
        try:
            async with self._client() as ec2:
                await ec2.terminate_instances(InstanceIds=[instance_id])
        except ClientError as exc:
            code = self._ec2_error_code(exc)
            if code in {"InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed", "InvalidInstanceID"}:
                return
            if code == "IncorrectInstanceState":
                return
            self._raise_ec2_error(exc)

    async def get_state(self, instance_id: str) -> InstanceState:
        instance = await self._describe_instance(instance_id)
        if instance is None:
            return InstanceState.TERMINATED
        return self._normalize_ec2_state(instance["State"]["Name"])

    async def get_private_ip(self, instance_id: str) -> str | None:
        instance = await self._describe_instance(instance_id)
        if instance is None:
            return None
        return instance.get("PrivateIpAddress")

    async def get_public_ip(self, instance_id: str) -> str | None:
        instance = await self._describe_instance(instance_id)
        if instance is None:
            return None
        return instance.get("PublicIpAddress")

    async def discover(self, deployment_id: str) -> list[DiscoveredInstance]:
        filters = [
            {"Name": f"tag:{TAG_MANAGED}", "Values": ["true"]},
            {"Name": f"tag:{TAG_DEPLOYMENT}", "Values": [deployment_id]},
            {"Name": f"tag:{TAG_ROLE}", "Values": [TAG_ROLE_WORKER]},
        ]
        discovered: list[DiscoveredInstance] = []
        try:
            async with self._client() as ec2:
                paginator = ec2.get_paginator("describe_instances")
                async for page in paginator.paginate(Filters=filters):
                    for reservation in page.get("Reservations", []):
                        for instance in reservation.get("Instances", []):
                            tags = self._tags_from_instance(instance)
                            discovered.append(
                                DiscoveredInstance(
                                    instance_id=instance["InstanceId"],
                                    worker_id=tags.get(TAG_WORKER_ID),
                                    state=self._normalize_ec2_state(instance["State"]["Name"]),
                                    tags=tags,
                                )
                            )
        except ClientError as exc:
            self._raise_ec2_error(exc)
        return discovered

    @staticmethod
    def _ec2_error_code(exc: ClientError) -> str:
        return exc.response.get("Error", {}).get("Code", "")

    @staticmethod
    def _classify_ec2_error(exc: ClientError) -> CloudError:
        code = AwsEc2Provider._ec2_error_code(exc)
        message = exc.response.get("Error", {}).get("Message", str(exc))
        category, retryable = _EC2_ERROR_CLASSIFICATION.get(code, (CloudErrorCategory.UNKNOWN, False))
        return CloudError(category=category, code=code, message=message, retryable=retryable)

    @staticmethod
    def _raise_ec2_error(exc: ClientError) -> NoReturn:
        raise AwsEc2Provider._classify_ec2_error(exc) from exc

    @staticmethod
    def _normalize_ec2_state(vendor_state: str) -> InstanceState:
        return _EC2_STATE_MAP.get(vendor_state, InstanceState.UNKNOWN)

    @staticmethod
    def _tags_to_specifications(tags: dict[str, str]) -> list[dict[str, Any]]:
        if not tags:
            return []
        return [{"ResourceType": "instance", "Tags": [{"Key": key, "Value": value} for key, value in tags.items()]}]

    @staticmethod
    def _tags_from_instance(instance: dict[str, Any]) -> dict[str, str]:
        return {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}
