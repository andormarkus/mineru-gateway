"""AWS EC2 provider unit tests via moto ThreadedMotoServer + aioboto3."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from mineru_gateway.cloud.aws.ec2 import AwsEc2Provider
from mineru_gateway.cloud.errors import CloudError, CloudErrorCategory
from mineru_gateway.cloud.types import (
    TAG_DEPLOYMENT,
    TAG_GENERATION,
    TAG_MANAGED,
    TAG_ROLE,
    TAG_ROLE_WORKER,
    TAG_WORKER_ID,
    InstanceState,
)


@pytest.fixture
def ec2_provider(moto_ec2_endpoint: str) -> AwsEc2Provider:
    cloud = MagicMock()
    cloud.launch_template.return_value = (None, "1", "us-east-1")
    return AwsEc2Provider(region="us-east-1", endpoint_url=moto_ec2_endpoint, cloud=cloud, deployment_id="dep-1")


def _boto3_sync(moto_ec2_endpoint: str):
    import boto3

    return boto3.client("ec2", endpoint_url=moto_ec2_endpoint, region_name="us-east-1")


def _tag_specs(tags: dict[str, str]) -> list[dict]:
    return [{"ResourceType": "instance", "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}]


@pytest.mark.asyncio
async def test_stop_and_start(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.stop(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.SUSPENDED

    await ec2_provider.start(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.RUNNING


@pytest.mark.asyncio
async def test_idempotent_stop_and_start(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.stop(instance_id)
    await ec2_provider.stop(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.SUSPENDED

    await ec2_provider.start(instance_id)
    await ec2_provider.start(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.RUNNING


@pytest.mark.asyncio
async def test_launch_from_template(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    ec2.create_launch_template(
        LaunchTemplateName="lt-test", LaunchTemplateData={"ImageId": "ami-test", "InstanceType": "t2.micro"}
    )
    lt_id = ec2.describe_launch_templates(LaunchTemplateNames=["lt-test"])["LaunchTemplates"][0]["LaunchTemplateId"]

    cloud = cast(MagicMock, ec2_provider._cloud)
    cloud.launch_template.return_value = (lt_id, "1", "us-east-1")
    instance_id = await ec2_provider.launch("worker-1", deployment_id="dep-1", generation=0)

    assert instance_id.startswith("i-")
    assert await ec2_provider.get_state(instance_id) == InstanceState.RUNNING


@pytest.mark.asyncio
async def test_launch_tags_instance(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    ec2.create_launch_template(
        LaunchTemplateName="lt-tags", LaunchTemplateData={"ImageId": "ami-test", "InstanceType": "t2.micro"}
    )
    lt_id = ec2.describe_launch_templates(LaunchTemplateNames=["lt-tags"])["LaunchTemplates"][0]["LaunchTemplateId"]

    cloud = cast(MagicMock, ec2_provider._cloud)
    cloud.launch_template.return_value = (lt_id, "1", "us-east-1")
    instance_id = await ec2_provider.launch("worker-1", deployment_id="dep-1", generation=2)

    described = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
    tag_map = {tag["Key"]: tag["Value"] for tag in described["Tags"]}
    assert tag_map[TAG_DEPLOYMENT] == "dep-1"
    assert tag_map[TAG_WORKER_ID] == "worker-1"
    assert tag_map[TAG_GENERATION] == "2"
    assert tag_map[TAG_MANAGED] == "true"
    assert tag_map[TAG_ROLE] == TAG_ROLE_WORKER


@pytest.mark.asyncio
async def test_discover_instances_by_deployment(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)

    ec2.run_instances(
        ImageId="ami-test",
        InstanceType="t2.micro",
        MinCount=1,
        MaxCount=1,
        TagSpecifications=_tag_specs(
            {
                TAG_MANAGED: "true",
                TAG_DEPLOYMENT: "dep-a",
                TAG_WORKER_ID: "worker-a",
                TAG_ROLE: TAG_ROLE_WORKER,
                TAG_GENERATION: "0",
            }
        ),
    )
    ec2.run_instances(
        ImageId="ami-test",
        InstanceType="t2.micro",
        MinCount=1,
        MaxCount=1,
        TagSpecifications=_tag_specs(
            {TAG_MANAGED: "true", TAG_DEPLOYMENT: "dep-b", TAG_WORKER_ID: "worker-b", TAG_ROLE: TAG_ROLE_WORKER}
        ),
    )

    discovered = await ec2_provider.discover("dep-a")
    assert len(discovered) == 1
    assert discovered[0].worker_id == "worker-a"
    assert discovered[0].state == InstanceState.RUNNING
    assert discovered[0].tags[TAG_DEPLOYMENT] == "dep-a"


@pytest.mark.asyncio
async def test_terminate(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.terminate(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.TERMINATED


@pytest.mark.asyncio
async def test_idempotent_terminate(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.terminate(instance_id)
    await ec2_provider.terminate(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.TERMINATED


@pytest.mark.asyncio
async def test_get_private_ip(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    ip = await ec2_provider.get_private_ip(instance_id)
    assert ip


@pytest.mark.asyncio
async def test_get_private_ip_missing_instance_returns_none(ec2_provider: AwsEc2Provider) -> None:
    assert await ec2_provider.get_private_ip("i-nonexistent") is None


@pytest.mark.asyncio
async def test_get_state_unknown(ec2_provider: AwsEc2Provider) -> None:
    assert await ec2_provider.get_state("i-nonexistent") == InstanceState.TERMINATED


def test_normalize_ec2_state_mapping() -> None:
    assert AwsEc2Provider._normalize_ec2_state("running") == InstanceState.RUNNING
    assert AwsEc2Provider._normalize_ec2_state("stopped") == InstanceState.SUSPENDED
    assert AwsEc2Provider._normalize_ec2_state("pending") == InstanceState.STARTING
    assert AwsEc2Provider._normalize_ec2_state("terminated") == InstanceState.TERMINATED
    assert AwsEc2Provider._normalize_ec2_state("weird-state") == InstanceState.UNKNOWN


def test_classify_ec2_error_by_code() -> None:
    from botocore.exceptions import ClientError

    exc = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "RunInstances")
    err = AwsEc2Provider._classify_ec2_error(exc)
    assert isinstance(err, CloudError)
    assert err.category == CloudErrorCategory.AUTH
    assert err.code == "UnauthorizedOperation"

    exc = ClientError({"Error": {"Code": "Throttling", "Message": "slow down"}}, "StartInstances")
    err = AwsEc2Provider._classify_ec2_error(exc)
    assert err.category == CloudErrorCategory.RETRYABLE
    assert err.retryable is True
