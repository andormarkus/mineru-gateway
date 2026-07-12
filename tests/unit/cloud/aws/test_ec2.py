"""AWS EC2 provider unit tests via moto ThreadedMotoServer + aioboto3."""

from __future__ import annotations

import pytest

from mineru_gateway.cloud.aws.ec2 import AwsEc2Provider, _normalize_ec2_state
from mineru_gateway.cloud.types import InstanceState


@pytest.fixture
def ec2_provider(moto_ec2_endpoint: str) -> AwsEc2Provider:
    return AwsEc2Provider(region="us-east-1", endpoint_url=moto_ec2_endpoint)


def _boto3_sync(moto_ec2_endpoint: str):
    import boto3

    return boto3.client("ec2", endpoint_url=moto_ec2_endpoint, region_name="us-east-1")


@pytest.mark.asyncio
async def test_tier_a_suspend_resume(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.suspend_instance(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.SUSPENDED

    await ec2_provider.resume_instance(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.RUNNING


@pytest.mark.asyncio
async def test_tier_b_launch_from_template(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    ec2.create_launch_template(
        LaunchTemplateName="lt-test", LaunchTemplateData={"ImageId": "ami-test", "InstanceType": "t2.micro"}
    )
    lt_id = ec2.describe_launch_templates(LaunchTemplateNames=["lt-test"])["LaunchTemplates"][0]["LaunchTemplateId"]

    instance_id = await ec2_provider.launch_instance(lt_id, version="1")
    assert instance_id.startswith("i-")
    assert await ec2_provider.get_state(instance_id) == InstanceState.RUNNING


@pytest.mark.asyncio
async def test_tier_b_terminate(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    await ec2_provider.terminate_instance(instance_id)
    assert await ec2_provider.get_state(instance_id) == InstanceState.TERMINATED


@pytest.mark.asyncio
async def test_get_private_ip(ec2_provider: AwsEc2Provider, moto_ec2_endpoint: str) -> None:
    ec2 = _boto3_sync(moto_ec2_endpoint)
    resp = ec2.run_instances(ImageId="ami-test", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    instance_id = resp["Instances"][0]["InstanceId"]

    ip = await ec2_provider.get_private_ip(instance_id)
    assert ip


@pytest.mark.asyncio
async def test_get_state_unknown(ec2_provider: AwsEc2Provider) -> None:
    assert await ec2_provider.get_state("i-nonexistent") == InstanceState.UNKNOWN


def test_normalize_ec2_state_mapping() -> None:
    assert _normalize_ec2_state("running") == InstanceState.RUNNING
    assert _normalize_ec2_state("stopped") == InstanceState.SUSPENDED
    assert _normalize_ec2_state("pending") == InstanceState.STARTING
    assert _normalize_ec2_state("terminated") == InstanceState.TERMINATED
    assert _normalize_ec2_state("weird-state") == InstanceState.UNKNOWN
