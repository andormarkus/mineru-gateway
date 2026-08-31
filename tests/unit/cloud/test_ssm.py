"""Tests for ssm:/ launch-template reference resolution."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from mineru_gateway.cloud.aws.ssm import is_ssm_reference, resolve_cloud_config, resolve_ssm_reference
from mineru_gateway.cloud.errors import CloudError
from mineru_gateway.config import CloudConfig


def _cloud(template_id: str | None) -> CloudConfig:
    return CloudConfig.model_validate({"provider": "aws", "aws": {"launch_template_id": template_id}})


def test_is_ssm_reference() -> None:
    assert is_ssm_reference("ssm:/mineru-gateway/sandbox/launch-template-id")
    assert not is_ssm_reference("lt-0abc123")
    assert not is_ssm_reference(None)
    assert not is_ssm_reference("")


def test_resolve_cloud_config_passthrough_without_reference() -> None:
    cloud = _cloud("lt-0abc123")
    assert resolve_cloud_config(cloud) is cloud


def test_resolve_cloud_config_resolves_launch_template() -> None:
    cloud = _cloud("ssm:/mineru-gateway/sandbox/launch-template-id")
    with patch("mineru_gateway.cloud.aws.ssm.boto3.client") as client_factory:
        client_factory.return_value.get_parameter.return_value = {"Parameter": {"Value": " lt-resolved "}}
        resolved = resolve_cloud_config(cloud)

    client_factory.assert_called_once_with("ssm", region_name="us-east-1")
    assert resolved.aws.launch_template_id == "lt-resolved"
    assert resolved.launch_template() == ("lt-resolved", "$Latest", "us-east-1")
    assert cloud.aws.launch_template_id == "ssm:/mineru-gateway/sandbox/launch-template-id"  # original untouched


def test_resolve_ssm_reference_requires_absolute_path() -> None:
    with pytest.raises(ValueError, match="absolute parameter path"):
        resolve_ssm_reference("ssm:mineru-gateway/lt", region="us-east-1")


def test_resolve_ssm_reference_parameter_not_found() -> None:
    error = ClientError({"Error": {"Code": "ParameterNotFound", "Message": "nope"}}, "GetParameter")
    with patch("mineru_gateway.cloud.aws.ssm.boto3.client") as client_factory:
        client_factory.return_value.get_parameter.side_effect = error
        with pytest.raises(CloudError, match="worker stack that publishes it"):
            resolve_ssm_reference("ssm:/mineru-gateway/sandbox/launch-template-id", region="eu-central-1")


def test_resolve_ssm_reference_empty_value() -> None:
    with patch("mineru_gateway.cloud.aws.ssm.boto3.client") as client_factory:
        client_factory.return_value.get_parameter.return_value = {"Parameter": {"Value": "  "}}
        with pytest.raises(CloudError, match="empty"):
            resolve_ssm_reference("ssm:/mineru-gateway/sandbox/launch-template-id", region="eu-central-1")
