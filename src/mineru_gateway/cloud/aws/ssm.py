"""SSM Parameter Store references for cloud config values.

Launch template ids are created by the worker CloudFormation stack and
published to Parameter Store, so onboarding never copy-pastes ``lt-`` ids into
gateway config. A config value of ``ssm:/mineru-gateway/sandbox/launch-template-id``
is resolved to the stored value once, at provider init.
"""

from __future__ import annotations

import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from mineru_gateway.cloud.errors import CloudError, CloudErrorCategory
from mineru_gateway.config import CloudConfig

logger = logging.getLogger(__name__)

SSM_PREFIX = "ssm:"


def is_ssm_reference(value: str | None) -> bool:
    """True when a config value is an ``ssm:/path`` reference."""
    return value is not None and value.startswith(SSM_PREFIX)


def resolve_ssm_reference(value: str, *, region: str) -> str:
    """Resolve ``ssm:/path`` to the parameter's value (sync; startup only)."""
    name = value[len(SSM_PREFIX) :]
    if not name.startswith("/"):
        raise ValueError(f"SSM reference must be an absolute parameter path: {value!r}")
    client = boto3.client("ssm", region_name=region)
    try:
        response = client.get_parameter(Name=name)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("ParameterNotFound", "ParameterVersionNotFound"):
            raise CloudError(
                category=CloudErrorCategory.NOT_FOUND,
                code=error_code or "SSMParameterNotFound",
                message=f"SSM parameter '{name}' not found — deploy the worker stack that publishes it",
            ) from exc
        raise CloudError(
            category=CloudErrorCategory.AUTH
            if error_code in ("AccessDeniedException", "UnauthorizedOperation")
            else CloudErrorCategory.RETRYABLE,
            code=error_code or "SSMError",
            message=f"Cannot read SSM parameter '{name}': {exc}",
        ) from exc
    except BotoCoreError as exc:
        raise CloudError(
            category=CloudErrorCategory.RETRYABLE,
            code="SSMUnreachable",
            message=f"Cannot reach SSM for parameter '{name}': {exc}",
        ) from exc
    resolved = response["Parameter"]["Value"].strip()
    if not resolved:
        raise CloudError(
            category=CloudErrorCategory.NOT_FOUND, code="SSMParameterEmpty", message=f"SSM parameter '{name}' is empty"
        )
    logger.info("Resolved SSM parameter %s", name)
    return resolved


def resolve_cloud_config(cloud: CloudConfig) -> CloudConfig:
    """Return a copy of ``cloud`` with any ``ssm:`` references resolved."""
    template_id = cloud.aws.launch_template_id
    if not is_ssm_reference(template_id):
        return cloud
    assert template_id is not None  # narrowed by is_ssm_reference
    resolved = resolve_ssm_reference(template_id, region=cloud.aws.region)
    logger.info("Launch template resolved via SSM: %s", resolved)
    return cloud.model_copy(update={"aws": cloud.aws.model_copy(update={"launch_template_id": resolved})})
