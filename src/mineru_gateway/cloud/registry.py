"""Cloud provider registry — AWS compute + storage factories keyed by ``cloud.provider``."""

from __future__ import annotations

import logging
from collections.abc import Callable

from mineru_gateway.cloud.aws.ec2 import AwsEc2Provider
from mineru_gateway.cloud.aws.s3 import S3ObjectStore
from mineru_gateway.cloud.base import CloudStorageProvider, ComputeProvider
from mineru_gateway.config import GatewaySettings
from mineru_gateway.startup_guard import StartupDependencyError

logger = logging.getLogger(__name__)

ProviderFactory = Callable[[GatewaySettings], ComputeProvider]
StoreFactory = Callable[[GatewaySettings], CloudStorageProvider]

_COMPUTE_FACTORIES: dict[str, ProviderFactory] = {
    "aws": lambda settings: AwsEc2Provider(
        region=settings.cloud.aws.region,
        endpoint_url=settings.cloud.aws.ec2_endpoint_url,
        cloud=settings.cloud,
        deployment_id=settings.deployment_id,
    )
}

_STORE_FACTORIES: dict[str, StoreFactory] = {
    "aws": lambda settings: S3ObjectStore(
        bucket=settings.cloud.aws.bucket, endpoint_url=settings.cloud.aws.endpoint_url, region=settings.cloud.aws.region
    )
}


def available_providers() -> list[str]:
    """Return compute provider names with a registered factory."""
    return sorted(_COMPUTE_FACTORIES)


def available_store_providers() -> list[str]:
    """Return storage provider names with a registered factory."""
    return sorted(_STORE_FACTORIES)


def get_provider(name: str | None, *, settings: GatewaySettings) -> ComputeProvider:
    """Return a configured compute provider for ``name`` (defaults to config)."""
    resolved = name or settings.cloud.provider
    factory = _COMPUTE_FACTORIES.get(resolved)
    if factory is None:
        raise ValueError(f"Cloud provider '{resolved}' is not implemented. Available: {available_providers()}")
    return factory(settings)


def get_store(name: str | None, *, settings: GatewaySettings) -> CloudStorageProvider:
    """Return a configured object store for ``name`` (defaults to config)."""
    resolved = name or settings.cloud.provider
    factory = _STORE_FACTORIES.get(resolved)
    if factory is None:
        raise ValueError(
            f"Object store for provider '{resolved}' is not implemented. Available: {available_store_providers()}"
        )
    return factory(settings)


def init_provider(settings: GatewaySettings) -> ComputeProvider | None:
    """Build the configured compute provider."""
    if not settings.cloud_workers_enabled:
        return None
    provider = get_provider(settings.cloud.provider, settings=settings)
    logger.info("Cloud compute provider initialized: %s", provider.name)
    return provider


async def init_store(settings: GatewaySettings, *, required: bool = True) -> CloudStorageProvider | None:
    """Build and validate the object store from settings."""
    cloud = settings.cloud
    if not cloud.is_object_store_configured():
        if required:
            raise StartupDependencyError(f"cloud.{cloud.provider} bucket/container name is required")
        return None
    store = get_store(cloud.provider, settings=settings)
    await store.prepare()
    logger.info("Object store initialized: provider=%s bucket=%s", cloud.provider, cloud.object_store_bucket())
    return store
