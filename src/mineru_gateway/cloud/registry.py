"""Cloud provider registry — compute + storage factories keyed by ``cloud.provider``.

Add Azure/GCP by implementing providers under ``cloud/azure/`` or ``cloud/gcp/`` and
registering factories below. Scheduler/autoscaler and gateway code stay provider-agnostic
via the ABCs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from mineru_gateway.cloud.aws.ec2 import AwsEc2Provider
from mineru_gateway.cloud.aws.s3 import S3ObjectStore
from mineru_gateway.cloud.base import CloudStorageProvider, CloudWorkerProvider
from mineru_gateway.config import CloudConfig, get_settings

logger = logging.getLogger(__name__)

ProviderFactory = Callable[[CloudConfig], CloudWorkerProvider]
StoreFactory = Callable[[CloudConfig], CloudStorageProvider]

_COMPUTE_FACTORIES: dict[str, ProviderFactory] = {
    "aws": lambda cloud: AwsEc2Provider(region=cloud.aws.region, endpoint_url=cloud.aws.ec2_endpoint_url),
    # "azure": lambda cloud: AzureVmProvider(region=cloud.azure.region),
    # "gcp": lambda cloud: GcpComputeProvider(region=cloud.gcp.region),
}

_STORE_FACTORIES: dict[str, StoreFactory] = {
    "aws": lambda cloud: S3ObjectStore(
        bucket=cloud.aws.bucket,
        endpoint_url=cloud.aws.endpoint_url,
        region=cloud.aws.region,
    ),
    # "azure": lambda cloud: AzureBlobStore(container=cloud.azure.container, account_url=cloud.azure.account_url),
    # "gcp": lambda cloud: GcsObjectStore(bucket=cloud.gcp.bucket),
}


def available_providers() -> list[str]:
    """Return compute provider names with a registered factory."""
    return sorted(_COMPUTE_FACTORIES)


def available_store_providers() -> list[str]:
    """Return storage provider names with a registered factory."""
    return sorted(_STORE_FACTORIES)


def get_provider(name: str | None = None) -> CloudWorkerProvider:
    """Return a configured compute provider for ``name`` (defaults to config)."""
    cloud = get_settings().cloud
    resolved = name or cloud.provider
    factory = _COMPUTE_FACTORIES.get(resolved)
    if factory is None:
        raise ValueError(
            f"Cloud provider '{resolved}' is not implemented. Available: {available_providers()}"
        )
    return factory(cloud)


def get_store(name: str | None = None) -> CloudStorageProvider:
    """Return a configured object store for ``name`` (defaults to config)."""
    cloud = get_settings().cloud
    resolved = name or cloud.provider
    factory = _STORE_FACTORIES.get(resolved)
    if factory is None:
        raise ValueError(
            f"Object store for provider '{resolved}' is not implemented. Available: {available_store_providers()}"
        )
    return factory(cloud)


def init_provider() -> CloudWorkerProvider | None:
    """Build the configured compute provider, returning it (or None if it can't be built)."""
    try:
        provider = get_provider(get_settings().cloud.provider)
        logger.info("Cloud compute provider initialized: %s", provider.name)
        return provider
    except Exception:
        logger.warning("Cloud provider unavailable — running without cloud scaling", exc_info=True)
        return None


async def init_store() -> CloudStorageProvider | None:
    """Build + prepare the object store from settings, returning it (or None if unconfigured)."""
    cloud = get_settings().cloud
    if not cloud.is_object_store_configured():
        return None
    try:
        store = get_store(cloud.provider)
        await store.prepare()
        logger.info(
            "Object store initialized: provider=%s bucket=%s",
            cloud.provider,
            cloud.object_store_bucket(),
        )
        return store
    except Exception:
        logger.warning("Object store unavailable — running without durable result/payload storage", exc_info=True)
        return None
