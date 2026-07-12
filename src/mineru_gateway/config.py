"""Gateway configuration via pydantic-settings (YAML + env, startup-only).

Precedence (low → high): defaults → ``config.yaml`` → env (``MINERU_GATEWAY_*`` with ``__`` nesting) → CLI flags

Config is read **once** at boot; changes require a restart (cheap — state lives in the DB). Secrets live in config/env
only; ``config.yaml`` is gitignored.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_CONFIG_PATH = "config.yaml"

_cached_settings: GatewaySettings | None = None


# --- Nested config models ----------------------------------------------------


class AwsCloudConfig(BaseModel):
    """AWS EC2 + S3 settings — launch-template-backed (CLOUD_WORKERS.md)."""

    region: str = "us-east-1"
    launch_template_id: str | None = None
    launch_template_version: str = "$Default"
    subnet_id: str | None = None
    security_group_id: str | None = None
    instance_type: str = "g5.xlarge"
    bucket: str = "mineru-results"
    endpoint_url: str | None = None  # S3/SeaweedFS dev/test; None → AWS S3
    ec2_endpoint_url: str | None = None  # EC2 moto/dev; None → AWS EC2


class AzureCloudConfig(BaseModel):
    """Azure VM + Blob settings — parallel shape to AWS for provider-agnostic scheduler code."""

    region: str = "eastus"
    launch_template_id: str | None = None
    launch_template_version: str | None = None
    subnet_id: str | None = None
    security_group_id: str | None = None
    container: str = "mineru-results"
    account_url: str | None = None


class GcpCloudConfig(BaseModel):
    """GCP Compute + GCS settings — parallel shape to AWS for provider-agnostic scheduler code."""

    region: str = "us-central1"
    launch_template_id: str | None = None
    launch_template_version: str | None = None
    subnet_id: str | None = None
    security_group_id: str | None = None
    bucket: str = "mineru-results"


class ScalingConfig(BaseModel):
    """Target-tracking autoscaling on queue depth."""

    target_per_worker: int = 4
    min_workers: int = 0
    max_workers: int = 12
    idle_cooldown_seconds: int = 300
    scale_up_cooldown_seconds: int = 60
    poll_interval_seconds: int = 10


class CacheConfig(BaseModel):
    """Content-addressed dedup cache."""

    enabled: bool = True
    ttl_seconds: int = 604800  # 7 days
    sweeper_interval_seconds: int = 600


class RotationConfig(BaseModel):
    """Tier B rotation scheduling (CLOUD_WORKERS.md). Leader-only background loop."""

    interval_seconds: int = 604800  # weekly
    drain_timeout_seconds: int = 600


class SchedulerConfig(BaseModel):
    """Scheduler process config (active-passive, central-queue dispatch)."""

    heartbeat_interval_seconds: float = 5.0
    lease_timeout_seconds: float = 30.0
    dispatch_interval_seconds: float = 5.0
    drain_interval_seconds: float = 30.0
    health_monitor_interval_seconds: float = 10.0


class CloudConfig(BaseModel):
    """Active cloud provider and per-vendor settings."""

    provider: str = "aws"
    aws: AwsCloudConfig = Field(default_factory=AwsCloudConfig)
    azure: AzureCloudConfig = Field(default_factory=AzureCloudConfig)
    gcp: GcpCloudConfig = Field(default_factory=GcpCloudConfig)

    def launch_template(self) -> tuple[str | None, str | None, str]:
        """Return ``(template_id, template_version, region)`` for the active provider."""
        if self.provider == "aws":
            return self.aws.launch_template_id, self.aws.launch_template_version, self.aws.region
        if self.provider == "azure":
            return self.azure.launch_template_id, self.azure.launch_template_version, self.azure.region
        if self.provider == "gcp":
            return self.gcp.launch_template_id, self.gcp.launch_template_version, self.gcp.region
        raise ValueError(f"Unknown cloud provider: {self.provider!r}")

    def object_store_bucket(self) -> str | None:
        """Return bucket/container name for the active provider."""
        if self.provider == "aws":
            return self.aws.bucket or None
        if self.provider == "azure":
            return self.azure.container or None
        if self.provider == "gcp":
            return self.gcp.bucket or None
        return None

    def is_object_store_configured(self) -> bool:
        """True when the active provider has a non-empty object store target."""
        return bool(self.object_store_bucket())


class AttributionConfig(BaseModel):
    """MinerU §2 attribution — on by default; this is a license obligation."""

    enabled: bool = True
    powered_by_header: str = "MinerU"
    version_header: str = "MinerU-Version"


class AuthConfig(BaseModel):
    """API-key auth. Disabled by default — until enabled, the gateway must be network-isolated."""

    enabled: bool = False
    api_keys: list[str] = Field(default_factory=list)


class OtelConfig(BaseModel):
    """OpenTelemetry observability — OTLP exporter, off by default."""

    enabled: bool = False
    endpoint: str | None = None  # OTLP HTTP base, e.g. http://localhost:4318
    service_name: str = "mineru-gateway"
    scheduler_service_name: str = "mineru-scheduler"
    metrics_export_interval_seconds: int = 60


# --- Top-level settings ------------------------------------------------------


class GatewaySettings(BaseSettings):
    """Top-level gateway settings.

    Env prefix ``MINERU_GATEWAY_`` with ``__`` nesting, e.g. ``MINERU_GATEWAY_SCALING__MAX_WORKERS=12``.
    """

    model_config = SettingsConfigDict(env_prefix="MINERU_GATEWAY_", env_nested_delimiter="__", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./gateway.db"
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"  # DEBUG | INFO | WARNING | ERROR

    # Backpressure / admission control.
    max_file_size_bytes: int = 0  # 0 = no limit
    task_sla_seconds: int = 3600  # tasks pending longer than this are marked failed

    scaling: ScalingConfig = Field(default_factory=ScalingConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    rotation: RotationConfig = Field(default_factory=RotationConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    attribution: AttributionConfig = Field(default_factory=AttributionConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    otel: OtelConfig = Field(default_factory=OtelConfig)


# --- YAML config source ------------------------------------------------------


class YamlConfigSettingsSource:
    """Load ``config.yaml`` (if present) and deep-merge into settings.

    Implemented as a callable that returns a dict consumed by ``GatewaySettings``. Kept simple and explicit.
    """

    def __init__(self, path: str | Path = DEFAULT_CONFIG_PATH) -> None:
        """Store the YAML config path to load from."""
        self.path = Path(path)

    def __call__(self) -> dict[str, Any]:
        """Return the YAML mapping at ``self.path``, or {} if the file is absent."""
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{self.path}: top-level YAML must be a mapping, got {type(data).__name__}")
        return data


def load_settings(config_path: str | Path | None = None) -> GatewaySettings:
    """Build ``GatewaySettings`` merging defaults ← YAML ← env.

    Called once at startup. The result is cached and returned by :func:`get_settings`.
    """
    global _cached_settings
    path = config_path or DEFAULT_CONFIG_PATH
    yaml_data = YamlConfigSettingsSource(path)()
    _cached_settings = GatewaySettings(**yaml_data)
    return _cached_settings


def get_settings() -> GatewaySettings:
    """Return the singleton settings (loaded via :func:`load_settings` at startup)."""
    global _cached_settings
    if _cached_settings is None:
        return load_settings()
    return _cached_settings


def reset_settings_cache() -> None:
    """Clear the cached settings (used by tests that override config)."""
    global _cached_settings
    _cached_settings = None
