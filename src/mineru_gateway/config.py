"""Gateway configuration via pydantic-settings (YAML + env, startup-only).

Precedence (low → high): defaults → ``config.yaml`` → env (``MINERU_GATEWAY_*`` with ``__`` nesting) → CLI flags

Config is read **once** at boot; changes require a restart (cheap — state lives in the DB). Secrets live in config/env
only; ``config.yaml`` is gitignored.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

DEFAULT_CONFIG_PATH = "config.yaml"

# Fixed upper bound for accepted uploads (1 GiB). Not configurable.
MAX_FILE_SIZE_HARD_BYTES = 1024 * 1024 * 1024

_cached_settings: GatewaySettings | None = None

CloudProviderName = Literal["aws"]


# --- Nested config models ----------------------------------------------------


class AwsCloudConfig(BaseModel):
    """AWS EC2 + S3 settings — launch-template-backed (CLOUD_WORKERS.md)."""

    model_config = ConfigDict(extra="forbid")

    region: str = "us-east-1"
    launch_template_id: str | None = None
    launch_template_version: str = "$Latest"
    worker_address: Literal["private", "public"] = "private"
    bucket: str = "mineru-results"
    endpoint_url: str | None = None  # S3/SeaweedFS dev/test; None → AWS S3
    ec2_endpoint_url: str | None = None  # EC2 moto/dev; None → AWS EC2


class ScalingConfig(BaseModel):
    """Desired-capacity autoscaling on queue depth."""

    model_config = ConfigDict(extra="forbid")

    target_per_worker: int = 4
    min_workers: int = 0
    max_workers: int = 12
    idle_cooldown_seconds: int = 300
    scale_up_cooldown_seconds: int = 60

    @model_validator(mode="after")
    def _validate_bounds(self) -> ScalingConfig:
        if self.target_per_worker <= 0:
            raise ValueError("scaling.target_per_worker must be > 0")
        if self.min_workers < 0:
            raise ValueError("scaling.min_workers must be >= 0")
        if self.max_workers < 0:
            raise ValueError("scaling.max_workers must be >= 0")
        if self.min_workers > self.max_workers:
            raise ValueError("scaling.min_workers must be <= max_workers")
        return self


class ReconciliationConfig(BaseModel):
    """Worker reconciliation and launch readiness."""

    model_config = ConfigDict(extra="forbid")

    launch_readiness_timeout_seconds: int = 1800
    max_failure_count: int = 8
    stalled_worker_grace_seconds: int = 900

    @model_validator(mode="after")
    def _validate_positive(self) -> ReconciliationConfig:
        if self.launch_readiness_timeout_seconds <= 0:
            raise ValueError("reconciliation.launch_readiness_timeout_seconds must be > 0")
        if self.max_failure_count <= 0:
            raise ValueError("reconciliation.max_failure_count must be > 0")
        if self.stalled_worker_grace_seconds <= 0:
            raise ValueError("reconciliation.stalled_worker_grace_seconds must be > 0")
        return self


class RetentionConfig(BaseModel):
    """Object and DB retention cleanup."""

    model_config = ConfigDict(extra="forbid")

    retention_days: int = 30
    cleanup_interval_seconds: float = 3600.0

    @model_validator(mode="after")
    def _validate_positive(self) -> RetentionConfig:
        if self.retention_days <= 0:
            raise ValueError("retention.retention_days must be > 0")
        if self.cleanup_interval_seconds <= 0:
            raise ValueError("retention.cleanup_interval_seconds must be > 0")
        return self


class CacheConfig(BaseModel):
    """Content-addressed dedup cache."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    ttl_seconds: int = 604800  # 7 days
    sweeper_interval_seconds: int = 600


class RotationConfig(BaseModel):
    """Tier B rotation scheduling."""

    model_config = ConfigDict(extra="forbid")

    interval_seconds: int = 604800  # weekly
    readiness_timeout_seconds: int = 1800  # 30 minutes

    @model_validator(mode="after")
    def _validate_positive(self) -> RotationConfig:
        if self.interval_seconds <= 0:
            raise ValueError("rotation.interval_seconds must be > 0")
        if self.readiness_timeout_seconds <= 0:
            raise ValueError("rotation.readiness_timeout_seconds must be > 0")
        return self


class SchedulerConfig(BaseModel):
    """Scheduler process config — fast dispatch loop + slow reconcile loop."""

    model_config = ConfigDict(extra="forbid")

    dispatch_poll_interval_seconds: float = 0.5
    reconcile_poll_interval_seconds: float = 15.0

    @model_validator(mode="after")
    def _validate_positive(self) -> SchedulerConfig:
        if self.dispatch_poll_interval_seconds <= 0:
            raise ValueError("scheduler.dispatch_poll_interval_seconds must be > 0")
        if self.reconcile_poll_interval_seconds <= 0:
            raise ValueError("scheduler.reconcile_poll_interval_seconds must be > 0")
        return self


class CloudConfig(BaseModel):
    """Active cloud provider and per-vendor settings."""

    model_config = ConfigDict(extra="forbid")

    provider: CloudProviderName = "aws"
    aws: AwsCloudConfig = Field(default_factory=AwsCloudConfig)

    @field_validator("provider", mode="before")
    @classmethod
    def _normalize_provider(cls, v: Any) -> str:
        if v is None:
            return "aws"
        provider = str(v).lower()
        if provider != "aws":
            raise ValueError("Only cloud.provider='aws' is supported")
        return provider

    def launch_template(self) -> tuple[str | None, str | None, str]:
        """Return ``(template_id, template_version, region)`` for AWS."""
        return self.aws.launch_template_id, self.aws.launch_template_version, self.aws.region

    def object_store_bucket(self) -> str | None:
        """Return S3 bucket name."""
        return self.aws.bucket or None

    def is_object_store_configured(self) -> bool:
        """True when the active provider has a non-empty object store target."""
        return bool(self.object_store_bucket())


class AttributionConfig(BaseModel):
    """MinerU §2 attribution — on by default; this is a license obligation."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    powered_by_header: str = "MinerU"
    version_header: str = "MinerU-Version"


class AuthConfig(BaseModel):
    """API-key auth. Disabled by default — until enabled, the gateway must be network-isolated."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    api_key: str | None = None

    @model_validator(mode="after")
    def _validate_auth(self) -> AuthConfig:
        if self.enabled and not self.api_key:
            raise ValueError("auth.api_key is required when auth.enabled is true")
        return self

    def resolved_keys(self) -> list[str]:
        return [self.api_key] if self.api_key else []


class OtelConfig(BaseModel):
    """OpenTelemetry observability — OTLP exporter, off by default."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    endpoint: str | None = None
    service_name: str = "mineru-gateway"
    scheduler_service_name: str = "mineru-scheduler"
    metrics_export_interval_seconds: int = 60


# --- Top-level settings ------------------------------------------------------


class GatewaySettings(BaseSettings):
    """Top-level gateway settings."""

    model_config = SettingsConfigDict(env_prefix="MINERU_GATEWAY_", env_nested_delimiter="__", extra="forbid")

    deployment_id: str = "dev-local"
    database_url: str = "sqlite+aiosqlite:///./gateway.db"
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"
    max_file_size_bytes: int = 0
    task_sla_seconds: int = 3600
    cloud_workers_enabled: bool = False

    scaling: ScalingConfig = Field(default_factory=ScalingConfig)
    reconciliation: ReconciliationConfig = Field(default_factory=ReconciliationConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    rotation: RotationConfig = Field(default_factory=RotationConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    attribution: AttributionConfig = Field(default_factory=AttributionConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    otel: OtelConfig = Field(default_factory=OtelConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Highest-priority first: pydantic merges with ``deep_update(source, state)`` so
        # accumulated ``state`` wins — earlier tuple entries beat later ones.
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
        if _yaml_config_path is not None:
            sources.append(YamlSettingsSource(settings_cls, _yaml_config_path))
        return tuple(sources)

    @model_validator(mode="after")
    def _validate_deployment_and_cloud(self) -> GatewaySettings:
        if not self.deployment_id.strip():
            raise ValueError("deployment_id is required")
        if self.max_file_size_bytes < 0:
            raise ValueError("max_file_size_bytes must be >= 0")
        if self.max_file_size_bytes > MAX_FILE_SIZE_HARD_BYTES:
            raise ValueError(f"max_file_size_bytes cannot exceed hard limit of {MAX_FILE_SIZE_HARD_BYTES}")
        if self.cloud_workers_enabled and self.cloud.provider == "aws" and not self.cloud.aws.launch_template_id:
            raise ValueError("cloud.aws.launch_template_id is required when cloud workers are enabled")
        bucket = self.cloud.object_store_bucket()
        if not bucket:
            raise ValueError(f"cloud.{self.cloud.provider} bucket/container name is required")
        return self


# --- YAML config source (lower priority than env) --------------------------------

_yaml_config_path: Path | None = None


class YamlSettingsSource(PydanticBaseSettingsSource):
    """Load ``config.yaml`` when present — priority below env and CLI init overrides."""

    def __init__(self, settings_cls: type[BaseSettings], path: Path) -> None:
        super().__init__(settings_cls)
        self._data = self._load(path)

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: top-level YAML must be a mapping, got {type(data).__name__}")
        return data

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        if field_name in self._data:
            return self._data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return self._data


def load_settings(config_path: str | Path | None = None, **cli_overrides: Any) -> GatewaySettings:
    """Build settings: defaults ← YAML ← env ← ``cli_overrides`` (highest)."""
    global _cached_settings, _yaml_config_path
    _yaml_config_path = Path(config_path or DEFAULT_CONFIG_PATH)
    _cached_settings = GatewaySettings(**cli_overrides)
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
