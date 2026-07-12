"""Cross-cloud normalized types for compute lifecycle."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class InstanceState(StrEnum):
    """Vendor-neutral VM power/lifecycle state."""

    RUNNING = "running"
    SUSPENDED = "suspended"
    STARTING = "starting"
    STOPPING = "stopping"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    UNKNOWN = "unknown"


# Persisted Worker.cloud_state values.
CLOUD_STATE_PENDING = "pending"
CLOUD_STATE_RUNNING = "running"
CLOUD_STATE_STOPPING = "stopping"
CLOUD_STATE_TERMINATING = "terminating"
CLOUD_STATE_STOPPED = "stopped"
CLOUD_STATE_TERMINATED = "terminated"
CLOUD_STATE_UNKNOWN = "unknown"

_INSTANCE_TO_CLOUD_STATE: dict[InstanceState, str] = {
    InstanceState.RUNNING: CLOUD_STATE_RUNNING,
    InstanceState.SUSPENDED: CLOUD_STATE_STOPPED,
    InstanceState.STARTING: CLOUD_STATE_PENDING,
    InstanceState.STOPPING: CLOUD_STATE_STOPPING,
    InstanceState.TERMINATING: CLOUD_STATE_TERMINATING,
    InstanceState.TERMINATED: CLOUD_STATE_TERMINATED,
    InstanceState.UNKNOWN: CLOUD_STATE_UNKNOWN,
}


def cloud_state_from_instance(state: InstanceState) -> str:
    return _INSTANCE_TO_CLOUD_STATE.get(state, CLOUD_STATE_UNKNOWN)


# EC2 / VM tag keys for managed workers.
TAG_MANAGED = "mineru-gateway-managed"
TAG_DEPLOYMENT = "mineru-gateway-deployment"
TAG_WORKER_ID = "mineru-gateway-worker-id"
TAG_ROLE = "mineru-gateway-role"
TAG_GENERATION = "mineru-gateway-generation"
TAG_ROLE_WORKER = "worker"


def build_worker_tags(*, worker_id: str, deployment_id: str, generation: int) -> dict[str, str]:
    return {
        TAG_MANAGED: "true",
        TAG_DEPLOYMENT: deployment_id,
        TAG_WORKER_ID: worker_id,
        TAG_ROLE: TAG_ROLE_WORKER,
        TAG_GENERATION: str(generation),
    }


@dataclass(frozen=True, slots=True)
class DiscoveredInstance:
    """A controller-managed VM discovered via deployment tags."""

    instance_id: str
    worker_id: str | None
    state: InstanceState
    tags: dict[str, str] = field(default_factory=dict)
