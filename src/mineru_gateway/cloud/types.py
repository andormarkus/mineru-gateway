"""Cross-cloud normalized types for compute lifecycle."""

from __future__ import annotations

from enum import StrEnum


class InstanceState(StrEnum):
    """Vendor-neutral VM power/lifecycle state."""

    RUNNING = "running"
    SUSPENDED = "suspended"
    STARTING = "starting"
    STOPPING = "stopping"
    TERMINATED = "terminated"
    UNKNOWN = "unknown"
