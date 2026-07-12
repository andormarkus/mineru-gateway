"""Tests for cross-cloud InstanceState enum."""

from __future__ import annotations

from mineru_gateway.cloud.types import InstanceState


def test_instance_state_values() -> None:
    assert InstanceState.RUNNING == "running"
    assert InstanceState.SUSPENDED == "suspended"
    assert InstanceState.UNKNOWN == "unknown"
