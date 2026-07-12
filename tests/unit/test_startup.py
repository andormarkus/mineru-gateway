"""Startup guard: refuse public bind without auth."""

from __future__ import annotations

import pytest

from mineru_gateway.config import AuthConfig, GatewaySettings
from mineru_gateway.startup_guard import PublicBindWithoutAuthError, enforce_bind_guard


def test_startup_guard_rejects_public_host_without_auth() -> None:
    settings = GatewaySettings(host="0.0.0.0", auth=AuthConfig(enabled=False))
    with pytest.raises(PublicBindWithoutAuthError, match="Refusing to bind"):
        enforce_bind_guard(settings)


def test_startup_guard_allows_public_host_with_auth() -> None:
    settings = GatewaySettings(host="0.0.0.0", auth=AuthConfig(enabled=True, api_key="test-key"))
    enforce_bind_guard(settings)


def test_startup_guard_allows_loopback_without_auth() -> None:
    settings = GatewaySettings(host="127.0.0.1", auth=AuthConfig(enabled=False))
    enforce_bind_guard(settings)


def test_startup_guard_cli_host_takes_precedence() -> None:
    settings = GatewaySettings(host="127.0.0.1")
    with pytest.raises(PublicBindWithoutAuthError):
        enforce_bind_guard(settings, cli_host="0.0.0.0")
