"""Regression tests for scheduler/config bugbot findings."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from mineru_gateway.config import get_settings, load_settings, reset_settings_cache


def test_load_settings_caches_custom_config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI --config path is visible to get_settings() after load_settings()."""
    monkeypatch.delenv("MINERU_GATEWAY_DATABASE_URL", raising=False)
    reset_settings_cache()
    config = tmp_path / "custom.yaml"
    config.write_text("database_url: sqlite+aiosqlite:///./from-custom.yaml\n", encoding="utf-8")

    load_settings(config)
    assert get_settings().database_url == "sqlite+aiosqlite:///./from-custom.yaml"

    reset_settings_cache()


def test_rejects_unknown_top_level_config_key(tmp_path: Path) -> None:
    reset_settings_cache()
    config = tmp_path / "bad.yaml"
    config.write_text("not_a_real_setting: true\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_settings(config)

    reset_settings_cache()


def test_env_overrides_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = tmp_path / "custom.yaml"
    config.write_text("database_url: sqlite+aiosqlite:///./from-yaml.db\n", encoding="utf-8")
    monkeypatch.setenv("MINERU_GATEWAY_DATABASE_URL", "sqlite+aiosqlite:///./from-env.db")
    reset_settings_cache()
    settings = load_settings(config)
    assert settings.database_url == "sqlite+aiosqlite:///./from-env.db"
    reset_settings_cache()


def test_cli_overrides_env_and_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = tmp_path / "custom.yaml"
    config.write_text("database_url: sqlite+aiosqlite:///./from-yaml.db\n", encoding="utf-8")
    monkeypatch.setenv("MINERU_GATEWAY_DATABASE_URL", "sqlite+aiosqlite:///./from-env.db")
    reset_settings_cache()
    settings = load_settings(config, database_url="sqlite+aiosqlite:///./from-cli.db")
    assert settings.database_url == "sqlite+aiosqlite:///./from-cli.db"
    reset_settings_cache()
