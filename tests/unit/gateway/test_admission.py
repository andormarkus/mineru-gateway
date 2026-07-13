"""Gateway admission: upload size limits."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from mineru_gateway.config import MAX_FILE_SIZE_HARD_BYTES, GatewaySettings
from mineru_gateway.gateway.admission import check_file_size, effective_file_size_limit, enforce_byte_limit


def test_effective_limit_uses_soft_when_set() -> None:
    settings = GatewaySettings(max_file_size_bytes=100)
    assert effective_file_size_limit(settings) == 100


def test_effective_limit_uses_hard_when_soft_unset() -> None:
    settings = GatewaySettings(max_file_size_bytes=0)
    assert effective_file_size_limit(settings) == MAX_FILE_SIZE_HARD_BYTES


def test_check_file_size_rejects_over_soft_limit() -> None:
    settings = GatewaySettings(max_file_size_bytes=100)
    with pytest.raises(HTTPException) as exc:
        check_file_size(200, settings=settings)
    assert exc.value.status_code == 413


def test_enforce_byte_limit_rejects_over_soft_limit() -> None:
    settings = GatewaySettings(max_file_size_bytes=50)
    with pytest.raises(HTTPException) as exc:
        enforce_byte_limit(100, settings=settings)
    assert exc.value.status_code == 413


def test_hard_limit_rejects_when_soft_unset() -> None:
    settings = GatewaySettings(max_file_size_bytes=0)
    with pytest.raises(HTTPException) as exc:
        enforce_byte_limit(MAX_FILE_SIZE_HARD_BYTES + 1, settings=settings)
    assert exc.value.status_code == 413


def test_hard_limit_allows_at_cap_when_soft_unset() -> None:
    settings = GatewaySettings(max_file_size_bytes=0)
    enforce_byte_limit(MAX_FILE_SIZE_HARD_BYTES, settings=settings)


def test_rejects_soft_limit_above_hard_cap() -> None:
    with pytest.raises(ValidationError, match="max_file_size_bytes cannot exceed hard limit"):
        GatewaySettings(max_file_size_bytes=MAX_FILE_SIZE_HARD_BYTES + 1)
