"""Gateway admission: upload size limits."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from mineru_gateway.config import GatewaySettings
from mineru_gateway.gateway.admission import check_file_size, enforce_byte_limit


def test_check_file_size_rejects_over_limit() -> None:
    settings = GatewaySettings(max_file_size_bytes=100)
    with pytest.raises(HTTPException) as exc:
        check_file_size(200, settings=settings)
    assert exc.value.status_code == 413


def test_enforce_byte_limit_rejects_over_limit() -> None:
    settings = GatewaySettings(max_file_size_bytes=50)
    with pytest.raises(HTTPException) as exc:
        enforce_byte_limit(100, settings=settings)
    assert exc.value.status_code == 413
