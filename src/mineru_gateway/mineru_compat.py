"""Compatibility shim for MinerU primitives used by the gateway."""

from __future__ import annotations

from mineru.cli.common import normalize_upload_filename
from mineru.cli.public_http_client_policy import is_public_bind_host, validate_public_http_client_request
from mineru.cli.router import MultipartPayload, StagedUpload, submit_payload_to_upstream
from mineru.version import __version__ as MINERU_VERSION


def validate_mineru_compat() -> str:
    """Assert supported MinerU protocol/version at process startup."""
    from mineru.cli.api_protocol import API_PROTOCOL_VERSION

    if API_PROTOCOL_VERSION != 2:
        raise RuntimeError(
            f"Expected MinerU API_PROTOCOL_VERSION == 2, got {API_PROTOCOL_VERSION}. "
            "The gateway needs updating for the new protocol."
        )
    major, minor = (int(p) for p in MINERU_VERSION.split(".")[:2])
    if (major, minor) != (3, 4):
        raise RuntimeError(f"MinerU version {MINERU_VERSION} is outside the supported range >=3.4.4,<3.5.")
    return MINERU_VERSION


__all__ = [
    "MINERU_VERSION",
    "MultipartPayload",
    "StagedUpload",
    "is_public_bind_host",
    "normalize_upload_filename",
    "submit_payload_to_upstream",
    "validate_mineru_compat",
    "validate_public_http_client_request",
]
