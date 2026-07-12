"""Safe temporary upload path handling."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mineru_gateway.mineru_compat import MultipartPayload


class UnsafeUploadNameError(ValueError):
    """Raised when a client upload name cannot be used safely."""


def sanitize_client_filename(name: str) -> str:
    """Return a safe display name for multipart metadata."""
    if not name or not name.strip():
        raise UnsafeUploadNameError("upload name is blank")
    if "\x00" in name:
        raise UnsafeUploadNameError("upload name contains NUL")
    if os.path.isabs(name):
        raise UnsafeUploadNameError("absolute upload paths are not allowed")
    if "/" in name or "\\" in name:
        raise UnsafeUploadNameError("path separators are not allowed in upload names")
    normalized = name.replace("\\", "/")
    parts = [part for part in normalized.split("/") if part not in ("", ".")]
    if any(part == ".." for part in parts):
        raise UnsafeUploadNameError("path traversal in upload name")
    safe = "/".join(parts)
    if not safe:
        raise UnsafeUploadNameError("upload name is empty after sanitization")
    return safe


def internal_upload_path(temp_dir: str, *, client_name: str) -> tuple[str, str]:
    """Return ``(internal_path, sanitized_client_name)`` under ``temp_dir``."""
    safe_name = sanitize_client_filename(client_name)
    internal_name = uuid.uuid4().hex
    dest = Path(temp_dir) / internal_name
    resolved = dest.resolve()
    base = Path(temp_dir).resolve()
    if not resolved.is_relative_to(base):
        raise UnsafeUploadNameError("resolved upload path escapes temp directory")
    return str(resolved), safe_name


def rewrite_staged_uploads(payload: MultipartPayload) -> None:
    """Rewrite staged uploads to internal paths under the payload temp directory."""
    import shutil

    from mineru_gateway.mineru_compat import StagedUpload

    temp_dir = payload.temp_dir
    rewritten: list[StagedUpload] = []
    for upload in payload.uploads:
        path, safe_name = internal_upload_path(temp_dir, client_name=upload.upload_name)
        shutil.move(upload.path, path)
        rewritten.append(
            StagedUpload(
                field_name=upload.field_name, upload_name=safe_name, content_type=upload.content_type, path=path
            )
        )
    payload.uploads = rewritten
