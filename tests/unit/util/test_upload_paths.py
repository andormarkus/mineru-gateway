"""Upload path sanitization tests."""

from __future__ import annotations

import tempfile

import pytest

from mineru_gateway.util.upload_paths import UnsafeUploadNameError, internal_upload_path, sanitize_client_filename


def test_rejects_absolute_path() -> None:
    with pytest.raises(UnsafeUploadNameError):
        sanitize_client_filename("/etc/passwd")


def test_rejects_traversal() -> None:
    with pytest.raises(UnsafeUploadNameError):
        sanitize_client_filename("../../secret.pdf")


def test_rejects_path_separators() -> None:
    with pytest.raises(UnsafeUploadNameError):
        sanitize_client_filename("dir/file.pdf")
    with pytest.raises(UnsafeUploadNameError):
        sanitize_client_filename("dir\\file.pdf")


def test_internal_path_stays_in_temp_dir() -> None:
    from pathlib import Path

    with tempfile.TemporaryDirectory() as temp_dir:
        path, safe = internal_upload_path(temp_dir, client_name="doc.pdf")
        assert safe == "doc.pdf"
        assert Path(path).resolve().is_relative_to(Path(temp_dir).resolve())
