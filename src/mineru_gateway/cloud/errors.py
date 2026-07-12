"""Structured cloud provider errors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CloudErrorCategory(StrEnum):
    RETRYABLE = "retryable"
    NOT_FOUND = "not_found"
    AUTH = "auth"
    QUOTA = "quota"
    INVALID_ID = "invalid_id"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


@dataclass
class CloudError(Exception):
    """Provider error with classification for lifecycle retry logic."""

    category: CloudErrorCategory
    code: str
    message: str
    retryable: bool = False

    def __str__(self) -> str:
        return f"{self.category}:{self.code}: {self.message}"
