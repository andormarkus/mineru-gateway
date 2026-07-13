"""Short identifier generation for workers and cache generations.

Worker IDs and cache generations both use a 12-hex-char slice of ``uuid4``. Centralizing the
format keeps the prefix and length consistent across the worker repository and cache service.
"""

from __future__ import annotations

import uuid

# Worker IDs and cache generations share this random-suffix length.
_SHORT_ID_LENGTH = 12


def short_id() -> str:
    """Return a 12-hex-char random string (a slice of ``uuid4().hex``)."""
    return uuid.uuid4().hex[:_SHORT_ID_LENGTH]


def worker_id() -> str:
    """Return a deployment worker ID (``cloud-`` prefix + :func:`short_id`)."""
    return f"cloud-{short_id()}"
