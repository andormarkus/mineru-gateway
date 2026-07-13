"""Shared bounded I/O helpers for chunked reads with byte-limit enforcement.

Both the gateway (packing staged uploads into a payload blob) and the scheduler (hashing
staged files for the dedup cache) read staged files in fixed-size chunks while enforcing the
configured byte limit. This module owns the shared chunk size and the bounded-read primitive.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mineru_gateway.config import GatewaySettings

# 1 MiB — the chunk size used by every bounded file/stream read in the codebase.
READ_CHUNK_SIZE = 1 << 20


def iter_bounded_file(path: str, *, settings: GatewaySettings, label: str = "multipart upload") -> Iterator[bytes]:
    """Yield fixed-size chunks from a file, enforcing the configured byte limit per read.

    Callers that need the whole file (e.g. ``_pack_payload``) join the chunks; callers that
    hash incrementally (e.g. ``hash_file_at_path``) feed each chunk to a digest. The byte
    limit is checked after each chunk so an oversized file is rejected as soon as it crosses
    the threshold rather than after a full read.
    """
    from mineru_gateway.gateway.admission import enforce_byte_limit

    total = 0
    with open(path, "rb") as handle:
        while chunk := handle.read(READ_CHUNK_SIZE):
            total += len(chunk)
            enforce_byte_limit(total, settings=settings, label=label)
            yield chunk
