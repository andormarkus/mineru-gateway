"""Shared datetime helpers (UTC normalization + ISO serialization).

Consolidates the three duplicated ``_dt_to_iso``/``_iso``/``_now_utc`` helpers and the inline
``if dt.tzinfo is None: dt = dt.replace(tzinfo=UTC)`` blocks that were spread across the DB registry,
worker manager, admin API, and scheduler loops.
"""

from __future__ import annotations

from datetime import UTC, datetime


def ensure_aware_utc(dt: datetime) -> datetime:
    """Return ``dt`` with UTC assumed if it is naive (no tzinfo).

    DB drivers (notably sqlite) can return naive datetimes; comparing them against aware ones raises.
    Normalize at every comparison boundary instead of repeating the guard at each call site.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def now_utc() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def to_iso(dt: datetime | None) -> str | None:
    """ISO-8601 string for a datetime (assuming UTC if naive), or ``None`` if ``dt`` is ``None``."""
    if dt is None:
        return None
    return ensure_aware_utc(dt).isoformat()
