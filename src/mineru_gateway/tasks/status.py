"""Internal task status constants and upstream normalization."""

from __future__ import annotations

from typing import Any

TASK_QUEUED = "queued"
TASK_DISPATCHING = "dispatching"
TASK_PROCESSING = "processing"
TASK_STORING_RESULT = "storing_result"
TASK_COMPLETED = "completed"
TASK_FAILED = "failed"
TASK_EXPIRED = "expired"

TASK_STATUSES_TERMINAL = frozenset({TASK_COMPLETED, TASK_FAILED, TASK_EXPIRED})

# Autoscaling queue depth — work still needing compute or dispatch slots.
TASK_STATUSES_AUTOSCALE_DEMAND = frozenset({TASK_QUEUED, TASK_DISPATCHING, TASK_PROCESSING})

# Per-worker compute slots while upstream work may still be running.
TASK_STATUSES_COMPUTE_CAPACITY = frozenset({TASK_DISPATCHING, TASK_PROCESSING})

# Blocks worker drain until upstream is terminal and durable result is handled.
TASK_STATUSES_DRAIN_BLOCKERS = frozenset({TASK_DISPATCHING, TASK_PROCESSING, TASK_STORING_RESULT})

_UPSTREAM_ACCEPTED_NONTERMINAL = frozenset({"queued", "pending", "processing"})

_UPSTREAM_TO_INTERNAL: dict[str, str] = {
    "completed": TASK_COMPLETED,
    "failed": TASK_FAILED,
    "expired": TASK_EXPIRED,
    TASK_DISPATCHING: TASK_DISPATCHING,
    TASK_PROCESSING: TASK_PROCESSING,
    TASK_STORING_RESULT: TASK_STORING_RESULT,
}


def normalize_upstream_status(status: str | None, *, current: str) -> str:
    """Map upstream worker status to internal task status."""
    if status is None:
        return current
    if status in _UPSTREAM_ACCEPTED_NONTERMINAL:
        mapped = TASK_PROCESSING
    else:
        mapped = _UPSTREAM_TO_INTERNAL.get(status)
        if mapped is None:
            return current
    if current in (TASK_PROCESSING, TASK_DISPATCHING, TASK_STORING_RESULT) and mapped in (
        TASK_QUEUED,
        TASK_DISPATCHING,
    ):
        return current
    return mapped


def apply_upstream_payload(row_status: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Return field updates from an upstream status payload."""
    updates: dict[str, Any] = {}
    if "status" in payload:
        updates["status"] = normalize_upstream_status(payload.get("status"), current=row_status)
    if "backend" in payload:
        updates["backend"] = payload["backend"]
    if "file_names" in payload:
        updates["file_names"] = list(payload["file_names"])
    if "error" in payload:
        updates["error"] = payload.get("error")
    return updates
