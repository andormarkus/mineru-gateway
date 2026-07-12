"""Task status normalization tests."""

from __future__ import annotations

from mineru_gateway.tasks.status import TASK_DISPATCHING, TASK_PROCESSING, normalize_upstream_status


def test_pending_maps_to_processing() -> None:
    assert normalize_upstream_status("pending", current="dispatching") == TASK_PROCESSING


def test_queued_maps_to_processing_after_accept() -> None:
    assert normalize_upstream_status("queued", current=TASK_PROCESSING) == TASK_PROCESSING


def test_processing_rejects_regression_to_queued() -> None:
    assert normalize_upstream_status("queued", current=TASK_PROCESSING) == TASK_PROCESSING
    assert normalize_upstream_status("queued", current=TASK_DISPATCHING) == TASK_PROCESSING


def test_unknown_status_preserves_current() -> None:
    assert normalize_upstream_status("weird", current="processing") == "processing"
