"""Scaling policy: target-tracking signal computation.

Signal = sum(pending + processing tasks) / count(running healthy workers). Cloud-agnostic (DB-side), survives restart,
maps onto AWS target-tracking later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import func, select

from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Task, Worker

logger = logging.getLogger(__name__)


@dataclass
class ScalingSignal:
    """The autoscaler's view of current load vs. capacity."""

    queue_depth: int  # pending + processing tasks
    running_workers: int  # healthy, enabled, running workers
    signal: float  # queue_depth / max(1, running_workers)
    target: float  # target_per_worker

    @property
    def should_scale_up(self) -> bool:
        """True when load exceeds the target per-worker threshold."""
        return self.signal > self.target


async def compute_signal(target_per_worker: float = 4.0) -> ScalingSignal:
    """Compute the current scaling signal from DB state."""
    async with get_db_session() as session:
        depth_stmt = select(func.count(Task.task_id)).where(Task.status.in_(["pending", "processing"]))
        queue_depth = (await session.execute(depth_stmt)).scalar() or 0

        workers_stmt = select(func.count(Worker.id)).where(
            Worker.enabled.is_(True), Worker.healthy.is_(True), Worker.state == "running"
        )
        running = (await session.execute(workers_stmt)).scalar() or 0

    signal = queue_depth / max(1, running)
    result = ScalingSignal(queue_depth=queue_depth, running_workers=running, signal=signal, target=target_per_worker)
    logger.debug(
        "Scaling signal: queue=%d running=%d signal=%.2f target=%.1f scale_up=%s",
        queue_depth,
        running,
        signal,
        target_per_worker,
        result.should_scale_up,
    )
    return result
