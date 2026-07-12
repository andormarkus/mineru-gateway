"""Scaling-event audit helper — append rows to ``scaling_events``.

Used by scheduler autoscaler, rotation, and worker-drain loops when workers are
started, stopped, or launched. Not a cloud provider — just shared DB write logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mineru_gateway.db.models import ScalingEvent

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def record_scaling_event(
    session: AsyncSession,
    *,
    action: str,
    reason: str,
    worker_id: str | None = None,
    triggered_by: str = "autoscaler",
    commit: bool = False,
) -> None:
    """Append a ``ScalingEvent`` audit row."""
    session.add(ScalingEvent(worker_id=worker_id, action=action, reason=reason, triggered_by=triggered_by))
    if commit:
        await session.commit()
