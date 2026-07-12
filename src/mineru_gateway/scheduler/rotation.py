"""Tier B rotation loop (CLOUD_WORKERS.md) — leader-only, scheduled + template-version-change.

Launches fresh instances from the current launch template, drains old ones, and terminates them.
Gradual: one at a time, never bulk-replace (preserves capacity). Audits to ``scaling_events``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from mineru_gateway.cloud.base import CloudWorkerProvider
from mineru_gateway.config import get_settings
from mineru_gateway.db.base import get_db_session
from mineru_gateway.db.models import Worker
from mineru_gateway.scaling.audit import record_scaling_event
from mineru_gateway.util.datetime import ensure_aware_utc

logger = logging.getLogger(__name__)


async def run_rotation(provider: CloudWorkerProvider | None) -> int:
    """Check if rotation is due, and if so, rotate one instance. Returns count rotated (0 or 1)."""
    settings = get_settings()
    interval = settings.rotation.interval_seconds

    async with get_db_session() as session:
        # Find the oldest running cloud worker.
        stmt = (
            select(Worker)
            .where(Worker.enabled.is_(True), Worker.state == "running", Worker.cloud_instance_id.isnot(None))
            .order_by(Worker.created_at.asc())
        )
        oldest = (await session.execute(stmt)).scalars().first()

        if oldest is None or provider is None:
            return 0

        # Check if rotation is due by age.
        created = ensure_aware_utc(oldest.created_at)
        age = datetime.now(UTC) - created

        if created > datetime.now(UTC) - timedelta(seconds=interval):
            logger.debug(
                "Rotation not due for worker %s (age=%.0fs, interval=%ds)",
                oldest.id,
                age.total_seconds(),
                interval,
            )
            return 0  # not due yet

        cloud = settings.cloud
        template_id, template_version, region = cloud.launch_template()
        if not template_id:
            logger.warning("Rotation due but no launch_template_id configured for provider %s", cloud.provider)
            return 0

        # Launch a fresh instance from the current template.
        logger.info(
            "Rotating: launching fresh instance to replace %s (age: %.0f days)",
            oldest.id,
            (datetime.now(UTC) - created).days,
        )
        try:
            new_instance_id = await provider.launch_instance(template_id, version=template_version)
            ip = await provider.get_private_ip(new_instance_id)

            # Register the new worker.
            new_worker = Worker(
                id=f"cloud-{new_instance_id}",
                source=cloud.provider,
                cloud_instance_id=new_instance_id,
                cloud_region=region,
                base_url=f"http://{ip}:8000",
                state="starting",
            )
            session.add(new_worker)

            # Mark the old worker for draining (the drain loop will finish it).
            oldest.state = "draining"

            await record_scaling_event(
                session,
                action="stop",
                reason=f"rotation: replacing with {new_instance_id}",
                worker_id=oldest.id,
                triggered_by="autoscaler",
            )
            await session.commit()
            logger.info(
                "Rotation started: worker %s → cloud-%s (old worker now draining)",
                oldest.id,
                new_instance_id,
            )
            return 1
        except Exception:
            logger.exception("Rotation launch failed")
            return 0
