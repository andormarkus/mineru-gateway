"""Pure desired-capacity calculation from queue depth."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ScalingInputs:
    """Snapshot inputs for one autoscale decision (counts are independent reads)."""

    queue_depth: int
    serviceable_workers: int
    starting_workers: int
    draining_workers: int
    stopping_workers: int


@dataclass
class ScalingSignal:
    queue_depth: int
    serviceable_workers: int
    starting_workers: int
    draining_workers: int
    stopping_workers: int
    desired_workers: int
    target: float

    @property
    def scaling_in_progress(self) -> bool:
        return self.starting_workers > 0 or self.draining_workers > 0 or self.stopping_workers > 0

    @property
    def should_scale_up(self) -> bool:
        return (self.serviceable_workers + self.starting_workers) < self.desired_workers

    @property
    def should_scale_down(self) -> bool:
        # Idle drain only when no scale-up (booting) or scale-down (drain/stop) is active.
        return self.serviceable_workers > self.desired_workers and not self.scaling_in_progress


def compute_scaling_signal(
    *,
    inputs: ScalingInputs,
    target_per_worker: int,
    min_workers: int,
    max_workers: int,
) -> ScalingSignal:
    queue_desired = 0 if inputs.queue_depth == 0 else math.ceil(inputs.queue_depth / target_per_worker)
    floor = max(queue_desired, 1 if inputs.queue_depth > 0 else 0)
    desired_workers = max(min_workers, min(floor, max_workers))
    return ScalingSignal(
        queue_depth=inputs.queue_depth,
        serviceable_workers=inputs.serviceable_workers,
        starting_workers=inputs.starting_workers,
        draining_workers=inputs.draining_workers,
        stopping_workers=inputs.stopping_workers,
        desired_workers=desired_workers,
        target=float(target_per_worker),
    )
