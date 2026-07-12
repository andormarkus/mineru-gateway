"""Pure desired-capacity calculation from queue depth."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class ScalingSignal:
    queue_depth: int
    serviceable_workers: int
    starting_workers: int
    desired_workers: int
    target: float

    @property
    def should_scale_up(self) -> bool:
        return (self.serviceable_workers + self.starting_workers) < self.desired_workers

    @property
    def should_scale_down(self) -> bool:
        return self.serviceable_workers > self.desired_workers


def compute_scaling_signal(
    *,
    queue_depth: int,
    serviceable_workers: int,
    starting_workers: int,
    target_per_worker: int,
    min_workers: int,
    max_workers: int,
) -> ScalingSignal:
    queue_desired = 0 if queue_depth == 0 else math.ceil(queue_depth / target_per_worker)
    floor = max(queue_desired, 1 if queue_depth > 0 else 0)
    desired_workers = max(min_workers, min(floor, max_workers))
    return ScalingSignal(
        queue_depth=queue_depth,
        serviceable_workers=serviceable_workers,
        starting_workers=starting_workers,
        desired_workers=desired_workers,
        target=float(target_per_worker),
    )
