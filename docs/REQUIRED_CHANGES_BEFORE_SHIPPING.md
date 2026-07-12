# Required Changes Before Shipping

This is the final bounded implementation scope for the internal gateway. Do not add unrelated architecture or address the accepted edge cases documented in `KNOWN_LIMITATIONS.md`.

## Force-terminate stalled workers

A worker that reaches `reconciliation.max_failure_count` must enter a bounded drain period and must not remain indefinitely in active worker inventory.

### 1. Add configuration

Add one reconciliation setting:

```yaml
reconciliation:
  stalled_worker_grace_seconds: 900
```

The default is 900 seconds (15 minutes).

### 2. Add durable stalled state

Add a nullable `stalled_at` timestamp to:

- the `Worker` model;
- the initial Alembic migration.

`stalled_at` records when the worker first reaches `max_failure_count`. Do not use `updated_at` because health checks and other writes can continually change it.

### 3. Update the failure transition

When `WorkerRepository.record_failure()` raises `failure_count` to `max_failure_count`:

1. Set `stalled_at` if it is not already set.
2. Set `healthy=False`.
3. Set `draining=True`.
4. Set `drain_target="terminated"`.
5. Stop dispatching new tasks to the worker.

Manual recovery must clear:

- `failure_count`;
- `retry_after`;
- `stalled_at`;
- the automatic stalled-worker drain intent, when applicable.

### 4. Converge stalled workers

Update the scheduler:

1. If a stalled worker has no inflight tasks or unstored results, set `desired_state="terminated"` immediately.
2. If it still has work and the grace period has not elapsed, leave it draining.
3. When the grace period expires, set `desired_state="terminated"` even if work remains.
4. Continue reconciling stalled workers that have termination intent.
5. Call the cloud provider's `terminate()` operation.
6. Finalize the worker row after the cloud reports it terminated.

The reconciliation loop must not skip a stalled worker whose `desired_state` is `terminated`.

### 5. Allow replacement capacity

Stalled and terminated-intent workers must not count as serviceable or starting capacity.

Autoscaling may create replacements while old failed workers drain. This can temporarily exceed `max_workers`; `max_workers` remains the steady-state capacity limit, while failed draining workers are outgoing inventory.

Keep this implementation simple:

- do not add lifecycle-operation tables;
- do not add a generic workflow engine;
- do not require manual recovery to restore capacity;
- preserve terminated worker rows for audit history.

### Acceptance criteria

- A stalled worker receives no new tasks.
- A stalled worker with no active work terminates immediately.
- A stalled worker with active work gets at most 15 minutes to drain.
- The cloud provider termination call occurs after the deadline.
- A stalled row cannot permanently block replacement capacity.
- Zero serviceable workers can recover automatically.

## Required deployment configuration

Before deployment:

1. Set `max_file_size_bytes` to a positive value appropriate for expected documents.
2. Enable API-key authentication.
3. Restrict network access to internal callers.
4. Configure object-storage lifecycle expiration for:
   - `payloads/`;
   - `results/`;
   - `cache/`.

## Final ship gate

Shipping requires only:

1. The stalled-worker behavior above is implemented.
2. A normal request follows `enqueue -> dispatch -> process -> store result -> retrieve result`.
3. A failed worker stops receiving tasks and is eventually terminated.
4. Replacement capacity starts without administrator intervention.
5. Required deployment settings are configured.

Do not start another broad architecture or edge-case review before shipping.
