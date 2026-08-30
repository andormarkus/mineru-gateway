# Ship Gate — SHIPPED (v0.1.0)

This was the final bounded implementation scope before the first release. It shipped.
Kept as a record of what v0.1.0 committed to; the acceptance list doubles as a
regression checklist when touching the worker lifecycle.

## Force-terminate stalled workers — shipped

A worker that reaches `reconciliation.max_failure_count` enters a bounded drain
(`reconciliation.stalled_worker_grace_seconds`, default 900s) and is force-terminated
after the grace period even if work remains. Stalled/terminated-intent workers never
count as serviceable capacity, so replacements launch automatically (temporary overshoot
of `max_workers` is outgoing inventory, not steady state). Manual recovery via
`POST /admin/workers/{id}/recover` clears `failure_count`, `retry_after`, `stalled_at`,
and the drain intent.

Regression tests: `tests/unit/scheduler/test_stalled_workers.py`.
Real-AWS coverage: `tests/e2e/test_cloud_scheduler.py` (`MINERU_GATEWAY_E2E=1` required).

## Deployment configuration — verified during the sandbox bring-up

- [ ] `max_file_size_bytes` positive (100 MiB in `config.example.yaml` and the sandbox config)
- [ ] API-key auth enabled (example config ships it on; startup guard refuses public binds without it)
- [ ] Network restricted (private workers + SSM-only host, zero inbound SG rules)
- [ ] Object-storage lifecycle expiration documented (README → Object storage)

Boxes are ticked when the sandbox deployment verifies each item.

## Final ship gate

1. [x] Stalled-worker behavior implemented and tested
2. [ ] Normal request path `enqueue → dispatch → process → store → retrieve` verified end-to-end against the deployed stack
3. [x] Failed workers stop receiving tasks and terminate
4. [x] Replacement capacity starts without administrator intervention
5. [ ] Required deployment settings configured and verified in the sandbox deployment

The accepted edge cases remain in `KNOWN_LIMITATIONS.md` and are still not scope.
