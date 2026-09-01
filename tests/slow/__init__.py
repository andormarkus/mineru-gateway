"""Tier-3 slow tests against a real external mineru-api worker.

Prerequisites:
  - ``task up`` for local SeaweedFS (S3 on :8333)
  - ``MINERU_TEST_WORKER_URL`` pointing at a manually registered mineru-api worker
  - ``MINERU_GATEWAY_CLOUD_WORKERS_ENABLED=false`` (default in slow fixtures)

## Pre-autoscaling coverage (IN SCOPE)

These tests exercise the static-worker path before EC2 autoscaling kicks in:

| Area | Slow test module |
|------|------------------|
| Worker protocol (/health, /tasks) | ``test_worker_protocol.py`` |
| Scheduler health probe + dispatch gate | ``test_worker_health_gateway.py`` |
| POST /tasks, GET /tasks/{id}/result | ``test_gateway_e2e.py`` |
| POST /v1/ocr | ``test_gateway_e2e.py`` |
| GET /health, /ready, / | ``test_pre_autoscaling.py`` |
| GET /tasks/{id} status API | ``test_pre_autoscaling.py`` |
| POST /file_parse | ``test_pre_autoscaling.py`` |
| Cache dedup + force bypass | ``test_pre_autoscaling.py`` |
| Admin workers drain/recover | ``test_pre_autoscaling.py`` |
| Admin cache invalidate/sweep | ``test_pre_autoscaling.py`` |

Scheduler loop in slow tests runs: health probe, stale-claim recovery, result sync,
SLA expiry, dispatch. It does **not** run reconcile, autoscale, rotation, or retention.

## Out of scope (requires cloud_workers_enabled + EC2/moto)

- EC2 worker launch / terminate / reconcile
- Queue-depth autoscaling (_apply_autoscaling)
- Scheduled worker rotation with replacement VMs
- Retention sweeper deleting old S3 objects
- Stalled-worker drain-to-terminate convergence

Run: ``task test-slow`` or ``pytest tests/slow -m slow``
"""
