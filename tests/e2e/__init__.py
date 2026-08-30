"""Tier-4 AWS EC2 end-to-end tests.

Two suites (run separately to control cost):

**Smoke** (``test_cloud_ec2.py``) — one warm worker, health + single task.

**Scheduler** (``test_cloud_scheduler.py``) — ordered tests on real AWS for:
  - scale from zero
  - autoscale up (2nd worker)
  - admin rotation + replacement EC2
  - autoscaler idle drain
  - admin drain terminate

These tests launch real GPU instances and cost money. They are skipped unless
``MINERU_GATEWAY_E2E=1`` is set explicitly (the Taskfile ``test-e2e*`` targets
set it for you) — a shell that merely still exports the cloud config will not
trip them.

Prerequisites:
  export MINERU_GATEWAY_E2E=1
  export AWS_REGION=eu-central-1
  export AWS_PROFILE=your-profile
  export MINERU_GATEWAY_CLOUD__AWS__LAUNCH_TEMPLATE_ID=lt-...
  export MINERU_GATEWAY_CLOUD__AWS__BUCKET=your-bucket
  export MINERU_GATEWAY_CLOUD__AWS__WORKER_ADDRESS=public
  export MINERU_GATEWAY_DEPLOYMENT_ID=e2e-pytest

Run smoke:  ``task test-e2e-smoke``
Run scheduler: ``task test-e2e-scheduler``

Optional tuning (same env vars as production):
  ``MINERU_GATEWAY_SCHEDULER__RECONCILE_POLL_INTERVAL_SECONDS`` (default 15)
  ``MINERU_GATEWAY_SCALING__IDLE_COOLDOWN_SECONDS`` (scheduler suite defaults to 60 in conftest)
  ``MINERU_GATEWAY_RECONCILIATION__LAUNCH_READINESS_TIMEOUT_SECONDS`` (default 1800)
  ``MINERU_GATEWAY_LOG_LEVEL`` (default INFO)
"""
