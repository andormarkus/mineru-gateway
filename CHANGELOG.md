# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-08-30

First shipped version. Salvaged and hardened the July control-plane work,
closed the ship gate (`docs/REQUIRED_CHANGES_BEFORE_SHIPPING.md`), and made the
sandbox deployment repeatable over SSM with a closed topology.

### Added
- Scheduler split into a 0.5s dispatch loop and a 15s wall-clock-aligned
  reconcile loop (`scheduler.dispatch_/reconcile_poll_interval_seconds`).
- Worker provisioning observability: `Worker.provisioning_detail`, `:8001`
  bootstrap-status responder polled while workers build (both worker CF
  templates), exposed via the admin workers API.
- Autoscaler hysteresis: no scale-down while scaling in progress, no idle-drain
  mid-rotation, bootstrapping-but-running workers count as starting capacity.
- `cloud.aws.worker_address` (`private` | `public`) and
  `ComputeProvider.get_public_ip()`.
- Worker CloudFormation stacks (`deploy/cloudformation/mineru-worker*.yaml`)
  with full GPU bootstrap user-data and the status responder.
- Controller stack (`controller.yaml`) — the control plane on one SSM-operated
  instance: gateway + scheduler + Postgres via compose, private subnet, no
  public IP, no key pair, no inbound SG rules, least-privilege scheduler role
  (EC2 + S3 + scoped PassRole + SSM parameter reads). No resource in the
  topology is internet-reachable. The stack also creates the results bucket
  (public access blocked, lifecycle orphan-expiry) unless given an existing
  one; the worker stack publishes its launch-template id to SSM Parameter
  Store, and the gateway config resolves `ssm:/...` references — onboarding is
  two create-stack calls plus a compose-up with no transcribed ids.
- Sandbox compose stack (`deploy/compose/docker-compose.sandbox.yml`):
  gateway + scheduler + Postgres + one-shot Alembic migrate.
- Account onboarding runbook (`docs/ONBOARDING_A_NEW_ACCOUNT.md`): network
  (public/private subnets + NAT), S3 bucket + lifecycle rules, GPU quota
  request, both CF stacks, cost map, and full teardown.
- Test tiers: `tests/slow` (real worker) and `tests/e2e` (real AWS EC2,
  opt-in via `MINERU_GATEWAY_E2E=1`, costs money), shared harness in
  `tests/helpers/`, sample PDF fixtures.
- SQLite schema bootstrap (`db/bootstrap.py`) so empty dev DB files work
  without Alembic.
- Stalled-worker convergence: bounded drain, force-termination, automatic
  replacement capacity (the v1 ship gate).
- This changelog.

### Changed
- `config.example.yaml` models a production-safe deployment: auth enabled with
  placeholder key, 100 MiB soft upload cap, `cloud_workers_enabled` documented.
- `WorkerRepository` refactored onto shared helpers; idempotent terminate;
  readable EC2 `Name` tags; edge-triggered dispatch logging.
- README: sandbox runbook (SSM bring-up/operate/teardown), test-tier table,
  corrected cloud-worker registration story (workers never self-register).

### Fixed
- Integration tier flakiness: dedup test now waits for the cache CAS to land
  (populate runs after task completion commits), and the wired-path fixture
  uses a file-backed sqlite so concurrent app + scheduler sessions no longer
  share one StaticPool connection.
- Ruff/pyright drift in the salvaged test code.

### Removed
- Stray experimental `Dockerfile.1` (unreferenced copy of upstream MinerU's
  GPU image; the worker CF builds from upstream at boot instead).
- **Public worker mode, entirely.** Workers are addressed by private IP only:
  `cloud.aws.worker_address` config, `ComputeProvider.get_public_ip()`, and
  the public-worker CloudFormation template are gone. The scheduler must run
  in the workers' VPC (the sandbox host does). Breaking for configs still
  carrying `worker_address` (`extra="forbid"` fails fast at startup).

## [Unreleased → 0.1.0 base]

The four commits below `b354b77` (July work: durable autoscaling gateway,
multi-cloud provider layer, S3 hardening, README) were developed against
`main` and folded into this release.
