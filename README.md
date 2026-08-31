# mineru-gateway

A production-oriented gateway for [MinerU](https://github.com/opendatalab/MinerU) document parsing. It provides a **Mistral-compatible `/v1/ocr` API**, durable task queuing, content-addressed caching, and **AWS EC2 autoscaling** for worker VMs — so you can run MinerU as a managed service behind LiteLLM, internal tools, or any HTTP client.

Built on top of MinerU by Opendatalab. See [Attribution](#attribution).

## Features

### API surface

- **`POST /v1/ocr`** — Mistral-compatible OCR facade. Accepts `document_url`, `image_url`, or base64 `file`, returns inline `{model, pages: [...]}` JSON. Drop-in target for [LiteLLM](https://docs.litellm.ai/docs/ocr) OCR routing.
- **`POST /tasks`** — Async task submission (HTTP 202). Poll status and fetch results separately.
- **`GET /tasks/{id}`** / **`GET /tasks/{id}/result`** — Task lifecycle and ZIP result retrieval.
- **`POST /file_parse`** — Synchronous MinerU-style parse; blocks until the result ZIP is ready.
- **`/admin/*`** — Worker registry, drain/rotate/recover controls, and cache invalidation.
- **`/health`** / **`/ready`** — Liveness and readiness probes (DB + object storage).

### Reliability & durability

- **PostgreSQL-backed task queue** — Gateway ingests; a separate scheduler dispatches. Single dispatch path for all routes.
- **S3-compatible object storage** — Payloads, results, and cache objects are stored durably (AWS S3 or any S3-compatible endpoint such as SeaweedFS for dev).
- **Content-addressed dedup cache** — SHA-256 keyed cache with configurable TTL. Bypass via `Cache-Control: no-cache` or `force=true`.
- **Retention sweeper** — Scheduled cleanup of expired tasks, results, and cache entries.
- **Stale-dispatch recovery** — Scheduler reclaims stuck dispatch claims and requeues work.

### Autoscaling & worker lifecycle

- **Queue-depth autoscaling** — Target-tracking on tasks per serviceable worker (`scaling.target_per_worker`).
- **EC2 launch-template workers** — Provision, reconcile, and terminate VMs tagged per deployment.
- **Health monitoring** — Periodic worker health checks; dispatch skips unhealthy nodes.
- **Drain & rotate** — Graceful drain to stop or terminate, scheduled weekly rotation, and admin-triggered emergency rotation.
- **Stalled-worker convergence** — Workers that exceed `max_failure_count` enter a bounded drain-to-terminate flow.

### Security & operations

- **API key authentication** — `X-API-Key` or `Authorization: Bearer` (configurable; required for public bind).
- **Startup guard** — Refuses to bind a public host without auth enabled.
- **Upload admission control** — Configurable soft size limit (1 GiB hard cap). Enforce the same limit at your reverse proxy.
- **SSRF protections** — Server-side URL fetching is restricted when the gateway is exposed on public interfaces.
- **OpenTelemetry** — Optional OTLP traces and business metrics (tasks ingested, dispatch latency, scale events, cache sweeps, etc.).
- **Structured request logging** — Correlation-friendly middleware on every request.

### Developer experience

- **FastAPI + async SQLAlchemy** — Python 3.13, type-checked with pyright, linted with ruff.
- **Alembic migrations** — Schema versioning for PostgreSQL production and SQLite dev.
- **Docker image** — Slim production image; gateway process does not bundle ML weights (workers are separate).
- **Local dev stack** — Docker Compose for Postgres + SeaweedFS S3; `task` runner for install, test, and migrate.

## Architecture

```
                    ┌─────────────────────────────────────────┐
  Clients / LiteLLM │           mineru-gateway (stateless)    │
  ─────────────────►│  /v1/ocr  /tasks  /file_parse  /admin   │
                    └───────────┬─────────────────────────────┘
                                │ ingest (payload → S3, row → DB)
                                ▼
                    ┌───────────────────────┐     ┌──────────────┐
                    │   PostgreSQL / SQLite  │     │  S3 bucket   │
                    │   tasks, workers, cache│◄───►│  payloads/   │
                    └───────────┬───────────┘     │  results/    │
                                │                 │  cache/      │
                                │ dispatch        └──────────────┘
                                ▼
                    ┌─────────────────────────────────────────┐
                    │     mineru-scheduler (single replica)   │
                    │  queue dispatch · autoscale · reconcile │
                    │  health · rotation · retention · cache  │
                    └───────────┬─────────────────────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
        ┌──────────┐     ┌──────────┐     ┌──────────┐
        │ Worker VM│     │ Worker VM│     │ Worker VM│
        │ mineru-api│     │ mineru-api│     │ mineru-api│
        └──────────┘     └──────────┘     └──────────┘
```

The gateway is a thin HTTP facade: it never talks to workers directly. The scheduler is the sole dispatcher, which keeps behavior consistent across sync and async endpoints.

## Quick start (local)

**Prerequisites:** [uv](https://docs.astral.sh/uv/), Python 3.13, Docker (for local Postgres + S3).

```bash
# Install dependencies
uv sync --all-extras

# Start Postgres and SeaweedFS (S3 on :8333)
task up

# Copy and edit config
cp config.example.yaml config.yaml

# Run migrations
task migrate

# Terminal 1 — API gateway
mineru-gateway --config config.yaml

# Terminal 2 — scheduler
mineru-scheduler --config config.yaml
```

The gateway listens on `http://127.0.0.1:8000`. OpenAPI docs are at `/docs`.

### Example: OCR request

```bash
curl -s http://127.0.0.1:8000/v1/ocr \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "mineru",
    "document": {
      "type": "document_url",
      "document_url": "https://example.com/sample.pdf"
    }
  }' | jq '.pages | length'
```

With auth enabled, add `-H 'X-API-Key: your-key'`.

### Example: async task

```bash
# Submit
curl -s -X POST http://127.0.0.1:8000/tasks \
  -F 'file=@document.pdf' \
  -F 'backend=hybrid-engine'

# Poll status
curl -s http://127.0.0.1:8000/tasks/<task_id>

# Download result ZIP
curl -sO http://127.0.0.1:8000/tasks/<task_id>/result
```

## LiteLLM integration

Point LiteLLM's OCR route at the gateway:

```yaml
# litellm config excerpt
ocr:
  - model_name: mineru
    litellm_params:
      model: ocr/mineru
      api_base: http://<gateway-host>:8000
      api_key: <your-api-key>   # when auth.enabled: true
```

The gateway normalizes MinerU ZIP output into Mistral's `{pages: [{index, markdown, images, ...}]}` shape.

## Configuration

Copy [`config.example.yaml`](config.example.yaml) to `config.yaml` (gitignored — holds secrets).

**Precedence** (low → high): defaults → `config.yaml` → environment (`MINERU_GATEWAY_*` with `__` nesting) → CLI flags.

| Area | Key settings |
|------|----------------|
| Database | `database_url` — SQLite for dev; `postgresql+asyncpg://...` for production |
| Auth | `auth.enabled`, `auth.api_key` |
| Object storage | `cloud.aws.bucket`, `cloud.aws.region`, optional `endpoint_url` for S3-compatible stores |
| Autoscaling | `scaling.min_workers`, `scaling.max_workers`, `scaling.target_per_worker` |
| Cloud workers | `cloud_workers_enabled: true`, `cloud.aws.launch_template_id` |
| Cache | `cache.enabled`, `cache.ttl_seconds` |
| Retention | `retention.retention_days` |
| Observability | `otel.enabled`, `otel.endpoint` (install `[otel]` extra) |

Environment variable example:

```bash
export MINERU_GATEWAY_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/gateway
export MINERU_GATEWAY_AUTH__ENABLED=true
export MINERU_GATEWAY_AUTH__API_KEY=secret
export MINERU_GATEWAY_SCALING__MAX_WORKERS=12
```

Config is read once at startup; changes require a process restart.

## Deployment

### Processes

Run two processes (or containers) from the same image:

```bash
# API gateway — stateless, can scale horizontally
mineru-gateway --config config.yaml --host 0.0.0.0 --port 8000

# Scheduler — exactly one replica in production
mineru-scheduler --config config.yaml
```

### Database

| Environment | Driver | Install |
|-------------|--------|---------|
| Production | PostgreSQL (`postgresql+asyncpg://...`) | `uv sync --extra postgres` |
| Dev / tests | SQLite (`sqlite+aiosqlite:///./gateway.db`) | included |

Apply migrations before starting: `alembic upgrade head`.

### Scheduler singleton

Run **exactly one** scheduler replica:

- **Kubernetes:** `replicas: 1`, deployment strategy `Recreate`
- **ECS:** desired count `1`; avoid overlapping tasks during deploys

PostgreSQL uses a session advisory lock to prevent accidental overlap. SQLite assumes a single local scheduler process.

### Object storage

Object storage is mandatory. Buckets are pre-provisioned — the application does not create them.

Configure a bucket lifecycle policy to expire orphaned objects under `payloads/`, `results/`, and `cache/` as a safety net. The scheduler deletes objects when tasks and cache entries age out, but lifecycle rules catch leaks from crashes or partial failures.

Set `max_file_size_bytes` as a soft upload cap (`0` = 1 GiB hard maximum only). Also enforce the same or smaller request-body limit at your reverse proxy or ingress.

### Network & auth

- Deploy on a private network when possible; treat callers as trusted internal services.
- Enable `auth.enabled: true` and set `auth.api_key` for any deployment reachable beyond a loopback or private VPC.
- `/health` and `/ready` remain unauthenticated for probes.

### Docker

```bash
task build
docker run --rm -p 8000:8000 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  mineru-gateway:latest --config /app/config.yaml
```

Run the scheduler as a separate container with the same config volume.

### Cloud workers (AWS)

Enable EC2-backed autoscaling:

```yaml
cloud_workers_enabled: true
deployment_id: "prod-us-east-1"   # scopes VM tags and reconciliation

cloud:
  provider: aws
  aws:
    region: us-east-1
    bucket: mineru-results
    launch_template_id: lt-0abc123
    launch_template_version: "$Latest"
```

Workers are addressed **by private IP only** — there is no public-worker mode. The scheduler must run in the same VPC as the workers (the sandbox host is).

Workers are stock upstream `mineru-api` instances — nothing registers with the
gateway. The scheduler launches them via the launch template, discovers them by
deployment tags, health-checks `:8000`, polls `:8001` for bootstrap progress
while they build, dispatches tasks, and terminates them when idle.

Provision the worker infrastructure with the CloudFormation stacks in
`deploy/cloudformation/`:

| Template | Purpose |
|----------|---------|
| `mineru-worker.yaml` | Worker launch template, IAM role, and SG: workers in a **private subnet**, no public IPs, `:8000`/`:8001` reachable only from the gateway-host SG. |
| `gateway-host.yaml` | The host that runs gateway + scheduler + Postgres (see below). |

### Sandbox deployment (SSM, closed topology)

The v1 sandbox topology: everything private, zero inbound ports, operated via
SSM Session Manager. The whole stack is repeatable — launch, deploy, iterate,
remove. **Starting from an empty AWS account?** Do
[`docs/ONBOARDING_A_NEW_ACCOUNT.md`](docs/ONBOARDING_A_NEW_ACCOUNT.md) first —
network (public + private subnets with NAT), the S3 bucket + lifecycle rules,
and the GPU quota request that gates everything.

```
 ┌──────────────────────────────── VPC ────────────────────────────────┐
 │ public subnet (NAT only)           private subnets                 │
 │ ┌───────────────┐                  ┌────────────────┐ ┌─────────┐ │
 │ │ NAT gateway   │◀──── egress ──────│ gateway host   │ │ GPU     │ │
 │ └───────────────┘                  │ gateway+worker │─▶ workers │ │
 │                no public IPs:      │ scheduler, pg  │ │ g5/g6,  │ │
 │                everything below    │ (t4g, compose) │ │ 0..2    │ │
 │                runs in private     └───────┬────────┘ └────┬────┘ │
 │                subnets                     │ 8000/8001 only│ S3    │
 └───────────────┼────────────────────────────┼───────────────┼──────┘
                 │ SSM agent outbound 443     │               │
            operator laptop            results bucket ◀───────┘
```

**How do you reach a host with no public IP?** The SSM agent on the instance
opens an *outbound* TLS connection to the AWS SSM service; `aws ssm
start-session` talks to that same service, which relays your session through
it. Nothing ever connects inbound to the host — that is why its security
group has no inbound rules and why it can sit in a private subnet.

**1 — Host stack** (the SSM-operated dev machine; ~$0.04/hr t4g.medium).
Create this first — the worker stack grants its SG ingress and the
`iam:PassRole` permission to this host's role:

```bash
aws cloudformation create-stack --stack-name mineru-gateway-host \
  --template-body file://deploy/cloudformation/gateway-host.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=vpc-... \
    ParameterKey=PrivateSubnetId,ParameterValue=subnet-... \
    ParameterKey=ResultsBucket,ParameterValue=andor-sandbox-... \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
# Note the SecurityGroupId and RoleName outputs.
```

**2 — Worker stack** (private subnets only; takes the host SG and host role
from step 1):

```bash
aws cloudformation create-stack --stack-name mineru-worker-sandbox \
  --template-body file://deploy/cloudformation/mineru-worker.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=vpc-... \
    ParameterKey=PrivateSubnetId,ParameterValue=subnet-... \
    ParameterKey=GatewaySecurityGroupId,ParameterValue=<host SecurityGroupId> \
    ParameterKey=HostRoleName,ParameterValue=<host RoleName> \
    ParameterKey=ResultsBucket,ParameterValue=andor-sandbox-... \
    ParameterKey=EnvironmentName,ParameterValue=sandbox \
  --capabilities CAPABILITY_NAMED_IAM
# Note the LaunchTemplateId output — it goes into the gateway config.
```

**3 — Deploy the stack over SSM** (no SSH, no keys, no open ports):

```bash
HOST=$(aws cloudformation describe-stacks --stack-name mineru-gateway-host \
  --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue' --output text)

aws ssm send-command --instance-ids "$HOST" \
  --document-name AWS-RunShellScript --comment "deploy mineru-gateway" \
  --commands 'git clone https://github.com/andormarkus/mineru-gateway.git ~/mineru-gateway &&
              cd ~/mineru-gateway && git checkout v0.1.0 &&
              cd deploy/compose &&
              cp .env.example .env && sed -i "s/change-me/$(openssl rand -hex 24)/" .env &&
              cp config.sandbox.yaml.example config.yaml &&
              docker compose -f docker-compose.sandbox.yml up -d --build'
# then edit ~/mineru-gateway/deploy/compose/config.yaml on the host:
#   api_key, bucket, launch_template_id, and the SAME postgres password as .env
```

**4 — Operate:**

```bash
aws ssm start-session --target "$HOST"    # interactive shell
aws ssm start-session --target "$HOST" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8000"],"localPortNumber":["8000"]}'
curl http://127.0.0.1:8000/health          # gateway through the tunnel
```

**5 — Remove the dev machine** (GPU workers drain to zero on their own via
`idle_cooldown_seconds`; the host is the only fixed cost):

```bash
aws cloudformation delete-stack --stack-name mineru-gateway-host
```

**Scale-from-zero contract.** With `min_workers: 0` the fleet costs nothing
idle, but the first request pays the worker cold boot — 10+ minutes on first
launch (the user-data builds the MinerU image; later boots are faster). That
outlasts the 300s sync-poll SLA: `/v1/ocr` and `/file_parse` return
`202 + task_id` while execution continues, and callers should prefer the async
`/tasks` flow. Cached documents return instantly regardless of fleet state.

## API reference (summary)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Service info and upstream attribution |
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness (DB + object store) |
| `POST` | `/v1/ocr` | Sync Mistral-compatible OCR |
| `POST` | `/tasks` | Async task enqueue (202) |
| `GET` | `/tasks/{id}` | Task status |
| `GET` | `/tasks/{id}/result` | Result ZIP (202 while pending) |
| `POST` | `/file_parse` | Sync MinerU parse |
| `GET` | `/admin/workers` | List workers |
| `POST` | `/admin/workers/{id}/drain` | Drain (optionally terminate) |
| `POST` | `/admin/workers/{id}/rotate` | Emergency rotation |
| `POST` | `/admin/workers/{id}/recover` | Clear failure state |
| `DELETE` | `/admin/cache/{key}` | Invalidate cache entry |
| `POST` | `/admin/cache/sweep` | Sweep expired cache rows |

Full request/response schemas: `/docs` (Swagger UI) or `/openapi.json`.

## Development

```bash
task install          # uv sync --all-extras
task lint             # ruff check + format
task typecheck        # pyright
task checks           # lint + typecheck + unit + postgres (the pre-commit combo)
task checks-full      # everything below except slow/e2e
```

Test tiers (all skip cleanly when their prerequisites are missing):

| Command | Tier | Requires |
|---------|------|----------|
| `task test` | Unit — FakeWorker, in-memory store | nothing |
| `task test-integration` | Wired paths against moto S3 | nothing (no Docker) |
| `task test-postgres` | Migrations + advisory lock | Docker (testcontainers) |
| `task test-slow` | Real gateway ↔ real `mineru-api` worker | `task up` + `MINERU_TEST_WORKER_URL` |
| `task test-e2e` | Real AWS EC2 — **costs money** | `MINERU_GATEWAY_E2E=1` + launch template + bucket env |

Hot-reload dev server:

```bash
task run   # uvicorn with --reload
```

## Observability

Install the OpenTelemetry extra and enable in config:

```bash
uv sync --extra otel
```

```yaml
otel:
  enabled: true
  endpoint: http://otel-collector:4318
  service_name: mineru-gateway
  scheduler_service_name: mineru-scheduler
```

Exported metrics include task ingest/dispatch counts, admission rejections, worker scale events, cache sweeps, retention deletions, and latency histograms. FastAPI and httpx are auto-instrumented for traces.

## Attribution

MinerU is Apache 2.0 with [additional terms](https://github.com/opendatalab/MinerU/blob/master/LICENSE). Section 2 requires online services built on MinerU to clearly indicate that MinerU is used.

When `attribution.enabled: true` (default), every response includes:

- `X-Powered-By: MinerU`
- `MinerU-Version: <upstream version>`

`/health` and `GET /` also expose an `upstream` block with name, version, and homepage.

## Docs

- [`docs/ONBOARDING_A_NEW_ACCOUNT.md`](docs/ONBOARDING_A_NEW_ACCOUNT.md) — from an empty AWS account to a running stack (network, bucket, GPU quota, both CF stacks, teardown)
- [`docs/KNOWN_LIMITATIONS.md`](docs/KNOWN_LIMITATIONS.md) — accepted edge cases and operational safeguards
- [`docs/REQUIRED_CHANGES_BEFORE_SHIPPING.md`](docs/REQUIRED_CHANGES_BEFORE_SHIPPING.md) — the v0.1.0 ship gate, as shipped
- [`docs/ideas/ship-v1.md`](docs/ideas/ship-v1.md) — the scoping one-pager behind v0.1.0

## Known limitations

See [`docs/KNOWN_LIMITATIONS.md`](docs/KNOWN_LIMITATIONS.md) for accepted edge cases (cache concurrency races, at-least-once dispatch window, multipart spooling, etc.) and recommended operational safeguards.

## License

[Apache License 2.0](LICENSE)

This project builds on [MinerU](https://github.com/opendatalab/MinerU) by Opendatalab. MinerU has its own license with additional attribution requirements — see [Attribution](#attribution) above.
