# mineru-gateway

Internal OCR gateway and VM controller wrapping MinerU with a Mistral-compatible `/v1/ocr` facade.

## Deployment

### Internal service

- Deploy on a private network; clients are trusted internal services.
- Protect HTTP APIs with a shared API key (`auth.enabled: true`, `auth.api_key`).
- `/health` and `/ready` are unauthenticated probes.

### Database

- **Production:** PostgreSQL (`postgresql+asyncpg://...`). Install the postgres extra: `uv sync --extra postgres`.
- **Development/tests:** SQLite (`sqlite+aiosqlite:///./gateway.db`).

### Scheduler

Run exactly **one** scheduler replica in production:

- **Kubernetes:** `replicas: 1`, deployment strategy `Recreate`.
- **ECS:** desired count `1`, avoid overlapping scheduler tasks during deploys.

The scheduler holds a PostgreSQL session advisory lock to prevent accidental overlap. SQLite assumes a single local scheduler process.

### Processes

```bash
# API gateway (stateless)
mineru-gateway --config config.yaml

# Scheduler (queue dispatch, autoscaling, worker reconciliation)
mineru-scheduler --config config.yaml
```

### Object storage

Object storage is mandatory. Configure the active cloud provider bucket/container in `config.yaml`. Buckets are pre-provisioned; the application does not create them.

Configure a bucket lifecycle policy (or equivalent) to expire orphaned objects under `payloads/`, `results/`, and `cache/` as a safety net. The scheduler deletes objects when tasks and cache entries age out, but lifecycle rules catch leaks from crashes or partial failures.

Set `max_file_size_bytes` in config and enforce the same or smaller HTTP request-body limit at your reverse proxy or ingress. Starlette parses multipart forms before the gateway copy loop runs.

See [`config.example.yaml`](config.example.yaml) for configuration. Run `task checks-full` before shipping.
