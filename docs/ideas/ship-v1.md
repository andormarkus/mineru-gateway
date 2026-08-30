# Ship v1 of mineru-gateway

One-pager from the 2026-08-30 ideation session that scoped the first release.

## Problem Statement

How might we salvage six weeks of stalled-but-90%-complete work and turn
mineru-gateway into a deployed, tagged v1 the internal team can call through
LiteLLM — closed to the internet, operated entirely via SSM?

## Recommended Direction

Salvage + harden + ship the exact topology the e2e suite validated in July,
hardened to private workers and an SSM-operated host. The feature work
(scheduler loop split, autoscaler hysteresis, provisioning observability,
real-worker test tiers, worker CloudFormation) was done and executed green —
the only thing missing was pressing "commit." Everything after that was
process: defuse the landmines (private-template status responder, e2e spend
guard, safe example config), wrap deployment in a repeatable
launch/iterate/teardown story (gateway-host stack + sandbox compose), document,
deploy, tag.

Rejected alternatives:

- **Public workers with CIDR allow-lists** — the "open workers" posture; the
  whole point of v1 was a closed topology (private subnets, SSM-only host,
  zero inbound ports).
- **Autoscaling off, one static warm worker** — benches the best-engineered
  part of the project; `min=0, max=2` gives identical idle cost with the
  machinery live.
- **CI/CD pipeline** — declined; local Taskfile gates (`task checks`) are the
  quality bar for v1.
- **Alembic 0002 back-compat migration** — greenfield; the edited `0001`
  *is* the v1 schema and DBs are disposable.

## Key Assumptions to Validate

- [x] The uncommitted suite still passes after six weeks of drift — verified
      (`task checks`, integration tier 12/12 clean after two harness fixes).
- [ ] Sandbox launch template / bucket / VPC still exist and credentials can
      be refreshed — checked at deploy time (clisso profile had expired).
- [ ] Private-worker path works end-to-end including the `:8001` responder —
      exercised by the cold-boot acceptance run (July's e2e used public).
- [x] Scale-from-zero usable → async-first usage — documented (cold boot
      outlasts the 300s sync SLA; `/tasks` is primary).
- [x] ~$0.04/hr host + bursty GPU cost acceptable — chosen (min=0, max=2).

## MVP Scope

Salvage commit; baseline repairs; `:8001` responder in the private template;
`MINERU_GATEWAY_E2E=1` spend guard; doc drift fixes; production-safe example
config; `gateway-host.yaml` + sandbox compose + config templates; README
runbook; CHANGELOG; sandbox deploy over SSM; cold-boot private-worker run;
LiteLLM round-trip acceptance; idle-drain to zero; merge + tag `v0.1.0` +
push.

## Not Doing (and Why)

- **CI/CD pipeline** — your call; local `task checks` gates v1.
- **Alembic back-compat (`0002`)** — greenfield; single `0001` is the v1
  schema; deployed DBs are disposable until a second environment exists.
- **ALB / public gateway ingress** — SSM port-forwarding for v1; ALB+ACM is
  the day-2 step when the team needs stable ingress without tunnels.
- **Multi-cloud (GCP/Azure)** — AWS-only validated; the ABCs stay as
  scaffolding, unused.
- **OCR image bytes** — `/v1/ocr` image payloads remain placeholders; no
  consumer today.
- **Rate limiting / multi-key auth** — internal single-key + startup bind
  guard suffices.
- **Worker image pinning / golden AMI** — accept long cold boots; revisit
  when boot latency actually hurts.
- **Prometheus `/metrics`** — OTel stays optional; add when there is a
  scraper to feed.
- **k8s/ECS** — one compose host is enough for the sandbox.
- **Stripping dead scaffolding** (`MISTRAL_JSON`, cloud ABCs) — zero runtime
  cost; cleanup can ride any later refactor.

## Open Questions

- None blocking. Sandbox liveness resolved at deploy time.
