# Known Internal-Gateway Limitations

These limitations are explicitly accepted for the internal `mineru-gateway`. They are not release blockers.

## Cache concurrency

Two requests can occasionally interact with the same expired cache entry simultaneously.

Possible effects:

- an avoidable cache miss;
- repeated processing;
- a request retry;
- an orphaned cache object.

The source task and durable task result remain authoritative. Cache data is never the source of truth.

No distributed locks, cache workflow tables, or elaborate generation protocol are required for this release.

## Cache invalidation race

An administrator can invalidate a cache entry while a task is populating it. The result may be a cache miss or later repopulation.

Administrative invalidation is rare, and repeating computation is preferable to adding coordination machinery.

## Cloud object deletion failure

Cloud object deletion can fail transiently after its database pointer is removed. This can leave an orphaned object.

The object-storage bucket lifecycle policy cleans these objects later. A durable object-deletion queue is intentionally out of scope.

## Multipart temporary spooling

Starlette parses multipart forms before the gateway performs its chunked size check. An oversized request may consume temporary disk space before the application returns HTTP 413.

This is accepted because the service is internal and callers are trusted. Deployments exposed to less-trusted networks should enforce an equivalent or smaller body-size limit at the reverse proxy or ingress.

## OCR malformed-filename cleanup

A malformed OCR filename can fail after a temporary directory is created. A small temporary-directory leak on this rejected request path is accepted for this release.

It may later be fixed with a local `try`/cleanup block.

## At-least-once dispatch crash window

If the scheduler crashes after MinerU accepts a task but before the upstream task ID is committed, recovery may submit the task again.

This remains accepted until the upstream MinerU API supports an idempotency key. The gateway returns only the result associated with the upstream task ID stored in PostgreSQL.

## Synchronous request timeout

Synchronous endpoints can stop waiting after their polling period and return HTTP 202 with the task ID. The task continues through the central queue, and callers can use the status and result endpoints.

Scheduler-managed client SLA behavior is not required. Existing SLA code may be removed in a later focused simplification, but it is not a release blocker when it does not interrupt execution.

## MinerU compatibility wrapper

`mineru_compat.py` remains as the single location for imports tied to the supported MinerU version. It is intentionally retained and is not considered unnecessary architecture.

## Deferred code cleanup

Large repository modules and duplicate test-facing cache helpers are not release blockers. Do not perform another broad refactor before shipping.

## Operational safeguards

Use the following operational safeguards instead of adding workflow machinery:

- application upload-size validation;
- optional proxy or ingress request-body limits;
- API-key authentication and private network access;
- object-storage lifecycle expiration;
- logging and caller retries.
