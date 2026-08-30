# syntax=docker/dockerfile:1.7
# Production image for mineru-gateway.
#
# The gateway process does NOT run ML models — it dispatches to worker nodes.
# So the base `mineru` install (no torch/vllm extras) is enough here.
# Worker images are separate (they run `python -m mineru.cli.fast_api`).

FROM python:3.13-slim AS builder

# uv: copy from the official slim image to get a pinned version cleanly.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install deps first (better layer caching). --frozen fails if uv.lock is stale.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=README.md,target=README.md \
    --mount=type=bind,source=LICENSE,target=LICENSE \
    uv sync --frozen --no-install-project --no-dev --extra postgres

# Now copy the source and install the project itself. The bind mounts from the
# layer above do not persist, so pyproject/lock must be mounted again here.
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=README.md,target=README.md \
    --mount=type=bind,source=LICENSE,target=LICENSE \
    uv sync --frozen --no-dev --extra postgres

# ----------------------------------------------------------------- runtime ---
FROM python:3.13-slim AS runtime

# cv2 (via mineru's import chain) needs X11/GL libs absent from slim images.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 libxcb1 \
    && rm -rf /var/lib/apt/lists/*

# Drop privileges.
RUN groupadd --system --gid 1001 gateway \
    && useradd --system --uid 1001 --gid gateway --create-home gateway

WORKDIR /app

COPY --from=builder --chown=gateway:gateway /app/.venv /app/.venv
COPY --chown=gateway:gateway src ./src
COPY --chown=gateway:gateway alembic.ini ./alembic.ini
COPY --chown=gateway:gateway alembic ./alembic

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER gateway

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)" || exit 1

ENTRYPOINT ["mineru-gateway"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
