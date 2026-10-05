# ---- builder: install dependencies into a venv ----
FROM python:3.14-slim AS builder

# The uv binary itself, copied straight from Astral's image rather than
# pip-installed — so it's available before anything Python-related is set up.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Dependency manifests first, nothing else. Docker caches each layer by its
# inputs — as long as these two files don't change, this (slow) step is
# skipped on every rebuild that only touches application code.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-dev

# Now the actual source, and install the project itself on top. README.md
# is needed too — pyproject.toml declares it as the package's readme, and
# the build backend refuses to build without the file it points to existing.
COPY src ./src
COPY README.md ./
RUN uv sync --locked --no-dev

# ---- final: just the venv + source, no uv, no build cache, no dev deps ----
FROM python:3.14-slim

WORKDIR /app
COPY --from=builder /app/.venv ./.venv
COPY --from=builder /app/src ./src

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# Default command runs the API. The drain worker (same image, no separate
# build) overrides this with its own command in its own K8s Deployment —
# see PLAN.md stage 7.
CMD ["uvicorn", "redis_k8s_view_counter.app:app", "--host", "0.0.0.0", "--port", "8000"]
