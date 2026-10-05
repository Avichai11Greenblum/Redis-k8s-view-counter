# View Counter / Analytics Service

A small analytics service built to demonstrate three technologies working together for distinct, non-redundant reasons: **Redis** as a write buffer (not a cache), **PostgreSQL** as the durable source of truth, and **Kubernetes** running the whole system as a set of independently scaled, self-healing components.

## Tech Stack

| | |
|---|---|
| **API** | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/) (async) |
| **Write buffer** | [Redis](https://redis.io/) 7 — `INCR`, AOF persistence |
| **Durable storage** | [PostgreSQL](https://www.postgresql.org/) 17 — `psycopg` 3 (async, connection-pooled) |
| **Orchestration** | [Kubernetes](https://kubernetes.io/) (minikube) — StatefulSets, Deployments, Services, ConfigMaps, Secrets, liveness/readiness probes |
| **Containerization** | Docker (multi-stage build) |
| **Tooling** | [uv](https://docs.astral.sh/uv/) (dependency/venv management) |
| **Testing** | pytest + pytest-asyncio (integration tests against real Redis/Postgres) |
| **CI** | GitHub Actions (tests run on every push, with service containers) |

## What It Does

Clients record a "view" for any named resource (`POST /views`) and read back per-minute time-series counts (`GET /views/{resource_id}`). The interesting part is what happens in between.

## Architecture

```
                 ┌──────────────┐
  POST /views ──▶│   FastAPI    │── INCR ──▶  Redis (write buffer)
                 │   (async)    │                   │
                 └──────────────┘                   │ RENAME (atomic)
                                                     ▼
                                         ┌─────────────────────┐
                                         │    Drain Worker      │
                                         │  (every N seconds)   │
                                         └──────────┬────────────┘
                                                     │ idempotent upsert
                                                     ▼
                                         ┌─────────────────────┐
  GET /views/{id} ◀── FastAPI ◀──────────│     PostgreSQL        │
                                         │  (source of truth)   │
                                         └─────────────────────┘
```

**Write path:** every view hits Redis first via an atomic `INCR` on a per-resource, per-minute counter key (`views:{resource_id}:{YYYYMMDDHHMM}`). The API acknowledges the request only once Redis has confirmed the write — durability from that point on comes from Redis's append-only file (`appendfsync everysec`).

**Drain:** a separate worker process periodically moves counts from Redis into Postgres in batches, using an atomic `RENAME` to freeze a counter before reading it — this closes the race where a concurrent `INCR` could otherwise be silently dropped. Each batch is applied to Postgres exactly once via an idempotency table, so a worker crash mid-batch and a subsequent restart can safely retry without double-counting.

**Read path:** reads go straight to Postgres, returning a per-minute time series for the requested window — a view counter that can show you trends forming, not just a single running total.

### Why a write buffer, not a cache

Writing every view straight to Postgres means every request takes a row-level lock on the same `(resource_id, bucket)` row — under real traffic on a popular resource, that serializes writes and generates heavy table bloat from constant row versioning. Routing writes through Redis first turns thousands of increments into a single aggregated upsert per drain cycle, regardless of how hot the resource is.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/views` | Record one view (buffered through Redis) |
| `GET` | `/views/{resource_id}?hours=24` | Per-minute counts for a resource over a time window |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe (checks Redis connectivity) |

## Running Locally

```bash
uv sync
docker compose up -d                 # Redis + Postgres
uv run uvicorn redis_k8s_view_counter.app:app --reload
uv run view-counter-worker           # in a separate terminal
```

## Testing

Integration tests run against real Redis and Postgres instances (not mocks), covering crash-recovery and idempotency guarantees directly:

```bash
docker compose up -d
uv run pytest tests/ -v
```

## Running on Kubernetes

```bash
minikube start --driver=docker
docker build -t view-counter:dev .
minikube image load view-counter:dev
kubectl apply -f k8s/
kubectl get pods -w
```

Redis and Postgres run as `StatefulSet`s with persistent volumes, so data survives pod restarts. The API and drain worker are independent `Deployment`s sharing one image, differentiated only by their startup command — reflecting that they're separate, independently scalable concerns.

## Project Structure

```
src/redis_k8s_view_counter/
  app.py            FastAPI app: routes, health checks, lifecycle
  redis_store.py     Redis key layout and write-path logic
  db.py              Postgres schema, idempotent batch application, reads
  drain_worker.py    Redis → Postgres drain loop and crash recovery
  schemas.py          Request/response models

k8s/                 Kubernetes manifests
tests/                Integration test suite
scripts/              Benchmarking and failure-injection tooling
```
