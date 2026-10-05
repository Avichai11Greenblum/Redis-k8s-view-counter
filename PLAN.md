# Redis + K8s View Counter — Project Plan

## Context
Learning/portfolio project to practice Postgres, Redis and beginner Kubernetes (minikube) together, aimed at backend/system-design interviews. Domain: a view counter / analytics service where each technology has a distinct job. Working style: Socratic, small steps with check-ins; Claude writes files, the user runs commands.

## Locked-in decisions
- **Dominant constraint (user-redefined):** an ACKed view may be lost only within ~1s of a Redis crash, never otherwise. ACK (HTTP 202) is sent only after Redis confirms the write.
- **View model:** event = `(resource_id, timestamp)`; counted per resource per UTC **minute** bucket (changed from hour — fine granularity lets trends show up live while draining, and aggregated per-bucket rows already form a timeline on their own, so no raw event table is needed). Unique/per-user views out of scope (possible later HyperLogLog extension).
- **Redis:** write buffer (not a cache). Key `views:{resource_id}:{YYYYMMDDHHMM}` via `INCR`. AOF on, `appendfsync everysec`, data on a persistent volume.
- **Buffer's value:** aggregation. N views on a hot key become one Postgres write per drain (avoids hot-row lock contention and MVCC bloat). A bucket can be drained multiple times while still open (RENAME takes whatever accumulated so far); Postgres accumulates via `count = count + N`, so counts stay correct and visibly climb while a minute is still in progress.
- **Postgres:** durable source of truth. `view_counts(resource_id TEXT, bucket_start TIMESTAMPTZ, count BIGINT)` PK `(resource_id, bucket_start)`; `applied_batches(batch_id PK)`. `TIMESTAMPTZ` (not a formatted string) so range queries like "last 24h" are native. `TEXT` for `resource_id` (Postgres treats `TEXT`/`VARCHAR(n)` identically; no DB-enforced length cap for now).
- **Drain (Redis to Postgres):** atomic `RENAME` to `draining:<batch_id>`, then one Postgres transaction that inserts `batch_id` into `applied_batches` and upserts `count = count + N`, then `DEL` the renamed key. Startup recovery drains leftover `draining:*` keys. Avoids lost increments (GET/DEL race) and double counting (crash between commit and DEL).
- **Drain trigger (proposed, open to challenge):** time-based, every 5–10s, configurable via env/ConfigMap.
- **API:** FastAPI. `POST /views` (write), `GET /views/{resource_id}` (read, Postgres only at first, so it lags by up to one drain interval), health endpoints for probes.
- **K8s scope:** minikube (Docker driver), pods/deployments/services/configmaps/probes. Out of scope: multi-node, cloud, autoscaling.

## Stages (each ends with a check-in; no jumping ahead)
1. **Write path — done.** `docker-compose.yml` (Redis with AOF everysec + volume, Postgres) and the app, split into `app.py` (FastAPI wiring: `lifespan`, `POST /views`), `redis_store.py` (`get_redis_client`, `get_bucket_key`, `increment_view`), `schemas.py` (`EventView`). Verified: writes increment the right key, and a hard kill (`docker compose kill redis`) followed by restart loses nothing, confirming AOF `everysec` durability.
2. **Postgres + read endpoint.** Add `psycopg` (async). Create tables on startup (`CREATE TABLE IF NOT EXISTS`). Add `GET /views/{resource_id}?hours=24` reading from Postgres. Decide together: bucket-range query shape.
3. **Drain worker — done.** Separate entry point (`drain_worker.py`, same image, later its own Deployment) running the RENAME, apply, DEL loop plus startup recovery. `BucketKey`/`DrainingKey` value objects in `redis_store.py` own key parsing/building. Verified end to end: `POST /views` → drained into Postgres → `GET /views/{resource_id}` returns the real count. Hardened along the way: `ResponseError` on RENAME ("no such key") is treated as losing a race to another worker, not a crash (relevant once K8s runs multiple replicas); keys whose bucket segment doesn't strictly match the expected length raise and get deleted instead of being silently misparsed (bit us once with leftover keys from the old hour-bucket format).
4. **Failure testing — partly done.** `tests/test_drain_worker.py` (pytest, against the real docker-compose Redis+Postgres): reproduces "crashed after Postgres commit, before deleting the Redis key" by stopping short of the DEL, then asserts `recover_pending` finishes it without double-counting — plus regression tests for malformed/unparseable keys in both `drain_key` and `recover_pending` (the latter was a real gap found and fixed while writing these tests: one bad key used to abort recovery for every other pending batch). Still open: kill Redis itself (hard `docker compose kill redis`) under real traffic and measure the actual loss window under `everysec` — left as a manual/documented step, not automated (inherently about real OS-level timing, not deterministic to script).
5. **Benchmark.** Throwaway `/views/direct` (direct Postgres `UPDATE`) vs buffered `/views`, hot-key load (script or `hey`). Compare write latency and read freshness before/after a drain.
6. **Containerize — done.** Two-stage `Dockerfile` (uv, Python 3.14-slim) and `.dockerignore`. One image serves both roles: default CMD runs the API, the worker overrides the command with `view-counter-worker`. Verified: built and ran against the docker-compose Redis/Postgres via the shared Docker network, confirmed a real write round-trip.
7. **Minikube.** `minikube start --driver=docker`, load the image. Manifests: ConfigMap (Redis URL, drain interval, non-secret settings), Secret for the Postgres password, Deployments for api and worker, Redis and Postgres as StatefulSets with PVCs (Redis PVC is required so the AOF survives pod recreation), Services. Probes: liveness `/healthz`, readiness `/readyz` (checks Redis ping). Exercises: delete pods and watch recovery, confirm the counter survives a Redis pod restart.
8. **Wrap-up.** README with architecture diagram, decision log and tradeoffs, and interview talking points (write buffer vs cache, at-least-once + idempotency, durability vs latency, hot-key contention).

## Open questions to revisit in-stage
- Drain interval value, and drain trigger (time vs hybrid) if the user wants to challenge it.
- Read semantics: Postgres only vs Postgres + pending Redis counts (freshness tradeoff).
- Whether the worker is a separate process (recommended) or a background task in the API.

## Critical files
- `docker-compose.yml`, `src/redis_k8s_view_counter/{app.py, redis_store.py, schemas.py, db.py, drain_worker.py}`, `tests/`, `.github/workflows/tests.yml` (exist)
- New: `Dockerfile`, `k8s/*.yaml`, `README.md`

## CI
`.github/workflows/tests.yml` runs `tests/` on every push/PR, with Redis and Postgres as GitHub Actions service containers (mirrors docker-compose). Uses `uv sync --locked --all-groups` so CI installs exactly what `uv.lock` pins.

## Verification (end to end)
- Local: POST views, drain, and `GET /views/...` returns the right total after a drain.
- Idempotency: kill worker between Postgres commit and DEL, restart, and the total is unchanged.
- Durability: `docker compose restart redis` and the counter survives (AOF).
- K8s: `kubectl get pods` all Ready, delete each pod and it recovers, `minikube service` reaches the API, counts survive a Redis pod restart.
