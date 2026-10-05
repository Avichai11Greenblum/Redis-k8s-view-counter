"""View counter API: write path (Redis) and read path (Postgres).

Each view is recorded as an INCR on a Redis counter keyed by resource and UTC
minute bucket. Redis is a write buffer: counts are drained to Postgres in
batches by a separate worker (stage 3), so this service never writes to
Postgres per view. Reads, for now, go straight to Postgres, so they lag
behind the write path by up to one drain interval.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, status

from .db import get_db_pool, get_view_buckets, increment_direct, init_tables
from .redis_store import get_redis_client, increment_view, truncate_to_bucket
from .schemas import EventView, ViewBucket, ViewsResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the Redis client and Postgres pool at startup, close at shutdown.

    Code before `yield` runs once when the app starts, code after it runs once
    when the app stops. Both clients live on `app.state` so every request
    reuses the same connections.
    """
    app.state.redis = get_redis_client()

    app.state.db_pool = get_db_pool()
    await app.state.db_pool.open()
    await init_tables(app.state.db_pool)

    yield

    await app.state.redis.aclose()
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)


@app.post("/views", status_code=status.HTTP_202_ACCEPTED)
async def record_view(event: EventView):
    """Record one view by incrementing its minute counter in Redis.

    Returns 202 only after Redis has answered the INCR, so an ACKed view is
    never one Redis hasn't seen. Durability after that point comes from Redis
    AOF (appendfsync everysec), which can lose up to ~1s on a crash.
    """
    key = await increment_view(app.state.redis, event.resource_id)
    return {"key": key}


@app.post("/views/direct", status_code=status.HTTP_202_ACCEPTED)
async def record_view_direct(event: EventView):
    """Benchmark-only (stage 5): write straight to Postgres, no Redis buffer.

    Exists purely as the "without the buffer" comparison point — not part of
    the real write path, and not something a production caller should use.
    """
    bucket_start = truncate_to_bucket(datetime.now(timezone.utc))
    await increment_direct(app.state.db_pool, event.resource_id, bucket_start)
    return {"resource_id": event.resource_id, "bucket_start": bucket_start}


@app.get("/views/{resource_id}", response_model=ViewsResponse)
async def read_views_from_db(resource_id: str, hours: int = 24):
    """Return per-minute view counts for a resource from Postgres.

    This only sees counts that a drain has already applied, so it lags behind
    the write path by up to one drain interval — it does not read Redis.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = await get_view_buckets(app.state.db_pool, resource_id, since)
    buckets = [ViewBucket(bucket_start=bucket_start, count=count) for bucket_start, count in rows]
    return ViewsResponse(
        resource_id=resource_id,
        since=since,
        total=sum(bucket.count for bucket in buckets),
        buckets=buckets,
    )
