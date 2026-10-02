"""View counter API: the write path.

Each view is recorded as an INCR on a Redis counter keyed by resource and UTC
hour bucket. Redis is a write buffer: counts are drained to Postgres in batches
by a separate worker, so this service never writes to Postgres per view.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, status

from .redis_store import get_redis_client, increment_view
from .schemas import EventView


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open one shared Redis client at startup and close it at shutdown.

    Code before `yield` runs once when the app starts, code after it runs once
    when the app stops. The client is kept on `app.state` so every request
    reuses the same connection pool.
    """
    app.state.redis = get_redis_client()
    yield
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/views", status_code=status.HTTP_202_ACCEPTED)
async def record_view(event: EventView):
    """Record one view by incrementing its hourly counter in Redis.

    Returns 202 only after Redis has answered the INCR, so an ACKed view is
    never one Redis hasn't seen. Durability after that point comes from Redis
    AOF (appendfsync everysec), which can lose up to ~1s on a crash.
    """
    key = await increment_view(app.state.redis, event.resource_id)
    return {"key": key}
