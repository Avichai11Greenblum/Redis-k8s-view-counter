"""View counter API: the write path.

Each view is recorded as an INCR on a Redis counter keyed by resource and UTC
hour bucket. Redis is a write buffer: counts are drained to Postgres in batches
by a separate worker, so this service never writes to Postgres per view.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, status
from pydantic import BaseModel
from redis.asyncio import Redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


class ViewEvent(BaseModel):
    """Request body for POST /views."""

    resource_id: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open one shared Redis client at startup and close it at shutdown.

    Code before `yield` runs once when the app starts, code after it runs once
    when the app stops. The client is kept on `app.state` so every request
    reuses the same connection pool.
    """
    app.state.redis = Redis.from_url(REDIS_URL, decode_responses=True)
    yield
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


def bucket_key(resource_id: str, now: datetime) -> str:
    """Build the Redis counter key, e.g. `views:post:42:2026092114`.

    The last segment is the fixed-width UTC hour bucket (YYYYMMDDHH), so the
    key can be split from the right even if `resource_id` contains colons.
    """
    return f"views:{resource_id}:{now:%Y%m%d%H}"


@app.post("/views", status_code=status.HTTP_202_ACCEPTED)
async def record_view(event: ViewEvent):
    """Record one view by incrementing its hourly counter in Redis.

    Returns 202 only after Redis has answered the INCR, so an ACKed view is
    never one Redis hasn't seen. Durability after that point comes from Redis
    AOF (appendfsync everysec), which can lose up to ~1s on a crash.
    """
    key = bucket_key(event.resource_id, datetime.now(timezone.utc))
    await app.state.redis.incr(key)
    return {"key": key}
