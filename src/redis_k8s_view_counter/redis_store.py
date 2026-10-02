"""Redis write-buffer logic: key layout and the INCR call.

No FastAPI here. Everything in this file is plain Redis logic that could be
unit-tested or reused (e.g. by the future drain worker) without the web layer.
"""

import os
from datetime import datetime, timezone

from redis.asyncio import Redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def get_redis_client() -> Redis:
    """Build a new Redis client from REDIS_URL."""
    return Redis.from_url(REDIS_URL, decode_responses=True)


def get_bucket_key(resource_id: str, now: datetime) -> str:
    """Build the Redis counter key, e.g. `views:post:42:2026092114`.

    The last segment is the fixed-width UTC hour bucket (YYYYMMDDHH), so the
    key can be split from the right even if `resource_id` contains colons.

    Examples:
        >>> get_bucket_key("post:42", datetime(2026, 9, 21, 14, 7, tzinfo=timezone.utc))
        'views:post:42:2026092114'
        >>> get_bucket_key("blog/redis-intro", datetime(2026, 1, 3, 9, 0, tzinfo=timezone.utc))
        'views:blog/redis-intro:2026010309'
    """
    return f"views:{resource_id}:{now:%Y%m%d%H}"


async def increment_view(redis: Redis, resource_id: str) -> str:
    """Record one view by incrementing its hourly counter in Redis.

    Returns the key that was incremented, once Redis has confirmed the write.
    """
    key = get_bucket_key(resource_id, datetime.now(timezone.utc))
    await redis.incr(key)
    return key
