"""Redis write-buffer logic: key layout, the INCR call, and drain support.

No FastAPI here. Everything in this file is plain Redis logic that could be
unit-tested or reused by the API and the drain worker alike.

Two key shapes live here, each as its own value object:
  BucketKey   -> views:{resource_id}:{YYYYMMDDHHMM}              (live, growing counter)
  DrainingKey -> draining:{resource_id}:{YYYYMMDDHHMM}:{batch_id} (frozen, mid-drain)
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from redis.asyncio import Redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

VIEW_KEY_PREFIX = "views"
DRAINING_KEY_PREFIX = "draining"
BUCKET_FORMAT = "%Y%m%d%H%M"
BUCKET_LENGTH = len("202609211407")  # fixed-width YYYYMMDDHHMM, always 12 digits


def _parse_bucket_str(bucket_str: str) -> datetime:
    """Strictly parse a YYYYMMDDHHMM segment.

    strptime alone isn't strict enough here: given a string shorter than the
    format expects, it can still find *some* valid interpretation by letting
    an earlier field (like %m) match fewer digits than normal — silently
    producing a wrong-but-plausible datetime instead of an error. That's how
    a leftover key from the old hour-only format (10 digits) used to get
    misparsed instead of rejected. Checking the length up front closes that.
    """
    if len(bucket_str) != BUCKET_LENGTH:
        raise ValueError(
            f"bucket segment {bucket_str!r} is not {BUCKET_LENGTH} digits "
            "(YYYYMMDDHHMM) — likely a stale key from an old key format"
        )
    return datetime.strptime(bucket_str, BUCKET_FORMAT).replace(tzinfo=timezone.utc)


def get_redis_client() -> Redis:
    """Build a new Redis client from REDIS_URL."""
    return Redis.from_url(REDIS_URL, decode_responses=True)


@dataclass(frozen=True)
class BucketKey:
    """A live, growing view counter for one resource and one minute bucket.

    str(key) gives the Redis key, e.g. `views:post:42:202609211407`. The
    bucket is a fixed-width trailing segment, so resource_id may itself
    contain colons without breaking the parse.

    Examples:
        >>> str(BucketKey("post:42", datetime(2026, 9, 21, 14, 7, tzinfo=timezone.utc)))
        'views:post:42:202609211407'
        >>> BucketKey.from_redis_key("views:post:42:202609211407")
        BucketKey(resource_id='post:42', bucket_start=datetime.datetime(2026, 9, 21, 14, 7, tzinfo=datetime.timezone.utc))
    """

    resource_id: str
    bucket_start: datetime

    def __str__(self) -> str:
        return f"{VIEW_KEY_PREFIX}:{self.resource_id}:{self.bucket_start:{BUCKET_FORMAT}}"

    @classmethod
    def from_redis_key(cls, key: str) -> "BucketKey":
        """Parse a `views:...` Redis key back into a BucketKey."""
        rest = key.removeprefix(f"{VIEW_KEY_PREFIX}:")
        resource_id, bucket_str = rest.rsplit(":", 1)
        return cls(resource_id, _parse_bucket_str(bucket_str))

    def draining_key(self, batch_id: str) -> "DrainingKey":
        """Build the DrainingKey this bucket moves to while being drained."""
        return DrainingKey(self.resource_id, self.bucket_start, batch_id)


@dataclass(frozen=True)
class DrainingKey:
    """A view counter frozen mid-drain, on its way to Postgres.

    batch_id is a fresh id per drain attempt (not derived from resource or
    bucket), so draining the same still-open bucket twice in a row produces
    two distinct keys/batches, never a collision.

    Examples:
        >>> str(DrainingKey("post:42", datetime(2026, 9, 21, 14, 7, tzinfo=timezone.utc), "a1b2c3"))
        'draining:post:42:202609211407:a1b2c3'
        >>> DrainingKey.from_redis_key("draining:page:count_example:202601030900:f00d")
        DrainingKey(resource_id='page:count_example', bucket_start=datetime.datetime(2026, 1, 3, 9, 0, tzinfo=datetime.timezone.utc), batch_id='f00d')
    """

    resource_id: str
    bucket_start: datetime
    batch_id: str

    def __str__(self) -> str:
        return f"{DRAINING_KEY_PREFIX}:{self.resource_id}:{self.bucket_start:{BUCKET_FORMAT}}:{self.batch_id}"

    @classmethod
    def from_redis_key(cls, key: str) -> "DrainingKey":
        """Parse a `draining:...` Redis key back into a DrainingKey.

        Parsed from the right: batch_id is always the last segment, the
        bucket is the one before it, and everything remaining is resource_id.
        """
        rest = key.removeprefix(f"{DRAINING_KEY_PREFIX}:")
        resource_id, bucket_str, batch_id = rest.rsplit(":", 2)
        return cls(resource_id, _parse_bucket_str(bucket_str), batch_id)


async def increment_view(redis: Redis, resource_id: str) -> str:
    """Record one view by incrementing its minute counter in Redis.

    Returns the key that was incremented, once Redis has confirmed the write.
    """
    key = BucketKey(resource_id, datetime.now(timezone.utc))
    await redis.incr(str(key))
    return str(key)


async def scan_keys(redis: Redis, prefix: str) -> list[str]:
    """List every key under a prefix using SCAN, not KEYS.

    KEYS walks the whole keyspace in one blocking call, freezing Redis for
    every other client until it finishes — a real problem at scale. SCAN
    walks it incrementally via a cursor, so normal traffic (our INCRs) isn't
    blocked while the worker looks for keys to drain.
    """
    return [key async for key in redis.scan_iter(match=f"{prefix}:*")]
