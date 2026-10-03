"""Drain worker: moves view counts from Redis into Postgres in batches.

Per-key drain, run on every views:* key found:
    1. RENAME views:{resource}:{bucket} -> draining:{resource}:{bucket}:{batch_id}
       Atomic, so a concurrent INCR either lands on the old key (and is
       picked up next drain) or the new one (created fresh after the
       rename) — never lost in between.
    2. Read the frozen count from the draining:* key.
    3. apply_batch(): idempotent Postgres upsert (see db.py).
    4. DEL the draining:* key.

If the worker dies between steps 3 and 4, the draining:* key is still in
Redis. On startup (and that's the only time), recover_pending() finds any
leftover draining:* keys and finishes them — step 3 is a no-op the second
time (same batch_id, already applied), so nothing is double-counted.
"""

import asyncio
import logging
import os
import uuid

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .db import apply_batch, get_db_pool, init_tables
from .redis_store import (
    DRAINING_KEY_PREFIX,
    VIEW_KEY_PREFIX,
    BucketKey,
    DrainingKey,
    get_redis_client,
    scan_keys,
)

DRAIN_INTERVAL_SECONDS = float(os.environ.get("DRAIN_INTERVAL_SECONDS", "5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s worker: %(message)s")
logger = logging.getLogger(__name__)


async def drain_key(redis: Redis, pool: AsyncConnectionPool, raw_key: str) -> None:
    """Drain a single live counter key (views:...) to Postgres.

    If another worker (a second replica, later, in K8s) already renamed this
    same key away between our SCAN and this RENAME, Redis raises "no such
    key". That's not a bug — it just means we lost the race for this one key,
    and the other worker now owns draining it. We skip it and move on.
    """
    try:
        bucket_key = BucketKey.from_redis_key(raw_key)
    except ValueError:
        # Not a race, not ours to drain — e.g. a leftover key from an old
        # key format that can never parse correctly. Nothing to apply, so
        # the only sane move is to remove it; otherwise it blocks every
        # future drain cycle forever.
        logger.warning("deleting unparseable key %s", raw_key)
        await redis.delete(raw_key)
        return

    draining_key = bucket_key.draining_key(uuid.uuid4().hex)

    try:
        await redis.rename(str(bucket_key), str(draining_key))
    except ResponseError as exc:
        if "no such key" in str(exc):
            logger.info("skipped %s, already claimed by another worker", bucket_key)
            return
        raise

    await apply_batch_and_delete(redis, pool, draining_key)


async def apply_batch_and_delete(redis: Redis, pool: AsyncConnectionPool, draining_key: DrainingKey) -> None:
    """Apply an already-renamed draining:* key to Postgres, then delete it.

    Shared by the normal drain path and startup recovery — both just need a
    DrainingKey, not anything about how it got there.
    """
    count = int(await redis.get(str(draining_key)))

    applied = await apply_batch(
        pool, draining_key.batch_id, draining_key.resource_id, draining_key.bucket_start, count
    )
    await redis.delete(str(draining_key))

    verb = "applied" if applied else "already applied, skipped"
    logger.info(f"{verb} {draining_key} count={count}")


async def drain_every_views_key(redis: Redis, pool: AsyncConnectionPool) -> int:
    """Drain every views:* key currently in Redis. Returns how many were drained."""
    raw_keys = await scan_keys(redis, VIEW_KEY_PREFIX)
    for raw_key in raw_keys:
        await drain_key(redis, pool, raw_key)
    return len(raw_keys)


async def recover_pending(redis: Redis, pool: AsyncConnectionPool) -> int:
    """Finish any draining:* keys left behind by a worker that crashed mid-batch.

    Only needs to run once, at startup, before the normal loop begins.
    """
    raw_keys = await scan_keys(redis, DRAINING_KEY_PREFIX)
    for raw_key in raw_keys:
        try:
            draining_key = DrainingKey.from_redis_key(raw_key)
        except ValueError:
            # Same reasoning as drain_key's unparseable-key handling: nothing
            # to recover from a key we can't even parse. Without this, one
            # bad leftover key would abort recovery for every other pending
            # batch too, since this loop would raise straight out of it.
            logger.warning("deleting unparseable key %s", raw_key)
            await redis.delete(raw_key)
            continue
        await apply_batch_and_delete(redis, pool, draining_key)
    if raw_keys:
        logger.info("recovered %d pending batch(es) from a previous run", len(raw_keys))
    return len(raw_keys)


async def run() -> None:
    redis = get_redis_client()
    pool = get_db_pool()
    await pool.open()
    await init_tables(pool)

    try:
        await recover_pending(redis, pool)
        logger.info("drain worker started, interval=%ss", DRAIN_INTERVAL_SECONDS)
        while True:
            drained = await drain_every_views_key(redis, pool)
            if drained:
                logger.info("drained %d key(s)", drained)
            await asyncio.sleep(DRAIN_INTERVAL_SECONDS)
    finally:
        await redis.aclose()
        await pool.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
