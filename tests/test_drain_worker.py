"""Stage 4: prove the drain worker's crash-safety claims, instead of
reproducing a crash by hand with a stopwatch.

Rather than actually kill -9'ing the worker process at the right
microsecond, each test reproduces the *sequence of effects* a crash would
leave behind — e.g. "Postgres committed, but the Redis key was never
deleted" — by calling the same functions the worker calls, stopping at the
point a crash would have interrupted them. The question these tests answer
is: given that leftover state, does recovery behave correctly?
"""

import uuid
from datetime import datetime, timezone

from redis_k8s_view_counter.db import apply_batch, get_view_buckets
from redis_k8s_view_counter.drain_worker import drain_key, recover_pending
from redis_k8s_view_counter.redis_store import BucketKey


async def total_count(db_pool, resource_id: str) -> int:
    since = datetime(2000, 1, 1, tzinfo=timezone.utc)
    rows = await get_view_buckets(db_pool, resource_id, since)
    return sum(count for _, count in rows)


async def test_apply_batch_is_idempotent(db_pool, resource_id, batch_ids):
    """Applying the same batch_id twice must only ever count once."""
    bucket_start = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex
    batch_ids.append(batch_id)

    first = await apply_batch(db_pool, batch_id, resource_id, bucket_start, 5)
    second = await apply_batch(db_pool, batch_id, resource_id, bucket_start, 5)

    assert first is True
    assert second is False  # the idempotency check caught the repeat
    assert await total_count(db_pool, resource_id) == 5  # not 10


async def test_recover_pending_resumes_a_crash_before_delete(redis_client, db_pool, resource_id, batch_ids):
    """The scenario stage 4 exists to prove: a worker that committed to
    Postgres but crashed before deleting the draining:* key. On restart,
    recover_pending must finish the batch without double-counting.
    """
    bucket_key = BucketKey(resource_id, datetime.now(timezone.utc))
    await redis_client.incr(str(bucket_key))
    await redis_client.incr(str(bucket_key))  # count = 2

    # This reproduces what drain_key does, but stops short of the final
    # DEL — that gap IS "the crash," made reproducible instead of a real
    # kill -9 timed by hand. batch_id mirrors production (uuid4().hex, no
    # colons) — DrainingKey parsing relies on batch_id being colon-free.
    draining_key = bucket_key.draining_key(uuid.uuid4().hex)
    batch_ids.append(draining_key.batch_id)
    await redis_client.rename(str(bucket_key), str(draining_key))
    await apply_batch(
        db_pool, draining_key.batch_id, draining_key.resource_id, draining_key.bucket_start, 2
    )
    # deliberately no redis_client.delete(...) here

    assert await redis_client.exists(str(draining_key))  # leftover, as if crashed

    recovered = await recover_pending(redis_client, db_pool)

    assert recovered == 1
    assert await total_count(db_pool, resource_id) == 2  # not 4
    assert not await redis_client.exists(str(draining_key))  # cleaned up


async def test_drain_key_skips_a_key_claimed_by_another_worker(redis_client, db_pool, resource_id):
    """Regression test for the race-handling added in drain_worker.py.

    A key that no longer exists when RENAME runs (another worker, a second
    replica later in K8s, got there first) must be skipped quietly, not
    crash the whole drain loop. Reproduced by simply never creating the key.
    """
    bucket_key = BucketKey(resource_id, datetime.now(timezone.utc))
    raw_key = str(bucket_key)  # never actually SET in Redis

    await drain_key(redis_client, db_pool, raw_key)  # must not raise


async def test_drain_key_deletes_unparseable_legacy_keys(redis_client, db_pool):
    """Regression test for the bug this branch fixed: a 10-digit, old
    hour-only-format key must be rejected and deleted, not silently
    misparsed into a wrong-but-valid datetime.
    """
    raw_key = "views:pytest-legacy-key:2026092609"  # 10 digits, pre-minute-bucket format
    await redis_client.set(raw_key, 1)

    await drain_key(redis_client, db_pool, raw_key)

    assert not await redis_client.exists(raw_key)


async def test_recover_pending_deletes_unparseable_draining_keys(redis_client, db_pool):
    """Regression test for a gap recover_pending had (found while fixing the
    tests above): one malformed draining:* key used to abort recovery for
    *every* pending batch found in the same scan, since the loop had no
    try/except around the parse and would raise straight out of it.
    """
    raw_key = "draining:pytest-legacy-key:2026092609:deadbeef"  # 10-digit bucket, old format
    await redis_client.set(raw_key, 1)

    recovered = await recover_pending(redis_client, db_pool)

    assert recovered == 1
    assert not await redis_client.exists(raw_key)
