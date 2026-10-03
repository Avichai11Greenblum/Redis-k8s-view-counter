"""Postgres access: schema setup and read queries.

This is the durable source of truth. The drain worker (stage 3) will write
here; for now this module only creates the tables and serves reads.
"""

import os
from datetime import datetime

from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://views:views@localhost:5432/views"
)

# Executed once at startup. IF NOT EXISTS makes this safe to run every time,
# so we don't need a separate migration step yet.
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS view_counts (
    resource_id  TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    count        BIGINT NOT NULL,
    PRIMARY KEY (resource_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS applied_batches (
    batch_id TEXT PRIMARY KEY
);
"""

# Step 1 of apply_batch: claim this batch_id.
INSERT_APPLIED_BATCH_SQL = (
    "INSERT INTO applied_batches (batch_id) VALUES (%s) "
    "ON CONFLICT DO NOTHING RETURNING batch_id"
)

# Step 2 of apply_batch: add this batch's count onto the bucket's running
# total. ON CONFLICT triggers if the row (resource_id, bucket_start) 
# already exists.
UPSERT_VIEW_COUNT_SQL = """
INSERT INTO view_counts (resource_id, bucket_start, count)
VALUES (%s, %s, %s)
ON CONFLICT (resource_id, bucket_start)
DO UPDATE SET count = view_counts.count + EXCLUDED.count
"""


def get_db_pool() -> AsyncConnectionPool:
    """Build a connection pool from DATABASE_URL.

    `open=False` because pool setup needs an event loop, which doesn't exist
    yet at import time — we open it explicitly during FastAPI's lifespan.
    """
    return AsyncConnectionPool(DATABASE_URL, open=False)


async def init_tables(pool: AsyncConnectionPool) -> None:
    """Create the tables if they don't already exist."""
    async with pool.connection() as conn:
        await conn.execute(CREATE_TABLES_SQL)


async def apply_batch(
    pool: AsyncConnectionPool,
    batch_id: str,
    resource_id: str,
    bucket_start: datetime,
    count: int,
) -> bool:
    """Apply one drained batch to view_counts, exactly once, ever.

    ***`INSERT ... ON CONFLICT DO NOTHING RETURNING` is the idempotency*** check: if
    batch_id is already in applied_batches, the insert is skipped, no row
    comes back, and we know not to add `count` again. Both statements run in
    one transaction, so "batch recorded" and "count applied" can't split —
    the caller is safe to retry (e.g. after a crash) and this is a no-op.

    Returns True if this call actually applied the count, False if the batch
    had already been applied by an earlier (or interrupted) attempt.
    """
    async with pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(INSERT_APPLIED_BATCH_SQL, (batch_id,))
            if await cur.fetchone() is None:
                return False  # already applied by a previous attempt — that's the idempotency check.

            await conn.execute(UPSERT_VIEW_COUNT_SQL, (resource_id, bucket_start, count))
            return True


async def get_view_buckets(
    pool: AsyncConnectionPool, resource_id: str, since: datetime
) -> list[tuple[datetime, int]]:
    """Return (bucket_start, count) rows for a resource, newest last.

    Only buckets at or after `since` are returned, so the caller controls the
    window (e.g. "last 24 hours") instead of this function hardcoding one.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT bucket_start, count
            FROM view_counts
            WHERE resource_id = %s AND bucket_start >= %s
            ORDER BY bucket_start ASC
            """,
            (resource_id, since),
        )
        return await cur.fetchall()
