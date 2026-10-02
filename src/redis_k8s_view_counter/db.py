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
