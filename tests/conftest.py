"""Shared fixtures for the integration test suite.

These tests run against the REAL Redis and Postgres from docker-compose,
not mocks — the thing under test is the interaction between the two
services across a simulated crash, which a mock can't reproduce honestly.
Run `docker compose up -d` before running these.
"""

import uuid

import pytest
import pytest_asyncio

from redis_k8s_view_counter.db import get_db_pool, init_tables
from redis_k8s_view_counter.redis_store import get_redis_client


@pytest_asyncio.fixture
async def redis_client():
    client = get_redis_client()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def db_pool():
    pool = get_db_pool()
    await pool.open()
    await init_tables(pool)
    yield pool
    await pool.close()


@pytest.fixture
def resource_id() -> str:
    """A unique resource id per test run.

    Prefixed so it can never collide with real data, and so tests never
    collide with each other even run in parallel.
    """
    return f"pytest:{uuid.uuid4().hex}"


@pytest.fixture
def batch_ids() -> list[str]:
    """Tests append any batch_id they create here; cleaned up exactly below.

    Deliberately exact-match cleanup, not a LIKE pattern keyed off
    resource_id: batch_id must never contain a colon (DrainingKey parsing
    relies on that), and resource_id here does contain one (`pytest:...`),
    so building one from the other is exactly the trap one of these tests
    exists to avoid.
    """
    return []


@pytest_asyncio.fixture(autouse=True)
async def cleanup_test_rows(db_pool, resource_id, batch_ids):
    """Remove whatever this test wrote."""
    yield
    async with db_pool.connection() as conn:
        await conn.execute("DELETE FROM view_counts WHERE resource_id = %s", (resource_id,))
        for batch_id in batch_ids:
            await conn.execute("DELETE FROM applied_batches WHERE batch_id = %s", (batch_id,))
