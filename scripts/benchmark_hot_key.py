"""Stage 5 benchmark: buffered (Redis) vs direct-to-Postgres writes under a
hot key — N concurrent requests, all incrementing the SAME resource_id, so
every write contends for the same (resource_id, bucket_start) row at the
Postgres layer. This is the write-path aggregation payoff, made visible.

Run with `uv run uvicorn redis_k8s_view_counter.app:app` already up. The
drain worker does not need to be running — this only measures write-path
latency/throughput, not the read path.

Usage:
    uv run python scripts/benchmark_hot_key.py [--n 500] [--concurrency 50]
"""

import argparse
import asyncio
import time

import httpx

BASE_URL = "http://localhost:8000"


async def fire(client: httpx.AsyncClient, path: str, resource_id: str) -> float:
    start = time.perf_counter()
    await client.post(path, json={"resource_id": resource_id})
    return time.perf_counter() - start


async def run_benchmark(path: str, resource_id: str, n: int, concurrency: int) -> None:
    latencies: list[float] = []
    sem = asyncio.Semaphore(concurrency)

    async def bounded_fire(client: httpx.AsyncClient) -> None:
        async with sem:
            latencies.append(await fire(client, path, resource_id))

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as client:
        start = time.perf_counter()
        await asyncio.gather(*(bounded_fire(client) for _ in range(n)))
        wall = time.perf_counter() - start

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]

    print(f"{path}  (all {n} requests hitting the same resource_id={resource_id!r})")
    print(f"  concurrency:     {concurrency}")
    print(f"  wall time:       {wall:.2f}s")
    print(f"  throughput:      {n / wall:.1f} req/s")
    print(f"  latency p50/p95: {p50 * 1000:.1f}ms / {p95 * 1000:.1f}ms")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--n", type=int, default=500, help="requests per endpoint (default: 500)")
    parser.add_argument("--concurrency", type=int, default=50, help="concurrent in-flight requests (default: 50)")
    args = parser.parse_args()

    asyncio.run(run_benchmark(path="/views", resource_id="hot-key-buffered", n=args.n, concurrency=args.concurrency))
    asyncio.run(run_benchmark(path="/views/direct", resource_id="hot-key-direct", n=args.n, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
