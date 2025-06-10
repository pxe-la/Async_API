import json

import pytest_asyncio
from redis.asyncio import Redis
from settings import settings


@pytest_asyncio.fixture
async def redis_client():
    redis_client = Redis(host=settings.redis_host, port=settings.redis_port)
    await redis_client.flushdb()

    yield redis_client

    await redis_client.flushdb()
    await redis_client.aclose()


@pytest_asyncio.fixture
async def get_redis_cache(redis_client):
    async def inner(cache_key):
        data = await redis_client.get(cache_key)
        return json.loads(data)

    return inner


@pytest_asyncio.fixture
async def flush_redis(redis_client):
    async def inner():
        await redis_client.flushdb()

    return inner


__all__ = ["redis_client", "get_redis_cache", "flush_redis"]
