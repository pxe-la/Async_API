from typing import Optional

from redis.asyncio import Redis

redis: Optional[Redis] = None


def init_redis(redis_client: Redis):
    global redis
    redis = redis_client


async def get_redis() -> Redis:
    if not redis:
        raise ValueError("Redis client is not initialized.")
    return redis


async def close_redis():
    global redis
    if redis:
        await redis.close()
        redis = None
