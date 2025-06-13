#!/usr/bin/env python3
import os

from backoff import backoff
from redis import Redis


@backoff()
def ping_redis(redis: Redis):
    redis.ping()


if __name__ == "__main__":
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    r = Redis(redis_host, socket_connect_timeout=1)

    ping_redis(r)
