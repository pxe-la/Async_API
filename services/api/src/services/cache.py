from typing import Optional, Protocol

from db.redis import get_redis


class CacheServiceProtocol(Protocol):
    async def get(self, key: str) -> Optional[str]: ...
    async def set(self, key: str, value: str, expire: int): ...


def get_cache_service() -> CacheServiceProtocol:
    return get_redis()
