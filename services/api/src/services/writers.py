from functools import lru_cache
from typing import Annotated, Optional

from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.writer import Writers
from redis.asyncio import Redis

WRITER_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class WriterService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, writer_id: int) -> Optional[Writers]:
        redis_key = f"writer:{writer_id}"

        cached_writer = await self.redis.get(redis_key)
        if cached_writer:
            return Writers.model_validate_json(cached_writer)

        try:

            writer_doc = await self.elastic.get(index="writers", id=str(writer_id))
        except NotFoundError:
            return None

        writer = Writers(**writer_doc["_source"])
        await self.redis.set(
            redis_key, writer.model_dump_json(), WRITER_CACHE_EXPIRE_IN_SECONDS
        )

        return writer


@lru_cache()
def get_writer_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> WriterService:
    return WriterService(redis, elastic)
