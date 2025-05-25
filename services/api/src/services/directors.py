from functools import lru_cache
from typing import Annotated, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.director import Directors

DIRECTOR_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class DirectorService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, director_id: int) -> Optional[Directors]:
        redis_key = f"director:{director_id}"


        cached_director = await self.redis.get(redis_key)
        if cached_director:
            return Directors.model_validate_json(cached_director)

        try:

            director_doc = await self.elastic.get(
                index="directors",
                id=str(director_id))
        except NotFoundError:
            return None


        director = Directors(**director_doc["_source"])
        await self.redis.set(
            redis_key,
            director.model_dump_json(),
            DIRECTOR_CACHE_EXPIRE_IN_SECONDS
        )

        return director


@lru_cache()
def get_director_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> DirectorService:
    return DirectorService(redis, elastic)