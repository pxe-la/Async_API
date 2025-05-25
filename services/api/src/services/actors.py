from functools import lru_cache
from typing import Annotated, Optional

from db.elastic import get_elastic
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.actor import Actors
from redis import get_redis
from redis.asyncio import Redis

ACTOR_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class ActorService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, actor_id: int) -> Optional[Actors]:
        redis_key = f"actor:{actor_id}"

        cached_actor = await self.redis.get(redis_key)
        if cached_actor:
            return Actors.model_validate_json(cached_actor)

        try:
            actor_doc = await self.elastic.get(
                index="actors", id=str(actor_id)  # Закрывающая скобка была пропущена
            )
        except NotFoundError:
            return None

        actor = Actors(**actor_doc["_source"])
        await self.redis.set(
            redis_key, actor.model_dump_json(), ACTOR_CACHE_EXPIRE_IN_SECONDS
        )

        return actor


@lru_cache()
def get_actor_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> ActorService:
    return ActorService(redis, elastic)
