from functools import lru_cache
from typing import Annotated, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from services.api.src.db.elastic import get_elastic
from services.api.src.db.redis import get_redis
from services.api.src.models.genre import Genre

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class GenreService:

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Optional[Genre]:
        redis_key = f"genre:{genre_id}"

        cached_genre = await self.redis.get(redis_key)
        if cached_genre:
            return Genre.model_validate_json(cached_genre)

        try:

            genre_doc = await self.elastic.get(index="genres", id=genre_id)
        except NotFoundError:
            return None

        genre = Genre(**genre_doc["_source"])

        await self.redis.set(
            redis_key, genre.model_dump_json(), GENRE_CACHE_EXPIRE_IN_SECONDS
        )

        return genre


@lru_cache()
def get_genre_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> GenreService:
    return GenreService(redis, elastic)
