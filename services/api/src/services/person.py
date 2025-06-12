import json
from functools import lru_cache
from typing import Annotated, List, Optional

from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.person import Person
from redis.asyncio import Redis

from .cache import CacheServiceProtocol, RedisCacheService
from .storage import StorageServiceProtocol, BaseElasticsearchService

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5
PERSON_LIST_CACHE_EXPIRE_IN_SECONDS = 60


class PersonService:
    INDEX = "persons"

    def __init__(self, cache: CacheServiceProtocol, storage: StorageServiceProtocol):
        self.cache = cache
        self.storage = storage

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        cache_key = self._get_person_cache_key(person_id)
        cached_person = await self.cache.get(cache_key)
        if cached_person:
            return Person.model_validate_json(cached_person)

        try:
            response = await self.storage.get(resource=self.INDEX, uuid=person_id)
        except NotFoundError:
            return None

        person = Person(**response["_source"])
        await self.cache.set(
            cache_key,
            person.model_dump_json(),
            PERSON_CACHE_EXPIRE_IN_SECONDS,
        )

        return person

    async def search_by_name(
        self, name: str, page_size: int, page_number: int
    ) -> List[Person]:
        cache_key = self._get_persons_search_cache_key(name, page_size, page_number)
        cached_persons = await self.cache.get(cache_key)
        if cached_persons:
            return [Person.model_validate_json(p) for p in json.loads(cached_persons)]

        response = await self.storage.search(
            resource=self.INDEX, page_size=page_size, page_number=page_number, name=name
        )

        persons = [Person(**hit["_source"]) for hit in response["hits"]["hits"]]
        await self.cache.set(
            cache_key,
            json.dumps([p.model_dump_json() for p in persons]),
            PERSON_LIST_CACHE_EXPIRE_IN_SECONDS,
        )

        return persons

    def _get_person_cache_key(self, person_id: str) -> str:
        return f"person:{person_id}"

    def _get_persons_search_cache_key(
        self, name: str, page_size: int, page_number: int
    ) -> str:
        return f"persons:search:{name}:{page_size}:{page_number}"


@lru_cache()
def get_person_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> PersonService:
    cache = RedisCacheService(redis)
    elastic_service = BaseElasticsearchService(elastic)
    return PersonService(cache, elastic_service)
