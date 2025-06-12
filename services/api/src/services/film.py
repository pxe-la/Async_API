import json
from functools import lru_cache
from typing import Annotated, Any, Iterable, List, Optional

from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.film import Film
from redis.asyncio import Redis

from .cache import CacheServiceProtocol, RedisCacheService
from .storage import BaseElasticsearchService, StorageServiceProtocol

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5
FILM_LIST_CACHE_EXPIRE_IN_SECONDS = 60


class FilmElasticSearchService(BaseElasticsearchService):
    async def search(  # type: ignore[override]
        self,
        resource: str,
        query: dict[str, Any],
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
        **kwargs: Any,
    ) -> list[dict]:
        body: dict[str, Any] = {
            "query": query,
        }

        if page_size:
            body["size"] = page_size

        if page_number and page_size:
            body["from"] = (page_number - 1) * page_size

        if sort is not None:
            sort_field = sort.lstrip("-")
            order = "desc" if sort.startswith("-") else "asc"
            body["sort"] = [{sort_field: {"order": order}}]

        response = await self.elastic.search(index=resource, body=body)

        return response["hits"]["hits"]


class FilmService:
    INDEX = "movies"

    def __init__(self, cache: CacheServiceProtocol, storage: StorageServiceProtocol):
        self.cache = cache
        self.storage = storage

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        cache_key = self._get_film_cache_key(film_id)
        cached_film = await self.cache.get(cache_key)
        if cached_film:
            return Film.model_validate_json(cached_film)

        response = await self.storage.get(resource=self.INDEX, uuid=film_id)

        film = Film(**response)
        await self.cache.set(
            cache_key,
            film.model_dump_json(),
            FILM_CACHE_EXPIRE_IN_SECONDS,
        )

        return film

    async def search_films(
        self,
        query: str,
        page_size: int,
        page_number: int,
    ) -> List[Film]:
        cache_key = self._get_films_search_cache_key(query, page_size, page_number)
        cached_films = await self._get_films_from_cache(cache_key)
        if cached_films:
            return cached_films

        response = await self.storage.search(
            resource=self.INDEX,
            query={
                "multi_match": {
                    "query": query,
                    "fields": [
                        "title^3",
                        "description",
                        "genres_names",
                        "actors_names",
                        "directors_names",
                        "writers_names",
                    ],
                    "fuzziness": "AUTO",
                },
            },
            page_size=page_size,
            page_number=page_number,
        )
        films = [Film(**hit["_source"]) for hit in response]
        await self._save_films_to_cache(cache_key, films)
        return films

    async def list_films(
        self,
        page_size: int,
        page_number: int,
        genre_id: Optional[str] = None,
        sort: str = "imdb_rating",
    ) -> List[Film]:
        cache_key = self._get_films_list_cache_key(
            sort, genre_id, page_size, page_number
        )
        cached_films = await self._get_films_from_cache(cache_key)
        if cached_films:
            return cached_films

        response = await self.storage.search(
            resource=self.INDEX,
            query=(
                {"match_all": {}}
                if genre_id is None
                else {
                    "nested": {
                        "path": "genres",
                        "query": {"term": {"genres.id": genre_id}},
                    }
                }
            ),
            page_size=page_size,
            page_number=page_number,
            sort=sort,
        )
        films = [Film(**hit["_source"]) for hit in response]
        await self._save_films_to_cache(cache_key, films)
        return films

    async def get_films_with_person(
        self,
        person_id: str,
        page_size: int,
        page_number: int,
        sort: str = "imdb_rating",
    ) -> List[Film]:
        cache_key = self._get_person_films_cache_key(person_id)
        cached_films = await self._get_films_from_cache(cache_key)
        if cached_films:
            return cached_films

        response = await self.storage.search(
            resource=self.INDEX,
            query={
                "bool": {
                    "should": [
                        {
                            "nested": {
                                "path": "actors",
                                "query": {"term": {"actors.id": person_id}},
                            }
                        },
                        {
                            "nested": {
                                "path": "directors",
                                "query": {"term": {"directors.id": person_id}},
                            }
                        },
                        {
                            "nested": {
                                "path": "writers",
                                "query": {"term": {"writers.id": person_id}},
                            }
                        },
                    ]
                }
            },
            page_size=page_size,
            page_number=page_number,
            sort=sort,
        )
        films = [Film(**hit["_source"]) for hit in response]
        await self._save_films_to_cache(cache_key, films)
        return films

    def _get_film_cache_key(self, film_id: str) -> str:
        return f"film:{film_id}"

    def _get_films_search_cache_key(
        self, query: str, page_size: int, page_number: int
    ) -> str:
        return f"film:search:{query}:{page_size}:{page_number}"

    def _get_films_list_cache_key(
        self, sort: str, genre_id: Optional[str], page_size: int, page_number: int
    ) -> str:
        return f"films:list:{sort}:{genre_id}:{page_size}:{page_number}"

    def _get_person_films_cache_key(self, person_id: str) -> str:
        return f"person:{person_id}:roles"

    async def _save_films_to_cache(self, cache_key: str, films: Iterable[Film]) -> None:
        await self.cache.set(
            cache_key,
            json.dumps([f.model_dump(mode="json") for f in films]),
            FILM_LIST_CACHE_EXPIRE_IN_SECONDS,
        )

    async def _get_films_from_cache(self, cache_key: str) -> Optional[List[Film]]:
        cached_films = await self.cache.get(cache_key)
        if cached_films:
            return [Film.model_validate(item) for item in json.loads(cached_films)]

        return None


@lru_cache()
def get_film_service(
    redis: Annotated[Redis, Depends(get_redis)],
    elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)],
) -> FilmService:
    cache = RedisCacheService(redis)
    elastic_service = FilmElasticSearchService(elastic)
    return FilmService(cache, elastic_service)
