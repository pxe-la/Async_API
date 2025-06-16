import json
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Annotated, List, Optional

from fastapi import Depends
from models.genre import Genre

from .cache import CacheServiceProtocol, get_cache_service
from .search import SearchServiceABC, get_search_service

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5
GENRE_LIST_CACHE_EXPIRE_IN_SECONDS = 60


class GenreServiceABC(ABC):
    @abstractmethod
    async def get_by_id(self, genre_id: str) -> Optional[Genre]:
        """Retrieve a genre by its ID."""
        ...

    @abstractmethod
    async def list_genres(self, page_size: int, page_number: int) -> List[Genre]:
        """List genres with pagination."""
        ...


class GenreService(GenreServiceABC):
    INDEX = "genres"

    def __init__(self, cache: CacheServiceProtocol, search_service: SearchServiceABC):
        self.cache = cache
        self.search_service = search_service

    async def get_by_id(self, genre_id: str) -> Optional[Genre]:
        cache_key = self._get_genre_cache_key(genre_id)
        cached_genre = await self.cache.get(cache_key)
        if cached_genre:
            return Genre.model_validate_json(cached_genre)

        response = await self.search_service.get(resource=self.INDEX, uuid=genre_id)

        if response is None:
            return None

        genre = Genre(**response)
        await self.cache.set(
            cache_key,
            genre.model_dump_json(),
            GENRE_CACHE_EXPIRE_IN_SECONDS,
        )

        return genre

    async def list_genres(
        self,
        page_size,
        page_number,
    ) -> List[Genre]:
        cache_key = self._get_genres_list_cache_key(page_size, page_number)
        cached_genres = await self._get_genres_from_cache(cache_key)
        if cached_genres:
            return cached_genres

        response = await self.search_service.get_list(
            resource=self.INDEX, page_size=page_size, page_number=page_number
        )

        genres = [Genre(**item) for item in response]
        await self._save_genres_to_cache(cache_key, genres)

        return genres

    def _get_genre_cache_key(self, genre_id: str) -> str:
        return f"genre:{genre_id}"

    def _get_genres_list_cache_key(self, page_size: int, page_number: int) -> str:
        return f"genres:list:{page_size}:{page_number}"

    async def _save_genres_to_cache(self, cache_key: str, genres: List[Genre]) -> None:
        await self.cache.set(
            cache_key,
            json.dumps([f.model_dump(mode="json") for f in genres]),
            GENRE_LIST_CACHE_EXPIRE_IN_SECONDS,
        )

    async def _get_genres_from_cache(self, cache_key: str) -> Optional[List[Genre]]:
        cached_genres = await self.cache.get(cache_key)
        if cached_genres:
            return [Genre.model_validate(item) for item in json.loads(cached_genres)]

        return None


@lru_cache()
def get_genre_service(
    cache_service: Annotated[CacheServiceProtocol, Depends(get_cache_service)],
    search_service: Annotated[SearchServiceABC, Depends(get_search_service)],
) -> GenreServiceABC:
    return GenreService(cache_service, search_service)
