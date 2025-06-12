from typing import Protocol, Optional, Any

from elasticsearch import AsyncElasticsearch

from models.film import Film


class StorageServiceProtocol(Protocol):
    async def get(self, resource: str, uuid: str) -> dict: ...

    async def search(
            self, resource: str, page_size: int, page_number: int,
            **kwargs: str
    ) -> list[dict]: ...

    async def get_list(
            self, resource: str, page_size: int, page_number: int
    ) -> list[dict]: ...


class BaseElasticsearchService(StorageServiceProtocol):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, resource: str, uuid: str) -> dict:
        return await self.elastic.get(index=resource, id=uuid)

    async def search(
            self, resource: str, page_size: int, page_number: int,
            **kwargs: str
    ) -> list[dict]:
        search_query = {
            "query": {"match": {**kwargs}},
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        return await self.elastic.search(index=resource, body=search_query)

    async def get_list(
            self, resource: str, page_size: int, page_number: int
    ) -> list[dict]:
        search_query = {
            "query": {"match_all": {}},
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        return await self.elastic.search(index=resource, body=search_query)


class FilmElasticSearchService(BaseElasticsearchService):
    INDEX = "movies"

    async def search(
            self,
            query: dict[str, Any],
            page_size: Optional[int] = None,
            page_number: Optional[int] = None,
            sort: Optional[str] = None,
            **kwargs: str,
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

        response = await self.elastic.search(index=self.INDEX, body=body)
        return [Film(**hit["_source"]) for hit in response["hits"]["hits"]]