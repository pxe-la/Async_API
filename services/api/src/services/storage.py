from typing import Any, Protocol, cast

from elasticsearch import AsyncElasticsearch, NotFoundError


class StorageServiceProtocol(Protocol):
    async def get(self, resource: str, uuid: str) -> dict[Any, Any]: ...

    async def search(
        self, resource: str, page_size: int, page_number: int, **kwargs: Any
    ) -> list[dict[Any, Any]]: ...

    async def get_list(
        self, resource: str, page_size: int, page_number: int
    ) -> list[dict[Any, Any]]: ...


class BaseElasticsearchService(StorageServiceProtocol):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, resource: str, uuid: str) -> dict[Any, Any]:
        try:
            response = await self.elastic.get(index=resource, id=uuid)
            return cast(dict[Any, Any], response["_source"])
        except NotFoundError:
            return {}

    async def search(
        self, resource: str, page_size: int, page_number: int, **kwargs: Any
    ) -> list[dict[Any, Any]]:
        search_query = {
            "query": {"match": {**kwargs}},
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        response = await self.elastic.search(index=resource, body=search_query)
        return [hit["_source"] for hit in response["hits"]["hits"]]

    async def get_list(
        self, resource: str, page_size: int, page_number: int
    ) -> list[dict[Any, Any]]:
        search_query = {
            "query": {"match_all": {}},
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        response = await self.elastic.search(index=resource, body=search_query)
        return [hit["_source"] for hit in response["hits"]["hits"]]
