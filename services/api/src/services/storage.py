from typing import Protocol

from elasticsearch import AsyncElasticsearch


class StorageServiceProtocol(Protocol):
    async def get(self, resource: str, uuid: str) -> dict: ...

    async def search(
            self, resource: str, page_size: int, page_number: int,
            **kwargs: str
    ) -> list[dict]: ...

    async def get_list(self, resource: str, page_size: int,
                       page_number: int) -> list[dict]: ...


class ElasticsearchService(StorageServiceProtocol):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, resource: str, uuid: str) -> dict:
        return self.elastic.get(index=resource, id=uuid)

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

    async def get_list(self, resource: str, page_size: int,
                       page_number: int) -> list[dict]:
        search_query = {
            "query": {"match_all": {}},
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        return await self.elastic.search(index=resource, body=search_query)
