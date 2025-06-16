from typing import Any, Optional

from elasticsearch import AsyncElasticsearch
from services.storage import StorageServiceProtocol


class ElasticsearchStorageService(StorageServiceProtocol):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, resource: str, uuid: str) -> Any:
        response = await self.elastic.get(index=resource, id=uuid)

        return response["_source"]

    async def search_by_field(
        self,
        resource: str,
        field: str,
        query: str,
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[Any]:
        return await self.search_raw_query(
            resource,
            query={"match": {field: query}},
            page_size=page_size,
            page_number=page_number,
            sort=sort,
        )

    async def search_raw_query(
        self,
        resource: str,
        query: dict[str, Any],
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[Any]:
        search_query: dict[str, Any] = {
            "query": query,
            "size": page_size,
            "from": (page_number - 1) * page_size,
        }

        if sort is not None:
            sort_field = sort.lstrip("-")
            order = "desc" if sort.startswith("-") else "asc"
            search_query["sort"] = [{sort_field: {"order": order}}]

        response = await self.elastic.search(index=resource, body=search_query)
        return [hit["_source"] for hit in response["hits"]["hits"]]

    async def get_list(
        self,
        resource: str,
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[dict[Any, Any]]:
        return await self.search_raw_query(
            resource=resource,
            query={"match_all": {}},
            page_size=page_size,
            page_number=page_number,
            sort=sort,
        )
