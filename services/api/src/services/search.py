from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Optional

from db.elastic import get_elastic
from elasticsearch import AsyncElasticsearch, NotFoundError


class SearchServiceABC(ABC):
    @abstractmethod
    async def get(self, resource: str, uuid: str) -> Optional[Any]:
        """Retrieve a single resource by its UUID.

        Args:
            resource: The type/index of resource to query
            uuid: Unique identifier of the resource

        Returns:
            Dictionary containing resource data
        """
        ...

    @abstractmethod
    async def get_list(
        self, resource: str, page_size: int, page_number: int
    ) -> list[dict[str, Any]]:
        """Retrieve a paginated list of resources.

        Args:
            resource: The type/index of resource to query
            page_size: Number of items per page
            page_number: Page number to retrieve (1-based)

        Returns:
            List of dictionaries containing resource data
        """
        ...

    @abstractmethod
    async def search_by_field(
        self,
        resource: str,
        field: str,
        query: str,
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[dict[Any, Any]]:
        """Search resources by matching a specific field value.

        Args:
            resource: The type/index of resource to search
            field: Field name to search in
            query: Search query string
            page_size: Number of items per page
            page_number: Page number to retrieve (1-based)
            sort: Optional sort field with direction prefix (e.g. "-field_name")

        Returns:
            List of dictionaries containing matched resource data
        """
        ...

    @abstractmethod
    async def search_raw_query(
        self,
        resource: str,
        query: dict[str, Any],
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Search resources using a raw query.

        Args:
            resource: The type/index of resource to search
            query: Raw query dictionary specific to storage backend
            page_size: Number of items per page
            page_number: Page number to retrieve (1-based)
            sort: Optional sort field with direction prefix (e.g. "-field_name")

        Returns:
            List of dictionaries containing matched resource data
        """

        ...


class ElasticsearchSearchService(SearchServiceABC):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, resource: str, uuid: str) -> Optional[Any]:
        try:
            response = await self.elastic.get(index=resource, id=uuid)
        except NotFoundError:
            return None

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
    ) -> list[dict[str, Any]]:
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
    ) -> list[dict[str, Any]]:
        return await self.search_raw_query(
            resource=resource,
            query={"match_all": {}},
            page_size=page_size,
            page_number=page_number,
            sort=sort,
        )


@lru_cache()
def get_search_service() -> SearchServiceABC:
    return ElasticsearchSearchService(get_elastic())
