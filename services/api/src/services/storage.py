from typing import Any, Optional, Protocol


class StorageServiceProtocol(Protocol):
    async def get(self, resource: str, uuid: str) -> dict[Any, Any]:
        """Retrieve a single resource by its UUID.

        Args:
            resource: The type/index of resource to query
            uuid: Unique identifier of the resource

        Returns:
            Dictionary containing resource data
        """
        ...

    async def get_list(
        self, resource: str, page_size: int, page_number: int
    ) -> list[dict[Any, Any]]:
        """Retrieve a paginated list of resources.

        Args:
            resource: The type/index of resource to query
            page_size: Number of items per page
            page_number: Page number to retrieve (1-based)

        Returns:
            List of dictionaries containing resource data
        """
        ...

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

    async def search_raw_query(
        self,
        resource: str,
        query: dict[str, Any],
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[Any]:
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
