from typing import Any, Optional, Protocol


class StorageServiceProtocol(Protocol):
    async def get(self, resource: str, uuid: str) -> dict[Any, Any]: ...

    async def search_by_text_fields(
        self,
        resource: str,
        fields: dict[str, str],
        page_size: int,
        page_number: int,
        sort: Optional[str] = None,
    ) -> list[dict[Any, Any]]: ...

    async def get_list(
        self, resource: str, page_size: int, page_number: int
    ) -> list[dict[Any, Any]]: ...
