import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from settings import settings


@pytest_asyncio.fixture(scope="session")
async def es_client():
    client = AsyncElasticsearch(hosts=settings.es_url, verify_certs=False)
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="module")
async def es_fill_index(es_client):
    async def inner(index: str, mapping: dict, data: list[dict]):
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)

        await es_client.indices.create(index=index, **mapping)
        _, errors = await async_bulk(client=es_client, actions=data)
        if errors:
            raise Exception("Ошибка записи данных в Elasticsearch")

        await es_client.indices.refresh(index=index)

    return inner


class ElasticsearchManager:
    def __init__(
        self,
        client: AsyncElasticsearch,
        main_index: str,
    ):
        self.client = client
        self.main_index = main_index
        self.backup_index = f"{main_index}_backup"

    async def backup(self):
        if await self.client.indices.exists(index=self.main_index):
            if await self.client.indices.exists(index=self.backup_index):
                await self.client.indices.delete(index=self.backup_index)

            await self.client.reindex(
                body={
                    "source": {"index": self.main_index},
                    "dest": {"index": self.backup_index},
                },
                wait_for_completion=True,
                refresh=True,
            )

    async def clean(self):
        if await self.client.indices.exists(index=self.main_index):
            await self.client.delete_by_query(
                index=self.main_index,
                body={"query": {"match_all": {}}},
                refresh=True,
            )

    async def restore(self):
        if await self.client.indices.exists(index=self.main_index):
            await self.client.indices.delete(index=self.main_index)

        if await self.client.indices.exists(index=self.backup_index):
            await self.client.reindex(
                body={
                    "source": {"index": self.backup_index},
                    "dest": {"index": self.main_index},
                },
                wait_for_completion=True,
                refresh=True,
            )

    async def cleanup_backup(self):
        if await self.client.indices.exists(index=self.backup_index):
            await self.client.indices.delete(index=self.backup_index)


@pytest_asyncio.fixture(scope="function")
async def es_manager(request, es_client):
    index_name = request.param
    manager = ElasticsearchManager(client=es_client, main_index=index_name)

    await manager.backup()

    yield manager

    try:
        await manager.restore()
    finally:
        await manager.cleanup_backup()


__all__ = ["es_client", "es_fill_index", "es_manager"]
