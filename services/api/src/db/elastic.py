from typing import Optional

from elasticsearch import AsyncElasticsearch

es: Optional[AsyncElasticsearch] = None


def init_elastic(es_client: AsyncElasticsearch):
    global es
    es = es_client


async def get_elastic() -> AsyncElasticsearch:
    if not es:
        raise ValueError("Elasticsearch client is not initialized.")
    return es


async def close_elastic():
    global es
    if es:
        await es.close()
        es = None
