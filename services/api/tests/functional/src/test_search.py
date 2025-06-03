import json
import uuid
import asyncio
import aiohttp
import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from redis.asyncio import Redis

from tests.functional.settings import test_settings


@pytest_asyncio.fixture(scope='session')
def _function_event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(name="redis_client", scope="session")
async def redis_client():
    redis_client = Redis(host=test_settings.redis_host,
                         port=test_settings.redis_port)
    yield redis_client
    await redis_client.close()


@pytest_asyncio.fixture(name="es_client", scope="session")
async def es_client():
    es_client = AsyncElasticsearch(hosts=test_settings.es_host,
                                   verify_certs=False)
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(name="client_http_session", scope="session")
async def client_http_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture(name="make_get_request")
async def make_get_request(client_http_session):
    async def inner(url, query_data):
        full_url = test_settings.service_url + "/" + url

        async with client_http_session.get(full_url,
                                           params=query_data) as response:
            response_dict = {
                "body": await response.json(),
                "status": response.status
            }
        return response_dict

    return inner


@pytest_asyncio.fixture(name="es_data")
async def es_data():
    es_data = [{
        "id": str(uuid.uuid4()),
        "imdb_rating": 8.5,
        "title": "The Star",
        "description": "New World",
        "genres_names": ["Fantasy", "Fantastic"],
        "directors_names": ["Stan"],
        "actors_names": ["Ann", "Bob"],
        "writers_names": ["Ben", "Howard"],
        "genres": [
            {"id": "ef86b8ff-3c82-4d31-ad8e-72b69f5E3f95", "name": "Fantasy"},
            {"id": "fb111f22-121e-44a7-b78f-b19191810fbf", "name": "Fantastic"}
        ],
        "directors": [
            {"id": "ef86b8ff-3c82-4d31-ad8e-72b69f4e3f85", "name": "Stan"},
        ],
        "actors": [
            {"id": "ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95", "name": "Ann"},
            {"id": "fb111f22-121e-44a7-b78f-b19191810fbf", "name": "Bob"}
        ],
        "writers": [
            {"id": "caf76c67-c0fe-477e-8766-3ab3ff2574b5", "name": "Ben"},
            {"id": "b45bd7bc-2e16-46d5-b125-983d356768c6", "name": "Howard"}
        ],
    } for _ in range(60)]

    bulk_query = []
    for row in es_data:
        bulk_query.append({
            "_op_type": "index",
            "_index": test_settings.es_index,
            "_id": row["id"],
            "_source": row
        })
    return bulk_query


@pytest_asyncio.fixture(name="es_write_data")
async def es_write_data(es_client):
    async def inner(data: list[dict]):
        if await es_client.indices.exists(index=test_settings.es_index):
            await es_client.indices.delete(index=test_settings.es_index)
        await es_client.indices.create(index=test_settings.es_index,
                                       **test_settings.es_index_mapping)
        updated, errors = await async_bulk(client=es_client, actions=data)
        await es_client.indices.refresh(index=test_settings.es_index)

        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')

    return inner


@pytest_asyncio.fixture(name="get_redis_cache")
async def get_redis_cache(redis_client):
    async def inner(query_data: dict[str, str]):
        search = query_data.get("query", "")
        page_size = query_data.get("page_size", 50)
        page = query_data.get("page", 1)
        data = await redis_client.get(
            f"film:search:{search}:{page_size}:{page}")
        return json.loads(data)

    return inner


@pytest.mark.parametrize(
    "query_data, expected_answer",
    [
        (
                {"query": "The Star"},
                {"status": 200, "length": 50}
        ),
        (
                {"query": "Mashed Potato"},
                {"status": 200, "length": 0}
        ),
        (
                {"query": "The Star", "page_size": 30},
                {"status": 200, "length": 30}
        ),
        (
                {"query": "The Star", "page_size": 40, "page_number": 2},
                {"status": 200, "length": 20}
        )
    ]
)
@pytest.mark.asyncio
async def test_search(get_redis_cache, make_get_request, es_write_data,
                      es_data: list[dict],
                      query_data,
                      expected_answer):
    await es_write_data(es_data)

    response = await make_get_request("api/v1/films/search", query_data)
    cache = await get_redis_cache(query_data)

    cache_ids = set([obj["id"] for obj in cache])
    response_ids = set([obj["uuid"] for obj in response["body"]])

    # Проверка кэша
    assert cache_ids == response_ids
    # Статус код
    assert response["status"] == expected_answer["status"]
    # Количетсво объектов
    assert len(response["body"]) == expected_answer["length"]
