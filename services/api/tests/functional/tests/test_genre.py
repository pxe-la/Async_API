import json
import uuid

import pytest
import pytest_asyncio

index_name = "genres"
with open("resources/es_genres_mapping.json", "r") as f:
    index_mapping = json.load(f)


@pytest_asyncio.fixture(scope="module", autouse=True)
async def seed_es(es_fill_index, es_genres_asset):

    index_data = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_id": movie["id"],
            "_source": movie,
        }
        for movie in es_genres_asset
    ]

    await es_fill_index(index_name, index_mapping, index_data)


@pytest_asyncio.fixture
async def es_data(test_settings):
    es_data = [
        {
            "id": str(uuid.uuid4()),
            "name": "Drama",
            "description": "some desc",
        }
        for _ in range(60)
    ]

    bulk_query = []
    for row in es_data:
        bulk_query.append(
            {
                "_op_type": "index",
                "_index": test_settings.es_index,
                "_id": row["id"],
                "_source": row,
            }
        )
    return bulk_query


@pytest.mark.parametrize(
    "query_data, expected_answer",
    [
        ({}, {"status": 200, "length": 50}),
        ({"page_size": 40, "page_number": 1}, {"status": 200, "length": 40}),
        ({"page_size": 40, "page_number": 2}, {"status": 200, "length": 40}),
    ],
)
@pytest.mark.asyncio
async def test_genres_list(
    get_redis_cache,
    make_get_request,
    query_data,
    expected_answer,
):
    response = await make_get_request("api/v1/genres/", query_data)

    cache_key = "genres:list"

    page_size = query_data.get("page_size", "50")
    cache_key += ":" + str(page_size)

    page_number = query_data.get("page_number", "1")
    cache_key += ":" + str(page_number)

    cache = await get_redis_cache(cache_key)
    cache_ids = {obj["id"] for obj in cache}
    response_ids = {obj["uuid"] for obj in response["body"]}

    assert cache_ids == response_ids

    assert response["status"] == expected_answer["status"]

    assert len(response["body"]) == expected_answer["length"]


@pytest.mark.asyncio
async def test_genres_detail_200(get_redis_cache, make_get_request, es_client):
    response = await make_get_request("api/v1/genres/", {})
    obj = response["body"][0]
    obj_uuid = obj["uuid"]

    response_2 = await make_get_request(f"api/v1/genres/{obj_uuid}", {})

    cache_obj = await get_redis_cache(f"genre:{obj_uuid}")

    assert response_2["status"] == 200
    assert response_2["body"] == obj
    assert cache_obj["id"] == obj["uuid"]
