import json

import pytest
import pytest_asyncio

index_name = "movies"
with open("resources/es_movies_mapping.json", "r") as f:
    index_mapping = json.load(f)


@pytest_asyncio.fixture(scope="module")
async def es_movies_asset():
    with open("assets/es_movies.json", "r") as film_file:
        movies = json.load(film_file)

    return movies


@pytest.mark.skip(reason="Not ready")
@pytest.mark.parametrize(
    "query_data, expected_answer",
    [
        ({"query": "The Star"}, {"status": 200, "length": 50}),
        ({"query": "Mashed Potato"}, {"status": 200, "length": 0}),
        ({"query": "The Star", "page_size": 30}, {"status": 200, "length": 30}),
        (
            {"query": "The Star", "page_size": 40, "page_number": 2},
            {"status": 200, "length": 20},
        ),
    ],
)
@pytest.mark.asyncio
async def test_search(
    get_redis_cache,
    make_get_request,
    query_data,
    expected_answer,
):
    response = await make_get_request("api/v1/films/search", query_data)
    cache = await get_redis_cache("film", "search", query_data)

    cache_ids = {obj["id"] for obj in cache}
    response_ids = {[obj["uuid"] for obj in response["body"]]}

    # Проверка кэша
    assert cache_ids == response_ids
    # Статус код
    assert response["status"] == expected_answer["status"]
    # Количетсво объектов
    assert len(response["body"]) == expected_answer["length"]
