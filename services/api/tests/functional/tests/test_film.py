import json
from http import HTTPStatus
from uuid import uuid4

import pytest
import pytest_asyncio
from utils.is_uuid_valid import is_valid_uuid

index_name = "movies"
with open("resources/es_movies_mapping.json", "r") as f:
    index_mapping = json.load(f)


@pytest_asyncio.fixture(scope="module", autouse=True)
async def seed_es(es_fill_index, es_movies_asset):
    await es_fill_index(index_name, index_mapping, es_movies_asset)


@pytest.mark.parametrize(
    "uuid",
    [
        "b1f1e8a6-e310-47d9-a93c-6a7b192bac0e",
        "2dd036a4-f5d0-4e81-8073-a36da2a684b7",
        "edc66eec-eda9-4541-af98-4ec4012a740d",
    ],
)
@pytest.mark.asyncio
async def test_get_film_by_id(es_movies_asset, make_get_request, uuid):
    expected_movie = next(movie for movie in es_movies_asset if movie["id"] == uuid)

    response = await make_get_request(f"api/v1/films/{uuid}")

    assert response["status"] == HTTPStatus.OK

    response_body = response["body"]

    assert response_body["uuid"] == expected_movie["id"]
    assert response_body["title"] == expected_movie["title"]
    assert response_body["imdb_rating"] == expected_movie["imdb_rating"]

    def sort_key(x):
        return x["id"]

    assert sorted(response_body["genre"], key=sort_key) == sorted(
        expected_movie["genres"], key=sort_key
    )
    assert sorted(response_body["actors"], key=sort_key) == sorted(
        expected_movie["actors"], key=sort_key
    )
    assert sorted(response_body["writers"], key=sort_key) == sorted(
        expected_movie["writers"], key=sort_key
    )
    assert sorted(response_body["directors"], key=sort_key) == sorted(
        expected_movie["directors"], key=sort_key
    )


@pytest.mark.asyncio
async def test_get_non_existent_film_by_id(make_get_request):
    response = await make_get_request(f"api/v1/films/{uuid4()}")

    assert response["status"] == HTTPStatus.NOT_FOUND
    assert response["body"] == {"detail": "film not found"}


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"sort": "-imdb_rating", "page_size": 10, "page_number": 1},
        {"sort": "-imdb_rating", "page_size": 50, "page_number": 1},
        {"sort": "imdb_rating", "page_size": 10, "page_number": 5},
        {"sort": "imdb_rating", "page_size": 50, "page_number": 2},
        {
            "page_size": 50,
            "page_number": 2,
            "genre_id": "526769d7-df18-4661-9aa6-49ed24e9dfd8",
        },
        {
            "sort": "imdb_rating",
            "page_size": 50,
            "page_number": 2,
            "genre_id": "b92ef010-5e4c-4fd0-99d6-41b6456272cd",
        },
    ],
)
@pytest.mark.asyncio
async def test_list_films(flush_redis, make_get_request, params):
    response = await make_get_request("api/v1/films/", params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) == params.get("page_size", 50)

    for film in response["body"]:
        assert is_valid_uuid(film["uuid"])
        assert type(film["title"]) is str
        assert 0.0 <= film["imdb_rating"] <= 10.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sort_param",
    [
        None,
        "-imdb_rating",
        "imdb_rating",
    ],
)
async def test_list_films_sort(make_get_request, sort_param):
    params = {"sort": sort_param} if sort_param else {}
    response = await make_get_request("api/v1/films/", params)

    assert response["status"] == HTTPStatus.OK

    ratings = [
        film["imdb_rating"]
        for film in response["body"]
        if film["imdb_rating"] is not None
    ]

    reverse_order = sort_param.startswith("-") if sort_param else True
    assert ratings == sorted(ratings, reverse=reverse_order)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"sort": "wrong_sort_param"},
        {"sort": "-wrong_sort_param"},
        {"page_size": "string_instead_of_int"},
        {"page_size": 10.5},
        {"page_size": 0},
        {"page_size": -10},
        {"page_size": 1000},
        {"page_number": "string_instead_of_int"},
        {"page_number": 1.5},
        {"page_number": 0},
    ],
)
async def test_list_films_wrong_parameter(make_get_request, params):
    response = await make_get_request("api/v1/films/", params)

    assert response["status"] == HTTPStatus.UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"page_size": 2, "page_number": 1},
        {"page_size": 2, "page_number": 2},
        {"page_size": 50, "page_number": 1},
        {"page_size": 50, "page_number": 2},
        {"page_number": 2},
    ],
)
async def test_list_films_pagination_length(make_get_request, params):
    response = await make_get_request("api/v1/films/", params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) == params.get("page_size", 50)


@pytest.mark.asyncio
async def test_list_films_pagination_content(make_get_request):
    url = "api/v1/films/"
    response1 = await make_get_request(url, {"page_size": 50, "page_number": 1})
    response2 = await make_get_request(url, {"page_size": 50, "page_number": 2})
    response3 = await make_get_request(url, {"page_size": 100, "page_number": 1})

    assert (response1["body"] + response2["body"]) == response3["body"]


@pytest.mark.parametrize(
    "genre_id",
    [
        "526769d7-df18-4661-9aa6-49ed24e9dfd8",  # 27 entries
        "b92ef010-5e4c-4fd0-99d6-41b6456272cd",  # 88 entries
        "3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff",  # 244 entries
        str(uuid4()),  # Non-existent genre ID
    ],
)
@pytest.mark.asyncio
async def test_list_films_by_genre(make_get_request, es_movies_asset, genre_id):
    page_size = 100
    params = {"genre": genre_id, "page_size": page_size}

    queried_movie_ids = set()
    queried_total_length = 0
    for i in range(1, 100):
        response = await make_get_request("api/v1/films/", {**params, "page_number": i})

        assert response["status"] == HTTPStatus.OK

        queried_movie_ids.update({movie["uuid"] for movie in response["body"]})
        queried_total_length += len(response["body"])

        if len(response["body"]) < page_size:
            break

    expected_movie_ids_with_genre = {
        movie["id"]
        for movie in es_movies_asset
        if any(genre["id"] == genre_id for genre in movie["genres"])
    }

    assert queried_total_length == len(expected_movie_ids_with_genre)
    assert queried_movie_ids == expected_movie_ids_with_genre


@pytest.mark.parametrize(
    "params",
    [
        {"sort": "-imdb_rating", "page_size": 10, "page_number": 1},
        {"sort": "imdb_rating", "page_size": 10, "page_number": 1},
        {"genre_id": "6c162475-c7ed-4461-9184-001ef3d9f26e"},
    ],
)
@pytest.mark.parametrize("es_manager", index_name, indirect=True)
@pytest.mark.asyncio
async def test_list_films_cache(es_manager, make_get_request, params):
    response1 = await make_get_request("api/v1/films/", params)

    await es_manager.clean()

    response2 = await make_get_request("api/v1/films/", params)

    assert response1["status"] == HTTPStatus.OK
    assert response2["status"] == HTTPStatus.OK
    assert response1["body"] == response2["body"]
