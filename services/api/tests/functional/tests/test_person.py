import json
import random
from http import HTTPStatus
from uuid import uuid4

import pytest
import pytest_asyncio
from utils.is_uuid_valid import is_valid_uuid

index_name = "persons"
SEARCH_URL = "api/v1/persons/search"


with open("resources/es_persons_mapping.json", "r") as f:
    persons_mapping = json.load(f)

with open("resources/es_movies_mapping.json", "r") as f:
    movies_mapping = json.load(f)


@pytest_asyncio.fixture(scope="module", autouse=True)
async def seed_es(es_fill_index, es_persons_asset, es_movies_asset):
    await es_fill_index("persons", persons_mapping, es_persons_asset)
    await es_fill_index("movies", movies_mapping, es_movies_asset)


@pytest.mark.asyncio
async def test_get_person_by_id(make_get_request, es_persons_asset):
    person = random.choice(es_persons_asset)

    response = await make_get_request(f"api/v1/persons/{person['id']}")

    assert response["status"] == HTTPStatus.OK
    assert response["body"]["uuid"] == person["id"]
    assert response["body"]["name"] == person["name"]
    assert is_valid_uuid(response["body"]["uuid"])

    for film in response["body"]["films"]:
        assert is_valid_uuid(film["uuid"])
        assert len(film["roles"]) > 0
        assert all(isinstance(role, str) for role in film["roles"])
    assert isinstance(response["body"]["films"], list)


@pytest.mark.parametrize("es_manager", index_name, indirect=True)
@pytest.mark.asyncio
async def test_person_by_id_cache(es_manager, make_get_request, es_persons_asset):
    person = random.choice(es_persons_asset)
    person_id = person["id"]

    response1 = await make_get_request(f"api/v1/persons/{person_id}")
    await es_manager.clean()
    response2 = await make_get_request(f"api/v1/persons/{person_id}")

    assert response1["status"] == HTTPStatus.OK
    assert response2["status"] == HTTPStatus.OK
    assert len(response1["body"]) > 0
    assert response1["body"] == response2["body"]


@pytest.mark.asyncio
async def test_get_non_existent_person_by_id(make_get_request):
    non_existent_id = str(uuid4())
    response = await make_get_request(f"api/v1/persons/{non_existent_id}")

    assert response["status"] == HTTPStatus.NOT_FOUND


@pytest.mark.parametrize(
    "params",
    [
        {"query": "Karl Urban", "page_number": 1, "page_size": 10},
        {"query": "Jack", "page_number": 1, "page_size": 10},
        {"query": "Howard", "page_size": 5},
        {"query": "John"},
    ],
)
@pytest.mark.asyncio
async def test_search_person_response(make_get_request, params):
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) > 0
    for person in response["body"]:
        assert is_valid_uuid(person["uuid"])
        assert isinstance(person["name"], str)

        for film in person["films"]:
            assert is_valid_uuid(film["uuid"])
            assert len(film["roles"]) > 0
            assert all(isinstance(role, str) for role in film["roles"])


@pytest.mark.asyncio
async def test_search_person_pagination(make_get_request):
    page_size = 2
    query = "John"

    response1 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size, "page_number": 1}
    )
    response2 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size, "page_number": 2}
    )
    response3 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size * 2, "page_number": 1}
    )

    assert len(response1["body"]) == page_size
    assert len(response2["body"]) == page_size
    assert len(response3["body"]) > 0
    assert (response1["body"] + response2["body"]) == response3["body"]


@pytest.mark.asyncio
async def test_search_non_existent_person(make_get_request):
    params = {"query": "NonExistentPersonName"}
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) == 0


@pytest.mark.parametrize(
    "params",
    [
        {"query": "Karl Urban", "page_number": 1, "page_size": 10},
        {"query": "John"},
    ],
)
@pytest.mark.parametrize("es_manager", index_name, indirect=True)
@pytest.mark.asyncio
async def test_search_person_cache(es_manager, make_get_request, params):
    response1 = await make_get_request(SEARCH_URL, params)
    await es_manager.clean()
    response2 = await make_get_request(SEARCH_URL, params)

    assert response1["status"] == HTTPStatus.OK
    assert response2["status"] == HTTPStatus.OK
    assert len(response1["body"]) > 0
    assert response1["body"] == response2["body"]


@pytest.mark.parametrize(
    "params",
    [
        {"query": ""},
        {"query": "John", "page_number": -1},
        {"query": "John", "page_size": -1},
        {"query": "John", "page_size": 101},
        {"page_number": 1},
    ],
)
@pytest.mark.asyncio
async def test_search_person_wrong_parameter(make_get_request, params):
    response = await make_get_request(SEARCH_URL, params)
    assert response["status"] == HTTPStatus.UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
async def test_get_person_films(make_get_request, es_persons_asset, es_movies_asset):
    person_in_film = None
    expected_films = []

    for person in es_persons_asset:
        person_films = []
        for movie in es_movies_asset:
            actor_ids = [actor["id"] for actor in movie.get("actors", [])]
            writer_ids = [writer["id"] for writer in movie.get("writers", [])]
            director_ids = [director["id"] for director in movie.get("directors", [])]

            if person["id"] in actor_ids + writer_ids + director_ids:
                person_in_film = person
                person_films.append(movie)

        if person_in_film:
            expected_films = person_films
            break

    assert person_in_film is not None
    person_id = person_in_film["id"]

    response = await make_get_request(f"api/v1/persons/{person_id}/films")

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) > 0

    response_film_ids = {film["uuid"] for film in response["body"]}
    expected_film_ids = {movie["id"] for movie in expected_films}

    assert response_film_ids == expected_film_ids

    for film in response["body"]:
        assert is_valid_uuid(film["uuid"])
        assert isinstance(film["title"], str)
        assert 0.0 <= film["imdb_rating"] <= 10.0


@pytest.mark.asyncio
async def test_get_films_for_non_existent_person(make_get_request):
    non_existent_id = str(uuid4())
    response = await make_get_request(f"api/v1/persons/{non_existent_id}/film")

    assert response["status"] == HTTPStatus.NOT_FOUND
