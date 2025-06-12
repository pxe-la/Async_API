import json
from http import HTTPStatus
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel, Field, computed_field
from utils.is_uuid_valid import is_valid_uuid

index_name = "movies"
with open("resources/es_movies_mapping.json", "r") as f:
    index_mapping = json.load(f)


class Person(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str


class Movie(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    title: str = "Title with some text"
    imdb_rating: float = 5.0

    description: str = "Description with some text"

    genres: list = []
    actors: list[Person] = Field(default_factory=lambda: [Person(name="Person name")])
    directors: list[Person] = Field(
        default_factory=lambda: [Person(name="Person name")]
    )
    writers: list[Person] = Field(default_factory=lambda: [Person(name="Person name")])

    @computed_field  # type: ignore[misc]
    @property
    def genres_names(self) -> list[str]:
        return [genre.name for genre in self.genres]

    @computed_field  # type: ignore[misc]
    @property
    def actors_names(self) -> list[str]:
        return [actor.name for actor in self.actors]

    @computed_field  # type: ignore[misc]
    @property
    def directors_names(self) -> list[str]:
        return [director.name for director in self.directors]

    @computed_field  # type: ignore[misc]
    @property
    def writers_names(self) -> list[str]:
        return [writer.name for writer in self.writers]


SEARCH_QUERIES = {
    "title": "QwertyXyzRandomTitle",
    "description": "ZxcvbnAsdfghRandomDesc",
    "actor": "MnbvcxRandomActorName",
    "director": "LkjihgRandomDirectorName",
    "writer": "YuiopRandomWriterName",
}

TEST_MOVIES = {
    "title": Movie(
        title=f"Title with {SEARCH_QUERIES['title']} and some text",
    ),
    "description": Movie(
        description=f"Description with {SEARCH_QUERIES['description']} and some text",
    ),
    "actor": Movie(
        actors=[Person(name=f"Actor {SEARCH_QUERIES['actor']}")],
    ),
    "director": Movie(
        directors=[Person(name=f"Director {SEARCH_QUERIES['director']}")],
    ),
    "writer": Movie(
        writers=[Person(name=f"Writer {SEARCH_QUERIES['writer']}")],
    ),
}

SEARCH_URL = "api/v1/films/search"


@pytest_asyncio.fixture(scope="module", autouse=True)
async def seed_es(es_fill_index, es_movies_asset):
    special_movies = [movie.model_dump() for movie in TEST_MOVIES.values()]
    await es_fill_index(index_name, index_mapping, special_movies + es_movies_asset)


@pytest.mark.parametrize(
    "params",
    [
        {"query": "Karl Urban", "page_number": 1, "page_size": 100},
        {"query": "an explosion on their moon", "page_number": 1, "page_size": 100},
        {"query": "Adventure Star Trek", "page_number": 2, "page_size": 30},
        {"query": "Gene Roddenberry", "page_number": 2},
        {"query": "First Contact", "page_size": 20},
        {"query": "Rick Berman"},
    ],
)
@pytest.mark.asyncio
async def test_search_response(
    make_get_request,
    params,
):
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) > 0

    for film in response["body"]:
        assert is_valid_uuid(film["uuid"])
        assert type(film["title"]) is str
        assert 0.0 <= film["imdb_rating"] <= 10.0


@pytest.mark.asyncio
async def test_search_pagination(
    make_get_request,
):
    page_size = 20
    query = "Adventure"

    response1 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size, "page_number": 1}
    )
    response2 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size, "page_number": 2}
    )
    response3 = await make_get_request(
        SEARCH_URL, {"query": query, "page_size": page_size * 2, "page_number": 1}
    )

    assert len(response1["body"]) > 0
    assert len(response2["body"]) > 0
    assert len(response3["body"]) > 0

    assert (response1["body"] + response2["body"]) == response3["body"]


@pytest.mark.asyncio
async def test_non_existent_search_response(
    make_get_request,
):
    params = {"query": "NonExistentFilmTitle"}
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.OK
    assert len(response["body"]) == 0


@pytest.mark.parametrize(
    "params",
    [
        {"query": "Rick Berman"},
        {"query": "Karl Urban", "page_number": 1, "page_size": 100},
        {"query": "an explosion on their moon", "page_number": 1, "page_size": 100},
        {"query": "Adventure Star Trek", "page_number": 2, "page_size": 30},
        {"query": "Gene Roddenberry", "page_number": 2},
        {"query": "First Contact", "page_size": 20},
    ],
)
@pytest.mark.parametrize("es_manager", index_name, indirect=True)
@pytest.mark.asyncio
async def test_search_cache(es_manager, make_get_request, params):
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
        {"query": "", "page_number": 1, "page_size": 100},
        {"query": "an explosion on their moon", "page_number": -1, "page_size": 100},
        {"query": "Adventure Star Trek", "page_number": 0, "page_size": 10},
        {"query": "Gene Roddenberry", "page_number": 2, "page_size": 0},
        {"query": "Gene Roddenberry", "page_number": 2, "page_size": -5},
        {"page_number": 1, "page_size": 10},
    ],
)
@pytest.mark.parametrize("es_manager", index_name, indirect=True)
@pytest.mark.asyncio
async def test_search_wrong_parameter(es_manager, make_get_request, params):
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.UNPROCESSABLE_ENTITY


@pytest.mark.parametrize(
    "test_param",
    ["title", "description", "actor", "director", "writer"],
)
@pytest.mark.asyncio
async def test_search_by_field(
    es_fill_index, es_movies_asset, make_get_request, test_param
):
    query = SEARCH_QUERIES[test_param]
    expected_movie = TEST_MOVIES[test_param]
    expected_ids = {expected_movie.id}

    params = {"query": query, "page_number": 1, "page_size": 10}
    response = await make_get_request(SEARCH_URL, params)

    assert response["status"] == HTTPStatus.OK

    returned_ids = {film["uuid"] for film in response["body"]}
    assert returned_ids == expected_ids
