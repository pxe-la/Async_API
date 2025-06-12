import json

import pytest_asyncio


@pytest_asyncio.fixture(scope="module")
async def es_movies_asset():
    with open("assets/es_movies.json", "r") as film_file:
        movies = json.load(film_file)

    return movies


@pytest_asyncio.fixture(scope="module")
async def es_genres_asset():
    with open("assets/es_genres.json", "r") as film_file:
        movies = json.load(film_file)

    return movies


@pytest_asyncio.fixture(scope="module")
async def es_persons_asset():
    with open("assets/es_persons.json", "r") as film_file:
        movies = json.load(film_file)

    return movies


__all__ = ["es_movies_asset", "es_genres_asset", "es_persons_asset"]
