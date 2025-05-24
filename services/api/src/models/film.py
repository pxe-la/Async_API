from typing import Set
from uuid import UUID

from pydantic import BaseModel

from .genre import Genre
from .person import Person


class Film(BaseModel):
    id: UUID
    title: str
    description: str | None
    imdb_rating: float | None

    genres: Set[Genre]
    genre_names: Set[str]

    actors: Set[Person]
    actors_names: Set[str]

    directors: Set[Person]
    directors_names: Set[str]

    writers: Set[Person]
    writers_names: Set[str]
