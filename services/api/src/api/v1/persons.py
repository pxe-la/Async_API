from enum import StrEnum
from http import HTTPStatus
from typing import Annotated, Iterable, List, Optional
from uuid import UUID

from api.v1.films import FilmItemResponse
from fastapi import APIRouter, Depends, HTTPException, Query
from models.film import Film
from models.person import Person
from pydantic import BaseModel
from services.film import FilmService, get_film_service
from services.person import PersonService, get_person_service

router = APIRouter()


class RoleName(StrEnum):
    ACTOR = "actor"
    DIRECTOR = "director"
    WRITER = "writer"


class PersonDetailsResponseFilms(BaseModel):
    uuid: UUID
    roles: list[RoleName]

    @classmethod
    def from_models(cls, person: Person, film: Film) -> "PersonDetailsResponseFilms":
        roles = [
            role
            for role, persons in [
                (RoleName.ACTOR, film.actors),
                (RoleName.DIRECTOR, film.directors),
                (RoleName.WRITER, film.writers),
            ]
            if cls._find_id_in_persons(persons, person.id) is not None
        ]

        return cls(uuid=film.id, roles=roles)

    @classmethod
    def _find_id_in_persons(
        cls, persons: list[Person], person_id: UUID
    ) -> Optional[Person]:
        for person in persons:
            if person.id == person_id:
                return person
        return None


class PersonResponse(BaseModel):
    uuid: UUID
    name: str
    films: list[PersonDetailsResponseFilms]

    @classmethod
    def from_models(
        cls, person: Person, person_films: Iterable[Film]
    ) -> "PersonResponse":
        person_films_response = [
            PersonDetailsResponseFilms.from_models(person, film)
            for film in person_films
        ]

        return cls(uuid=person.id, name=person.name, films=person_films_response)


@router.get(
    "/search",
    response_model=List[PersonResponse],
    summary="Full-text search",
    description="Returns a list of films matching the search query. "
    "You can search by title, genres, description, actors, directors, and writers.",
    response_description="Film name and IMDb rating",
    tags=["films"],
)
async def search_person_by_name(
    person_service: Annotated[PersonService, Depends(get_person_service)],
    film_service: Annotated[FilmService, Depends(get_film_service)],
    query: str = Query(..., min_length=1, description="Person name to search for"),
    page_size: int = Query(50, ge=1, le=100),
    page_number: int = Query(1, ge=1),
) -> List[PersonResponse]:
    persons = await person_service.search_by_name(query, page_size, page_number)

    return [
        PersonResponse.from_models(
            person, await film_service.get_films_with_person(str(person.id))
        )
        for person in persons
    ]


@router.get(
    "/{person_id}/films",
    response_model=List[FilmItemResponse],
    summary="Person films list",
    tags=["persons"],
)
async def get_person_films(
    film_service: Annotated[FilmService, Depends(get_film_service)],
    person_id: str,
) -> List[FilmItemResponse]:
    films = await film_service.get_films_with_person(person_id)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="No persons found")

    return [FilmItemResponse.from_model(film) for film in films]


@router.get(
    "/{person_id}",
    response_model=PersonResponse,
    summary="Person details",
    tags=["persons"],
)
async def get_person_by_id(
    person_service: Annotated[PersonService, Depends(get_person_service)],
    film_service: Annotated[FilmService, Depends(get_film_service)],
    person_id: str,
) -> PersonResponse:
    person = await person_service.get_by_id(person_id)

    person_films = await film_service.get_films_with_person(person_id)

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="person not found")

    return PersonResponse.from_models(person, person_films)
