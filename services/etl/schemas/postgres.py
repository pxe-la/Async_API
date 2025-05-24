import zoneinfo
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel


class UUIDMixin(BaseModel):
    id: UUID  # noqa: A003, VNE003

    def model_post_init(self, context: Any) -> None:
        if isinstance(self.id, str):
            self.id = UUID(self.id)


class TimeStampMixin(BaseModel):
    created_at: datetime
    updated_at: datetime

    def model_post_init(self, context: Any) -> None:
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.fromisoformat(self.updated_at)
        self.created_at.replace(tzinfo=zoneinfo.ZoneInfo(key="UTC"))
        self.updated_at.replace(tzinfo=zoneinfo.ZoneInfo(key="UTC"))


class TimeStampAndUUIDMixin(BaseModel):
    id: UUID  # noqa: A003, VNE003
    created_at: datetime
    updated_at: datetime

    def model_post_init(self, context: Any) -> None:
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.fromisoformat(self.updated_at)
        self.created_at = self.created_at.replace(tzinfo=timezone.utc)
        self.updated_at = self.updated_at.replace(tzinfo=timezone.utc)


class Genre(TimeStampAndUUIDMixin):
    name: str
    description: str


class FilmWork(TimeStampAndUUIDMixin):
    title: str
    description: str
    creation_date: date
    rating: float
    type: str  # noqa: A003, VNE003
    file_path: str
    certificate: Optional[str] = None


class Person(TimeStampAndUUIDMixin):
    full_name: str
    gender: Optional[str] = None


class GenreFilmWork(UUIDMixin):
    genre_id: UUID
    film_work_id: UUID
    created_at: datetime

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.genre_id, str):
            self.genre_id = UUID(self.genre_id)
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)
        self.created_at = self.created_at.replace(tzinfo=timezone.utc)


class PersonFilmWork(UUIDMixin):
    film_work_id: UUID
    person_id: UUID
    role: str
    created_at: datetime
