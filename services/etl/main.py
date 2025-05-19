import datetime
import json
import os
import time
from typing import Dict, List

import psycopg
import requests
from psycopg import ClientCursor, Cursor
from psycopg.rows import dict_row
from schemas.elastic_search import ESMovieDocument, Person
from utils.backoff import backoff  # type: ignore
from utils.logging_settings import logger  # noqa: F401
from utils.settings import settings
from utils.state import State  # type: ignore
from utils.storages.json_storage import JsonFileStorage  # type: ignore


class ElasticSearchLoader:
    @backoff()
    def create_index(self) -> None:
        requests.put(
            f"{self.base_url}/movies",
            data=json.dumps(
                {
                    "settings": {
                        "refresh_interval": "1s",
                        "analysis": {
                            "filter": {
                                "english_stop": {
                                    "type": "stop",
                                    "stopwords": "_english_",
                                },
                                "english_stemmer": {
                                    "type": "stemmer",
                                    "language": "english",
                                },
                                "english_possessive_stemmer": {
                                    "type": "stemmer",
                                    "language": "possessive_english",
                                },
                                "russian_stop": {
                                    "type": "stop",
                                    "stopwords": "_russian_",
                                },
                                "russian_stemmer": {
                                    "type": "stemmer",
                                    "language": "russian",
                                },
                            },
                            "analyzer": {
                                "ru_en": {
                                    "tokenizer": "standard",
                                    "filter": [
                                        "lowercase",
                                        "english_stop",
                                        "english_stemmer",
                                        "english_possessive_stemmer",
                                        "russian_stop",
                                        "russian_stemmer",
                                    ],
                                }
                            },
                        },
                    },
                    "mappings": {
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "imdb_rating": {"type": "float"},
                            "genres": {"type": "keyword"},
                            "title": {
                                "type": "text",
                                "analyzer": "ru_en",
                                "fields": {"raw": {"type": "keyword"}},
                            },
                            "description": {"type": "text", "analyzer": "ru_en"},
                            "directors_names": {"type": "text", "analyzer": "ru_en"},
                            "actors_names": {"type": "text", "analyzer": "ru_en"},
                            "writers_names": {"type": "text", "analyzer": "ru_en"},
                            "directors": {
                                "type": "nested",
                                "dynamic": "strict",
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "name": {"type": "text", "analyzer": "ru_en"},
                                },
                            },
                            "actors": {
                                "type": "nested",
                                "dynamic": "strict",
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "name": {"type": "text", "analyzer": "ru_en"},
                                },
                            },
                            "writers": {
                                "type": "nested",
                                "dynamic": "strict",
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "name": {"type": "text", "analyzer": "ru_en"},
                                },
                            },
                        },
                    },
                }
            ),
            headers={"Content-Type": "application/json"},
        )

    def __init__(self, api_host: str, api_port: int):
        self.base_url = f"http://{api_host}:{api_port}"  # noqa: E231

    @backoff()
    def load(self, docs: dict[str, ESMovieDocument]) -> None:
        if len(docs) == 0:
            logger.info("Загрузка не требуется")
            return
        logger.info(f"Загружаем {len(docs)} фильмов")
        request_body = ""
        for doc in docs.values():
            id_row = {"index": {"_index": "movies", "_id": str(doc.id)}}
            request_body += f"{json.dumps(id_row)}\n"  # Строка с id
            request_body += f"{doc.model_dump_json()}\n"  # Строка с объхектом
        request_body += "\n"  # Необходим перенос строки в конце
        response = requests.post(
            f"{self.base_url}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=request_body,
        )
        logger.info(response.status_code)


class PostgresProducer:
    def __init__(self, connect_data: dict, state: State):
        self.state = state
        self.connect_data = connect_data

    def get_modified_ids(self, table_name: str, cursor: Cursor) -> List[str]:
        date_time = self.state.get_state(f"{table_name}_proceed_date_time")
        if not date_time:
            date_time = datetime.datetime.min
        else:
            date_time = datetime.datetime.fromisoformat(date_time)

        query = f"""
                    SELECT id, modified
                    FROM content.{table_name}
                    WHERE modified > %s
                    ORDER BY modified
                    LIMIT 100;
                """  # noqa: E702, E231, E241

        cursor.execute(query, (date_time.isoformat(),))
        data = cursor.fetchall()

        if len(data) == 0:
            return []

        checkpoint_datetime = data[-1]["modified"]
        logger.info(
            f"{table_name}_proceed_date_time: {checkpoint_datetime.isoformat()}"
        )
        self.state.set_state(
            f"{table_name}_proceed_date_time", checkpoint_datetime.isoformat()
        )

        return [str(item["id"]) for item in data]

    def get_films_with_modified_persons(
        self, modified_persons: list, cursor: Cursor
    ) -> List[str]:
        query = """
                    SELECT fw.id, fw.modified
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    WHERE pfw.person_id = ANY(%s)
                    ORDER BY fw.modified
                    ;
                """
        cursor.execute(query, (modified_persons,))
        data = cursor.fetchall()
        return [str(item["id"]) for item in data]

    def get_films_by_ids(
        self, films_with_modified_persons: List[str], cursor: Cursor
    ) -> List[dict]:
        query = """
                    SELECT
                        fw.id as fw_id,
                        fw.title as fw_title,
                        fw.description as fw_description,
                        fw.rating as fw_rating,
                        fw.created as fw_created,
                        fw.modified as fw_modified,
                        pfw.role as pfw_role,
                        p.id as p_id,
                        p.full_name as p_full_name,
                        g.name as g_name
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.id = ANY(%s);
                """

        cursor.execute(query, (films_with_modified_persons,))
        data = cursor.fetchall()
        return data

    def merge_data_to_models(  # noqa: CCR001
        self, data: List[dict]
    ) -> Dict[str, ESMovieDocument]:
        docs: Dict[str, ESMovieDocument] = {}
        for row in data:
            doc = docs.get(row["fw_id"])
            if not doc:
                doc = ESMovieDocument(
                    id=row["fw_id"],
                    title=row["fw_title"],
                    description=row["fw_description"],
                    imdb_rating=row["fw_rating"],
                    genres=set(),
                    actors=set(),
                    actors_names=set(),
                    directors=set(),
                    directors_names=set(),
                    writers=set(),
                    writers_names=set(),
                )
                docs[row["fw_id"]] = doc

            genre = row.get("g_name")
            if genre:
                doc.genres.add(genre)
            if not row.get("p_id"):
                continue
            person = Person(id=row["p_id"], name=row["p_full_name"])
            role = row.get("pfw_role")
            if role == "actor":
                doc.actors.add(person)
                doc.actors_names.add(person.name)
            elif role == "writer":
                doc.writers.add(person)
                doc.writers_names.add(person.name)
            elif role == "director":
                doc.directors.add(person)
                doc.directors_names.add(person.name)
        return docs

    def get_films_with_modified_genres(
        self, genres_ids: List[str], cursor: Cursor
    ) -> List[str]:
        query = """
                    SELECT fw.id, fw.modified
                    FROM content.film_work fw
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    WHERE gfw.genre_id = ANY(%s)
                    ORDER BY fw.modified;
                """
        cursor.execute(query, (genres_ids,))
        data = cursor.fetchall()
        return [str(item["id"]) for item in data]

    @backoff()
    def get_film_works_by_modified_persons(  # noqa: CCR001
        self,
    ) -> Dict[str, ESMovieDocument]:

        with psycopg.connect(
            **self.connect_data, row_factory=dict_row, cursor_factory=ClientCursor
        ) as pg_conn:
            cursor = pg_conn.cursor()
            modified_persons_ids = self.get_modified_ids("person", cursor)
            films_ids = self.get_films_with_modified_persons(
                modified_persons_ids, cursor
            )
            film_data = self.get_films_by_ids(films_ids, cursor)
            merged_objects = self.merge_data_to_models(film_data)
            return merged_objects

    @backoff()
    def get_films_by_modified_self(self) -> Dict[str, ESMovieDocument]:  # noqa: CCR001
        with psycopg.connect(
            **self.connect_data, row_factory=dict_row, cursor_factory=ClientCursor
        ) as pg_conn:
            cursor = pg_conn.cursor()
            films_ids = self.get_modified_ids("film_work", cursor)
            films_data = self.get_films_by_ids(films_ids, cursor)
            merged_objects = self.merge_data_to_models(films_data)
            return merged_objects

    @backoff()
    def get_film_works_by_modified_genres(
        self,
    ) -> Dict[str, ESMovieDocument]:  # noqa: CCR001
        with psycopg.connect(
            **self.connect_data, row_factory=dict_row, cursor_factory=ClientCursor
        ) as pg_conn:
            cursor = pg_conn.cursor()
            genres_ids = self.get_modified_ids("genre", cursor)
            films_ids = self.get_films_with_modified_genres(genres_ids, cursor)
            films_data = self.get_films_by_ids(films_ids, cursor)
            merged_objects = self.merge_data_to_models(films_data)
            return merged_objects


if __name__ == "__main__":

    postgres_connect_data = {
        "dbname": settings.postgres_db,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "host": settings.sql_host,
        "port": settings.sql_port,
    }
    os.makedirs("states/", exist_ok=True)
    storage = JsonFileStorage("states/state.json")
    state = State(storage=storage)

    postgres_producer = PostgresProducer(postgres_connect_data, state)
    elastic_loader = ElasticSearchLoader(os.getenv("ELASTIC_HOST", "localhost"), 9200)

    elastic_loader.create_index()
    while True:
        films = postgres_producer.get_films_by_modified_self()
        elastic_loader.load(films)
        time.sleep(1)

        films = postgres_producer.get_film_works_by_modified_genres()
        elastic_loader.load(films)
        time.sleep(1)

        films = postgres_producer.get_film_works_by_modified_persons()
        elastic_loader.load(films)
        time.sleep(1)
