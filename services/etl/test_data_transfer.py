import os

import requests
from dotenv import load_dotenv
from psycopg import ClientCursor, Connection, connect
from psycopg.rows import dict_row
from utils.logging_settings import logger


class TestDataTransfer:
    def __init__(self, es_base_url: str, app_api_base_url: str, pg_conn: Connection):
        self.app_api_base_url = app_api_base_url
        self.es_base_url = es_base_url
        self.pg_conn = pg_conn

    def test_films_count(self) -> None:
        query = """SELECT id FROM content.film_work"""
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        for film_uuid in data:
            film_uuid_str = str(film_uuid["id"])
            response = requests.get(
                f"{self.es_base_url}/movies/_doc/{film_uuid_str}"  # noqa: E231
            )
            assert response.status_code == 200
        logger.info("Количество фильмов в БД == количеству документов в ES")

    def test_data_equal(self) -> None:
        query = """SELECT id FROM content.film_work"""
        cursor = self.pg_conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        for film_uuid in data:
            film_uuid_str = str(film_uuid["id"])

            app_api_request = requests.get(
                f"{self.app_api_base_url}/api/v1/movies/{film_uuid_str}"
            )
            assert app_api_request.status_code == 200
            app_api_object = app_api_request.json()

            es_api_request = requests.get(
                f"{self.es_base_url}/movies/_doc/{film_uuid_str}"
            )
            assert es_api_request.status_code == 200
            es_api_object = es_api_request.json()["_source"]

            assert app_api_object["title"] == es_api_object["title"]
            assert app_api_object["description"] == es_api_object["description"]
            assert app_api_object["rating"] == es_api_object["imdb_rating"]

            assert app_api_object["genres"].sort() == es_api_object["genres"].sort()

            assert (
                app_api_object["actors"].sort() == es_api_object["actors_names"].sort()
            )
            assert (
                app_api_object["directors"].sort()
                == es_api_object["directors_names"].sort()
            )
            assert (
                app_api_object["writers"].sort()
                == es_api_object["writers_names"].sort()
            )
        logger.info("Данные в БД и ES полностью совпадают")


if __name__ == "__main__":
    local_dotenv_path = "../local.env"
    prod_dotenv_path = "../.env"

    if os.path.exists(local_dotenv_path):
        load_dotenv(local_dotenv_path)
    elif os.path.exists(prod_dotenv_path):
        load_dotenv(prod_dotenv_path)

    postgres_connect_data = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("SQL_HOST"),
        "port": os.getenv("SQL_PORT"),
    }
    with connect(
        **postgres_connect_data, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        tests = TestDataTransfer(
            "http://localhost:9200", "http://localhost:8000", pg_conn
        )
        tests.test_data_equal()
