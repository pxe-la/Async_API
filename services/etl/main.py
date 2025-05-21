import os
import time

from logic.elastic_loader import ElasticSearchLoader
from logic.postgres_producer import PostgresProducer
from utils.settings import settings
from utils.state import State
from utils.storages.json_storage import JsonFileStorage

if __name__ == "__main__":

    postgres_connect_data = {
        "dbname": settings.postgres_db,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "host": settings.postgres_host,
        "port": settings.postgres_port,
    }
    os.makedirs("states/", exist_ok=True)
    storage = JsonFileStorage("states/state.json")
    state = State(storage=storage)

    postgres_producer = PostgresProducer(postgres_connect_data, state)
    elastic_loader = ElasticSearchLoader(settings.elastic_host, 9200)

    elastic_loader.create_indexes()
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
