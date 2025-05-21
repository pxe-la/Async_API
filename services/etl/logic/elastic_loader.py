import json

import requests
from schemas.elasticsearch import ESMovieDocument
from utils.backoff import backoff
from utils.logging_settings import logger


class ElasticSearchLoader:

    def create_index(self, file_path: str, index_name: str):
        with open(file_path, "r") as f:
            index_data = json.load(f)

        request = requests.put(
            f"{self.base_url}/{index_name}",
            json=index_data,
            headers={"Content-Type": "application/json"},
        )
        status_code = request.status_code
        if status_code == 400:
            logger.warning("Elastic WARNING:\n" + request.json())
        if status_code == 500:
            logger.warning("Elastic ERROR:\n" + request.json())

    @backoff()
    def create_indexes(self) -> None:
        self.create_index("resources/movie_index.json", "movies")
        self.create_index("resources/genre_index.json", "genres")

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
            request_body += f"{doc.model_dump_json()}\n"  # Строка с объектом
        request_body += "\n"  # Необходим перенос строки в конце
        response = requests.post(
            f"{self.base_url}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=request_body,
        )
        logger.info(response.status_code)
