import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

if os.path.exists("local.tests.env"):
    load_dotenv("local.tests.env")
else:
    load_dotenv("tests.env")


class TestSettings(BaseSettings):
    es_host: str = os.getenv("ES_URL")

    redis_host: str = os.getenv("REDIS_HOST")
    redis_port: str = os.getenv("REDIS_PORT")

    service_url: str = os.getenv("API_URL")

    es_index: str
    es_index_mapping: dict


# Экземпляр, который содержит настройки по умолчанию. Для фикстур в conftest.py
default_settings = TestSettings(es_index="", es_index_mapping={})
