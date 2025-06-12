import os

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

if os.path.exists("local.tests.env"):
    load_dotenv("local.tests.env")
else:
    load_dotenv("tests.env")


class TestSettings(BaseSettings):
    es_url: str = Field(alias="ES_URL")

    redis_host: str = Field(alias="REDIS_HOST")
    redis_port: str = Field(alias="REDIS_PORT")

    service_url: str = Field(alias="API_URL")


settings = TestSettings()
