from logging import config as logging_config

from pydantic_settings import BaseSettings, SettingsConfigDict

from .logger import LOGGING

logging_config.dictConfig(LOGGING)

dotenv_path = "../../.env"


# Валидирует настройки из .env
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    project_name: str = "Theater API"
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: str
    es_url: str
    redis_host: str
    redis_port: str


settings = Settings(_env_file=dotenv_path, _env_file_encoding="utf-8")  # type: ignore
