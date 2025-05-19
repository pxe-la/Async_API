import os

from pydantic_settings import BaseSettings, SettingsConfigDict

local_dotenv_path = "../../local.env"
prod_dotenv_path = "../../.env"

dotenv_path = None

if os.path.exists(local_dotenv_path):
    dotenv_path = local_dotenv_path
elif os.path.exists(prod_dotenv_path):
    dotenv_path = prod_dotenv_path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    debug: str
    secret_key: str
    sql_engine: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    sql_host: str
    sql_port: str
    sql_options: str
    database_type: str
    elastic_host: str


settings = Settings(_env_file=local_dotenv_path, _env_file_encoding="utf-8")
