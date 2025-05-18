from contextlib import asynccontextmanager

from api.v1 import films
from core import config
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from db.elastic import close_elastic, init_elastic
from db.redis import close_redis, init_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_redis(Redis(host=config.REDIS_HOST, port=config.REDIS_PORT))
    init_elastic(
        AsyncElasticsearch(hosts=[f"{config.ELASTIC_HOST}:{config.ELASTIC_PORT}"])
    )

    yield

    await close_redis()
    await close_elastic()


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)


app.include_router(films.router, prefix="/api/v1/films", tags=["films"])
