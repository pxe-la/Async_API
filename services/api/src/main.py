from contextlib import asynccontextmanager

from api.v1 import films, genres, persons
from core.config import settings
from db.elastic import close_elastic, init_elastic
from db.redis import close_redis, init_redis
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_redis(Redis(host=settings.redis_host, port=settings.redis_port))
    init_elastic(AsyncElasticsearch(hosts=[settings.es_url]))

    yield

    await close_redis()
    await close_elastic()


app = FastAPI(
    title=settings.project_name,
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)


app.include_router(films.router, prefix="/api/v1/films", tags=["films"])
app.include_router(genres.router, prefix="/api/v1/genres", tags=["genres"])
app.include_router(persons.router, prefix="/api/v1/persons", tags=["persons"])
