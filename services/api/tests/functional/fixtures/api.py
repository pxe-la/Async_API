import aiohttp
import pytest_asyncio
from settings import settings


@pytest_asyncio.fixture(scope="session")
async def client_http_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture
async def make_get_request(client_http_session):
    async def inner(url, query_data):
        full_url = settings.service_url + "/" + url

        async with client_http_session.get(full_url, params=query_data) as response:
            response_dict = {"body": await response.json(), "status": response.status}
        return response_dict

    return inner


__all__ = ["client_http_session", "make_get_request"]
