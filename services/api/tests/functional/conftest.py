import asyncio

import pytest_asyncio
from fixtures.api import *  # noqa: F401, F403
from fixtures.es import *  # noqa: F401, F403
from fixtures.redis import *  # noqa: F401, F403


@pytest_asyncio.fixture(scope="session")
def _function_event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
