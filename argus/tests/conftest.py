import pytest_asyncio

from tests._mocks.cloud_mock import CloudMock
from tests._mocks.ha_mock import HaMock


@pytest_asyncio.fixture
async def cloud_mock():
    mock = CloudMock()
    await mock.start()
    try:
        yield mock
    finally:
        await mock.close()


@pytest_asyncio.fixture
async def ha_mock():
    mock = HaMock()
    await mock.start()
    try:
        yield mock
    finally:
        await mock.close()


@pytest_asyncio.fixture
async def tmp_data_dir(tmp_path):
    yield tmp_path
