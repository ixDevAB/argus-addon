from datetime import UTC, datetime, timedelta

import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from argus_addon.ingress import build_app
from argus_addon.pair_client import CodeHolder


@pytest_asyncio.fixture
async def client(tmp_path):
    token_path = tmp_path / "token.txt"
    code_holder = CodeHolder()
    app = build_app(token_path, code_holder)
    server = TestServer(app)
    async with TestClient(server) as test_client:
        yield test_client, token_path, code_holder


async def test_index_waiting_when_no_token_and_no_code(client):
    test_client, _token_path, _holder = client
    resp = await test_client.get("/")
    assert resp.status == 200
    assert "text/html" in resp.headers.get("Content-Type", "")
    body = await resp.text()
    assert "Pair this Home with Argus" in body
    assert "Waiting for a pairing code" in body


async def test_index_shows_current_code(client):
    test_client, _token_path, holder = client
    holder.set("01939999-0000-7000-8000-000000000001", "012345", datetime.now(UTC) + timedelta(minutes=10))
    resp = await test_client.get("/")
    assert resp.status == 200
    body = await resp.text()
    assert "012345" in body
    assert "Scan this QR code with the Argus app" in body
    assert "<svg" in body


async def test_index_shows_expired_when_code_past_expiry(client):
    test_client, _token_path, holder = client
    holder.set("01939999-0000-7000-8000-000000000002", "654321", datetime.now(UTC) - timedelta(seconds=1))
    resp = await test_client.get("/")
    assert resp.status == 200
    body = await resp.text()
    assert "expired" in body
    assert "654321" not in body


async def test_index_after_token_saved_shows_paired(client):
    test_client, token_path, _holder = client
    token_path.write_text("present")
    resp = await test_client.get("/")
    assert resp.status == 200
    body = await resp.text()
    assert "Argus is paired" in body


async def test_no_token_post_route(client):
    test_client, _token_path, _holder = client
    resp = await test_client.post("/token", json={"token": "abc"})
    assert resp.status == 404


async def test_unpair_removes_token(client):
    test_client, token_path, _holder = client
    token_path.write_text("present")
    resp = await test_client.post("/unpair", allow_redirects=False)
    assert resp.status == 302
    assert not token_path.exists()
