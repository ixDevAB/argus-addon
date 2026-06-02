import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from argus_addon.ingress import build_app


@pytest_asyncio.fixture
async def client(tmp_path):
    token_path = tmp_path / "token.txt"
    app = build_app(token_path)
    server = TestServer(app)
    async with TestClient(server) as test_client:
        yield test_client, token_path


async def test_index_serves_pair_html_when_no_token(client):
    test_client, _token_path = client
    resp = await test_client.get("/")
    assert resp.status == 200
    assert "text/html" in resp.headers.get("Content-Type", "")
    body = await resp.text()
    assert "jsQR" in body
    assert "Pair this Home with Argus" in body


async def test_post_token_writes_file(client):
    test_client, token_path = client
    resp = await test_client.post("/token", json={"token": "abc"})
    assert resp.status == 200
    data = await resp.json()
    assert data == {"ok": True}
    assert token_path.read_text() == "abc"


async def test_post_token_rejects_empty(client):
    test_client, token_path = client
    resp = await test_client.post("/token", json={"token": ""})
    assert resp.status == 400
    assert not token_path.exists()


async def test_post_token_rejects_missing_field(client):
    test_client, token_path = client
    resp = await test_client.post("/token", data="not json", headers={"Content-Type": "application/json"})
    assert resp.status == 400
    assert not token_path.exists()


async def test_post_token_rejects_too_long(client):
    test_client, token_path = client
    resp = await test_client.post("/token", json={"token": "x" * 257})
    assert resp.status == 400
    assert not token_path.exists()


async def test_index_after_token_saved_shows_paired(client):
    test_client, token_path = client
    token_path.write_text("present")
    resp = await test_client.get("/")
    assert resp.status == 200
    body = await resp.text()
    assert "Argus is paired" in body


async def test_static_jsqr_served(client):
    test_client, _token_path = client
    resp = await test_client.get("/static/jsQR.min.js")
    assert resp.status == 200
    body = await resp.read()
    assert len(body) > 30000


async def test_post_token_strips_whitespace(client):
    test_client, token_path = client
    resp = await test_client.post("/token", json={"token": "  abc  "})
    assert resp.status == 200
    assert token_path.read_text() == "abc"
