import asyncio

import pytest

from argus_addon.ha_client import HaClient


class _ClosingWs:
    closed = False

    def __init__(self):
        self.close_calls = 0

    async def send_json(self, payload):
        raise RuntimeError("Cannot write to closing transport")

    async def close(self):
        self.close_calls += 1


async def test_request_resets_ws_on_closing_transport():
    client = HaClient(supervisor_token="test-token")
    ws = _ClosingWs()
    client._ws = ws
    with pytest.raises(ConnectionResetError):
        await client._request({"type": "get_states"})
    assert client._ws is None
    assert ws.close_calls == 1


async def test_reconnects_after_server_drop(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        first = await client.fetch_entities()
        assert any(e.entity_id == "binary_sensor.kitchen_motion" for e in first)
        await ha_mock.drop_connections()
        for _ in range(50):
            if client._ws is None or client._ws.closed:
                break
            await asyncio.sleep(0.02)
        second = await client.fetch_entities()
        assert any(e.entity_id == "binary_sensor.kitchen_motion" for e in second)
    finally:
        await client.close()


async def test_connect_and_fetch_entities(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = await client.fetch_entities()
        ids = {e.entity_id for e in entities}
        assert "binary_sensor.kitchen_motion" in ids
        assert "switch.siren_living" in ids
        motion = next(e for e in entities if e.entity_id == "binary_sensor.kitchen_motion")
        assert motion.device_class == "motion"
        assert motion.domain == "binary_sensor"
        siren = next(e for e in entities if e.entity_id == "switch.siren_living")
        assert siren.domain == "switch"
    finally:
        await client.close()
    assert ha_mock.auth_token == "test-token"


async def test_call_service(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        await client.call_service("switch", "turn_on", {"entity_id": "switch.siren_living"})
    finally:
        await client.close()
    assert len(ha_mock.service_calls) == 1
    call = ha_mock.service_calls[0]
    assert call["domain"] == "switch"
    assert call["service"] == "turn_on"
    assert call["service_data"] == {"entity_id": "switch.siren_living"}


async def test_subscribe_events(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        await client.subscribe_events()
    finally:
        await client.close()


async def test_version_populated(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        assert client.version() == "2026.5.0"
    finally:
        await client.close()


async def test_entity_filter_includes_siren_domain_and_entity_category(ha_mock):
    ha_mock.entities = [
        {"entity_id": "binary_sensor.kitchen_motion", "device_class": "motion", "platform": "zha"},
        {"entity_id": "switch.siren_living", "device_class": None, "platform": "zha"},
        {"entity_id": "siren.camera1_siren", "device_class": None, "platform": "reolink"},
        {"entity_id": "switch.camera1_ftp", "device_class": None, "entity_category": "config", "platform": "reolink"},
        {"entity_id": "light.kitchen", "device_class": None, "platform": "zha"},
        {"entity_id": "sensor.temperature", "device_class": "temperature", "platform": "zha"},
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = await client.fetch_entities()
        by_id = {e.entity_id: e for e in entities}
        # siren domain is now ingested; light/sensor still excluded.
        assert "siren.camera1_siren" in by_id
        assert by_id["siren.camera1_siren"].domain == "siren"
        assert "light.kitchen" not in by_id
        assert "sensor.temperature" not in by_id
        # entity_category is forwarded so Argus can auto-hide config/diagnostic clutter.
        assert by_id["switch.camera1_ftp"].entity_category == "config"
        assert by_id["binary_sensor.kitchen_motion"].entity_category is None
    finally:
        await client.close()
