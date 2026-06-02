from argus_addon.ha_client import HaClient


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


async def test_entity_filter_excludes_non_binary_switch(ha_mock):
    ha_mock.entities = [
        {"entity_id": "binary_sensor.kitchen_motion", "device_class": "motion", "platform": "zha"},
        {"entity_id": "switch.siren_living", "device_class": None, "platform": "zha"},
        {"entity_id": "light.kitchen", "device_class": None, "platform": "zha"},
        {"entity_id": "sensor.temperature", "device_class": "temperature", "platform": "zha"},
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = await client.fetch_entities()
        domains = {e.domain for e in entities}
        assert domains == {"binary_sensor", "switch"}
        ids = {e.entity_id for e in entities}
        assert "light.kitchen" not in ids
        assert "sensor.temperature" not in ids
    finally:
        await client.close()
