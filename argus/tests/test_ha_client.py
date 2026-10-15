import asyncio

import pytest

from argus_addon.ha_client import HaClient, merge_candidates


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
        assert "light.hall" in ids  # light outputs (deterrent) are synced too
        light = next(e for e in entities if e.entity_id == "light.hall")
        assert light.domain == "light"
    finally:
        await client.close()
    assert ha_mock.auth_token == "test-token"


async def test_fetch_forwards_platform_and_translation_key(ha_mock):
    ha_mock.devices = [{"id": "dev-front", "name": "Framsidan", "name_by_user": None, "area_id": None}]
    ha_mock.entities = [
        {
            "entity_id": "binary_sensor.framsidan_person",
            "device_class": None,
            "original_device_class": None,
            "platform": "reolink",
            "translation_key": "person",
            "device_id": "dev-front",
        },
        {
            "entity_id": "binary_sensor.framsidan_rorelse",
            "device_class": None,
            "original_device_class": "motion",
            "platform": "reolink",
            "translation_key": None,
            "device_id": "dev-front",
        },
        {"entity_id": "binary_sensor.hall_pir", "device_class": "motion", "platform": "zha"},
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = {e.entity_id: e for e in await client.fetch_entities()}
        person = entities["binary_sensor.framsidan_person"]
        assert person.platform == "reolink"
        assert person.translation_key == "person"
        assert person.device_class is None
        assert person.device_name == "Framsidan"
        motion = entities["binary_sensor.framsidan_rorelse"]
        assert motion.platform == "reolink"
        assert motion.translation_key is None
        assert motion.device_class == "motion"
        pir = entities["binary_sensor.hall_pir"]
        assert pir.platform == "zha"
        assert pir.translation_key is None
    finally:
        await client.close()


async def test_fetch_syncs_charging_state_and_enables_it_when_integration_disabled(ha_mock):
    ha_mock.devices = [{"id": "dev-front", "name": "Framsidan", "name_by_user": None, "area_id": None}]
    ha_mock.entities = [
        {
            "entity_id": "sensor.framsidan_batteri",
            "device_class": "battery",
            "platform": "reolink",
            "device_id": "dev-front",
        },
        {
            "entity_id": "sensor.framsidan_batteritillstand",
            "device_class": None,
            "original_device_class": "enum",
            "platform": "reolink",
            "translation_key": "battery_state",
            "entity_category": "diagnostic",
            "capabilities": {"options": ["discharging", "charging", "chargecomplete"]},
            "device_id": "dev-front",
            "disabled_by": "integration",
        },
        {
            "entity_id": "sensor.framsidan_batteritemperatur",
            "device_class": "temperature",
            "platform": "reolink",
            "translation_key": "battery_temperature",
            "device_id": "dev-front",
        },
    ]
    ha_mock.states = {
        "sensor.framsidan_batteri": {"state": "64", "attributes": {"device_class": "battery"}},
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        first = {e.entity_id for e in await client.fetch_entities()}
        # Disabled this round, but switched on in the registry and flagged for a resync.
        assert "sensor.framsidan_batteritillstand" not in first
        assert "sensor.framsidan_batteritemperatur" not in first
        assert ha_mock.registry_updates == [{"entity_id": "sensor.framsidan_batteritillstand", "disabled_by": None}]
        assert client.pending_resync is True

        # HA has materialised it: the next fetch carries it with its current value, and nothing
        # is re-enabled.
        ha_mock.states["sensor.framsidan_batteritillstand"] = {
            "state": "charging",
            "attributes": {"device_class": "enum", "options": ["discharging", "charging", "chargecomplete"]},
        }
        client.pending_resync = False
        second = {e.entity_id: e for e in await client.fetch_entities()}
        state = second["sensor.framsidan_batteritillstand"]
        assert state.translation_key == "battery_state"
        assert state.value == "charging"
        assert state.device_name == "Framsidan"
        assert len(ha_mock.registry_updates) == 1
        assert client.pending_resync is False

        states = {row["entity_id"]: row for row in await client.fetch_states()}
        assert states["sensor.framsidan_batteritillstand"]["state"] == "charging"
        assert "sensor.framsidan_batteritemperatur" not in states
    finally:
        await client.close()


async def test_fetch_includes_number_select_config_with_attrs_and_device(ha_mock):
    ha_mock.devices = [{"id": "dev-cam1", "name": "Camera 1", "name_by_user": None, "area_id": None}]
    ha_mock.entities = [
        {
            "entity_id": "number.camera1_pir_sensitivity",
            "device_class": None,
            "platform": "reolink",
            "entity_category": "config",
            "device_id": "dev-cam1",
        },
        {
            "entity_id": "select.camera1_day_night",
            "device_class": None,
            "platform": "reolink",
            "entity_category": "config",
            "device_id": "dev-cam1",
        },
        {"entity_id": "sensor.temperature", "device_class": "temperature", "platform": "zha"},
    ]
    ha_mock.states = {
        "number.camera1_pir_sensitivity": {
            "state": "35",
            "attributes": {"min": 0, "max": 50, "step": 1, "mode": "slider"},
        },
        "select.camera1_day_night": {
            "state": "Auto",
            "attributes": {"options": ["Auto", "Color", "Black&White"]},
        },
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        by_id = {e.entity_id: e for e in await client.fetch_entities()}
        assert "sensor.temperature" not in by_id  # non-config domain still excluded
        num = by_id["number.camera1_pir_sensitivity"]
        assert num.domain == "number"
        assert num.value == "35"
        assert num.attributes == {"min": 0, "max": 50, "step": 1, "mode": "slider"}
        assert num.device_id == "dev-cam1"
        assert num.device_name == "Camera 1"
        sel = by_id["select.camera1_day_night"]
        assert sel.domain == "select"
        assert sel.value == "Auto"
        assert sel.attributes == {"options": ["Auto", "Color", "Black&White"]}
    finally:
        await client.close()


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
        # siren + light domains are now ingested (outputs/deterrents); sensor still excluded.
        assert "siren.camera1_siren" in by_id
        assert by_id["siren.camera1_siren"].domain == "siren"
        assert "light.kitchen" in by_id
        assert by_id["light.kitchen"].domain == "light"
        assert "sensor.temperature" not in by_id
        # entity_category is forwarded so Argus can auto-hide config/diagnostic clutter.
        assert by_id["switch.camera1_ftp"].entity_category == "config"
        assert by_id["binary_sensor.kitchen_motion"].entity_category is None
    finally:
        await client.close()


def test_merge_candidates_appends_per_section_with_end_of_candidates():
    sdp = "v=0\r\ns=-\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=sendonly\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\n"
    merged = merge_candidates(
        sdp,
        [
            {"candidate": "candidate:1 1 udp 1 192.168.1.10 1 typ host", "sdpMLineIndex": 0},
            {"candidate": "candidate:2 1 udp 1 1.2.3.4 2 typ srflx", "sdpMLineIndex": 1},
            {"candidate": "", "sdpMLineIndex": 0},
        ],
    )
    lines = merged.split("\r\n")
    assert lines[2:6] == [
        "m=video 9 UDP/TLS/RTP/SAVPF 96",
        "a=sendonly",
        "a=candidate:1 1 udp 1 192.168.1.10 1 typ host",
        "a=end-of-candidates",
    ]
    assert lines[6:10] == [
        "m=audio 9 UDP/TLS/RTP/SAVPF 111",
        "a=sendonly",
        "a=candidate:2 1 udp 1 1.2.3.4 2 typ srflx",
        "a=end-of-candidates",
    ]
    assert merged.endswith("\r\n")
    assert merge_candidates(sdp, []) == sdp


async def test_webrtc_offer_collects_answer_and_trickled_candidates(ha_mock):
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        result = await client.webrtc_offer(
            "camera.living_room", "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n", candidate_window=0.2
        )
        assert result["session_id"] == "01MOCKSESSION"
        assert ha_mock.webrtc_offers == [
            {"entity_id": "camera.living_room", "offer": "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n"}
        ]
        assert "a=candidate:2 1 udp 1694498815 98.128.181.109 59174 typ srflx" in result["sdp"]
        assert "a=end-of-candidates" in result["sdp"]
        assert ha_mock.unsubscribed == []
        assert await client.webrtc_close("01MOCKSESSION") is True
        assert len(ha_mock.unsubscribed) == 1
        assert await client.webrtc_close("01MOCKSESSION") is False
    finally:
        await client.close()


async def test_webrtc_offer_error_event_raises_and_unsubscribes(ha_mock):
    ha_mock.webrtc_error = {"code": "webrtc_offer_failed", "message": "camera unavailable"}
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        with pytest.raises(RuntimeError, match="webrtc_offer_failed: camera unavailable"):
            await client.webrtc_offer("camera.living_room", "v=0\r\n", candidate_window=0.1)
        assert len(ha_mock.unsubscribed) == 1
    finally:
        await client.close()


async def test_webrtc_offer_times_out_without_answer(ha_mock):
    ha_mock.webrtc_answer_delay = 5.0
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        with pytest.raises(TimeoutError):
            await client.webrtc_offer("camera.living_room", "v=0\r\n", answer_timeout=0.3, candidate_window=0.1)
        assert len(ha_mock.unsubscribed) == 1
    finally:
        await client.close()


async def test_state_events_still_reach_on_event_while_a_webrtc_subscription_is_open(ha_mock):
    seen: list[dict] = []

    async def on_event(data):
        seen.append(data)

    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url, on_event=on_event)
    await client.connect()
    try:
        await client.webrtc_offer("camera.living_room", "v=0\r\n", candidate_window=0.1)
        await ha_mock.push_state_event("binary_sensor.kitchen_motion", "on")
        for _ in range(50):
            if seen:
                break
            await asyncio.sleep(0.02)
        assert seen and seen[0]["event"]["data"]["entity_id"] == "binary_sensor.kitchen_motion"
    finally:
        await client.close()


async def test_fetch_entities_skips_disabled_registry_entries(ha_mock):
    ha_mock.entities.append(
        {
            "entity_id": "camera.matsalen_balanserad",
            "device_class": None,
            "platform": "reolink",
            "friendly_name": "Matsalen Balanserad",
            "disabled_by": "integration",
        }
    )
    ha_mock.entities.append(
        {
            "entity_id": "camera.matsalen_lag_klarhet",
            "device_class": None,
            "platform": "reolink",
            "friendly_name": "Matsalen Låg klarhet",
            "disabled_by": None,
        }
    )
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        ids = {e.entity_id for e in await client.fetch_entities()}
        assert "camera.matsalen_lag_klarhet" in ids
        assert "camera.matsalen_balanserad" not in ids
    finally:
        await client.close()


async def test_fetch_entities_syncs_battery_sensors_with_level_and_skips_other_sensors(ha_mock):
    ha_mock.entities.append(
        {
            "entity_id": "sensor.camera1_batteri",
            "device_class": "battery",
            "platform": "reolink",
            "device_id": "dev-cam1",
        }
    )
    ha_mock.entities.append(
        {
            "entity_id": "sensor.camera1_temperature",
            "device_class": "temperature",
            "platform": "reolink",
            "device_id": "dev-cam1",
        }
    )
    ha_mock.states["sensor.camera1_batteri"] = {
        "state": "37",
        "attributes": {"device_class": "battery", "unit_of_measurement": "%"},
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = {e.entity_id: e for e in await client.fetch_entities()}
        assert "sensor.camera1_batteri" in entities
        assert entities["sensor.camera1_batteri"].domain == "sensor"
        assert entities["sensor.camera1_batteri"].value == "37"
        assert entities["sensor.camera1_batteri"].device_id == "dev-cam1"
        assert "sensor.camera1_temperature" not in entities
        states = {s["entity_id"]: s for s in await client.fetch_states()}
        assert states["sensor.camera1_batteri"]["state"] == "37"
        assert states["sensor.camera1_batteri"]["attributes"]["device_class"] == "battery"
    finally:
        await client.close()
