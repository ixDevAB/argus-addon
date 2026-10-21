import asyncio

import pytest

from argus_addon.ha_client import HaClient, history_changes, history_reaches, merge_candidates, tracker_from_state


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


async def test_fetch_collects_the_devices_behind_the_entities_and_their_hubs(ha_mock):
    ha_mock.devices = [
        {
            "id": "dev-hub",
            "name": "Living Room",
            "manufacturer": "Reolink",
            "model": "Reolink Home Hub",
            "sw_version": "v3.3.0.601",
            "hw_version": "BASE_WENNT6NA5",
            "area_id": "vardagsrum",
            "via_device_id": None,
        },
        {
            "id": "dev-front",
            "name": "Framsidan",
            "manufacturer": "Reolink",
            "model": "Argus 4 Pro",
            "sw_version": "v3.0.0.4348",
            "hw_version": None,
            "area_id": None,
            "via_device_id": "dev-hub",
        },
        {
            "id": "dev-unrelated",
            "name": "Printer",
            "manufacturer": "HP",
            "model": "X",
            "sw_version": None,
            "hw_version": None,
            "area_id": None,
            "via_device_id": None,
        },
    ]
    ha_mock.entities = [
        {
            "entity_id": "camera.framsidan_lag_klarhet",
            "device_class": None,
            "platform": "reolink",
            "device_id": "dev-front",
        },
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        await client.fetch_entities()
        devices = {d.id: d for d in client.devices}
        assert set(devices) == {"dev-front", "dev-hub"}
        assert devices["dev-front"].model == "Argus 4 Pro"
        assert devices["dev-front"].via_device_id == "dev-hub"
        assert devices["dev-hub"].sw_version == "v3.3.0.601"
        assert devices["dev-hub"].hw_version == "BASE_WENNT6NA5"
    finally:
        await client.close()


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


async def test_fetch_forwards_each_integrations_iot_class(ha_mock):
    ha_mock.iot_classes = {"yale": "cloud_push", "matter": "local_push"}
    ha_mock.entities = [
        {"entity_id": "binary_sensor.ytterdorr_dorr", "device_class": "door", "platform": "yale"},
        {"entity_id": "binary_sensor.hall_ingang_dorr", "device_class": "door", "platform": "matter"},
        {"entity_id": "binary_sensor.hall_pir", "device_class": "motion", "platform": "custom_thing"},
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = {e.entity_id: e for e in await client.fetch_entities()}
        assert entities["binary_sensor.ytterdorr_dorr"].iot_class == "cloud_push"
        assert entities["binary_sensor.hall_ingang_dorr"].iot_class == "local_push"
        assert entities["binary_sensor.hall_pir"].iot_class is None
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
        with pytest.raises(TimeoutError, match=r"^camera_no_answer: no WebRTC answer from camera\.living_room"):
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


async def test_state_subscription_comes_back_by_itself_after_the_link_drops(ha_mock):
    seen: list[dict] = []

    async def on_event(data):
        seen.append(data)

    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url, on_event=on_event)
    await client.connect()
    try:
        await ha_mock.drop_connections()
        # No request is made here on purpose: the alarm path must not depend on one.
        for _ in range(100):
            if client._ws_live() and len(ha_mock._sockets) == 1:
                break
            await asyncio.sleep(0.05)
        assert client._ws_live()
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


def test_tracker_from_state_reads_persons_and_device_trackers_only():
    person = tracker_from_state(
        {
            "entity_id": "person.alexander",
            "state": "home",
            "attributes": {"friendly_name": "Alexander", "device_trackers": ["device_tracker.iphone", 7]},
            "last_changed": "2026-10-20T07:00:00Z",
        }
    )
    assert person is not None
    assert person.kind == "person"
    assert person.name == "Alexander"
    assert person.device_trackers == ["device_tracker.iphone"]
    assert person.changed_at.isoformat() == "2026-10-20T07:00:00+00:00"
    router = tracker_from_state(
        {
            "entity_id": "device_tracker.okand",
            "state": "home",
            "attributes": {"source_type": "router", "mac": "1a:e8:29:53:59:3a"},
        }
    )
    assert router is not None
    assert router.source_type == "router"
    assert router.mac == "1A:E8:29:53:59:3A"
    assert person.mac is None
    assert router.name is None
    assert tracker_from_state({"entity_id": "binary_sensor.door", "state": "on"}) is None
    assert tracker_from_state({"entity_id": "person.nobody", "state": None}) is None


def test_tracker_from_state_keeps_when_a_scanner_last_reached_the_device():
    def gone(reachable):
        return tracker_from_state(
            {
                "entity_id": "device_tracker.milli",
                "state": "not_home",
                "attributes": {"source_type": "router", "last_time_reachable": reachable},
                "last_changed": "2026-10-20T17:02:00+00:00",
            }
        )

    assert gone("2026-10-20T18:30:12+02:00").last_seen_at.isoformat() == "2026-10-20T18:30:12+02:00"
    assert gone("2026-10-20T18:30:12").last_seen_at is None, "a time without a zone is not trusted"
    assert gone("soon").last_seen_at is None
    assert gone(None).last_seen_at is None


async def test_scanner_trackers_the_integration_disabled_are_switched_on(ha_mock):
    ha_mock.entities = [
        {"entity_id": "device_tracker.nmap_phone", "platform": "nmap_tracker", "disabled_by": "integration"},
        {"entity_id": "device_tracker.switched_off", "platform": "nmap_tracker", "disabled_by": "user"},
        {"entity_id": "binary_sensor.door", "platform": "zha", "disabled_by": "integration", "device_class": "door"},
    ]
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        await client.fetch_entities()
        assert ha_mock.registry_updates == [{"entity_id": "device_tracker.nmap_phone", "disabled_by": None}]
        assert client.pending_resync is True
    finally:
        await client.close()


async def test_a_new_scanner_tracker_is_switched_on_when_ha_makes_it(ha_mock):
    ha_mock.entities = [
        {"entity_id": "device_tracker.visitor", "platform": "nmap_tracker", "disabled_by": "integration"},
        {"entity_id": "device_tracker.kept_off", "platform": "nmap_tracker", "disabled_by": "user"},
    ]

    async def ignore(_data):
        return None

    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url, on_event=ignore)
    await client.connect()
    try:
        for entity_id in ("device_tracker.visitor", "device_tracker.kept_off", "light.lamp"):
            await ha_mock.push_event(
                {"event_type": "entity_registry_updated", "data": {"action": "create", "entity_id": entity_id}}
            )
        for _ in range(50):
            if ha_mock.registry_updates:
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.1)
        assert ha_mock.registry_updates == [{"entity_id": "device_tracker.visitor", "disabled_by": None}]
    finally:
        await client.close()


def _ts(hh, mm, ss=0):
    from datetime import UTC, datetime

    return datetime(2026, 10, 20, hh - 2, mm, ss, tzinfo=UTC).timestamp()


def _since():
    from datetime import UTC, datetime

    return datetime(2026, 10, 20, 4, 0, tzinfo=UTC)


def test_a_scanned_phone_left_when_a_scan_last_reached_it():
    # Unavailable since a scanner reload, so Home Assistant says nothing of its source.
    phone = tracker_from_state({"entity_id": "device_tracker.apple_64_e3_d0", "state": "unavailable", "attributes": {}})
    reached = "2026-10-20T18:40:50+02:00"
    rows = [
        {"s": "home", "a": {"last_time_reachable": "2026-10-20T18:25:59+02:00"}, "lu": _ts(6, 0)},
        {"s": "home", "a": {"last_time_reachable": "2026-10-20T18:36:00+02:00"}, "lu": _ts(18, 36, 0)},
        {"s": "unavailable", "a": {}, "lu": _ts(18, 36, 30)},
        {"s": "home", "a": {"last_time_reachable": "2026-10-20T18:36:36+02:00"}, "lu": _ts(18, 36, 36)},
        {"s": "home", "a": {"last_time_reachable": reached}, "lc": _ts(18, 36, 36), "lu": _ts(18, 40, 50)},
        {"s": "not_home", "a": {"last_time_reachable": reached}, "lu": _ts(19, 14, 35)},
        {"s": "unavailable", "a": {}, "lu": _ts(20, 11, 5)},
    ]
    got = history_changes(phone, rows, _since())
    assert [
        (t.state, t.changed_at.isoformat(), t.last_seen_at.isoformat() if t.last_seen_at else None) for t in got
    ] == [
        ("not_home", "2026-10-20T17:14:35+00:00", "2026-10-20T16:40:50+00:00"),
    ], "the state at the start is no news, unavailable is nothing, and gone is dated by the last scan"
    newcomer = history_changes(phone, [{"s": "home", "lu": _ts(9, 30)}], _since())
    assert [(t.state, t.changed_at.strftime("%H:%M")) for t in newcomer] == [("home", "07:30")], (
        "a device first seen after the start came"
    )


def test_a_location_comes_and_goes_as_it_says():
    app = tracker_from_state(
        {"entity_id": "device_tracker.alexanders_iphone", "state": "not_home", "attributes": {"source_type": "gps"}}
    )
    rows = [
        {"s": "not_home", "lu": _ts(6, 0)},
        {"s": "home", "lu": _ts(18, 38, 33)},
        {"s": "not_home", "lu": _ts(18, 43, 28)},
        {"s": "home", "lu": _ts(19, 59, 11)},
        {"s": "not_home", "lu": _ts(20, 2, 55)},
    ]
    got = history_changes(app, rows, _since())
    assert [(t.state, t.changed_at.strftime("%H:%M:%S"), t.last_seen_at) for t in got] == [
        ("home", "16:38:33", None),
        ("not_home", "16:43:28", None),
        ("home", "17:59:11", None),
        ("not_home", "18:02:55", None),
    ]


async def test_the_history_of_every_tracker_comes_back_oldest_first(ha_mock):
    ha_mock.history = {
        "device_tracker.phone": [{"s": "home", "lu": _ts(6, 0)}, {"s": "not_home", "lu": _ts(19, 0)}],
        "person.milli": [{"s": "home", "lu": _ts(6, 0)}, {"s": "not_home", "lu": _ts(18, 44)}],
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        trackers = [
            tracker_from_state(
                {"entity_id": "device_tracker.phone", "state": "not_home", "attributes": {"source_type": "router"}}
            ),
            tracker_from_state({"entity_id": "person.milli", "state": "not_home", "attributes": {}}),
        ]
        got, reaches = await client.fetch_tracker_history(_since(), trackers)
        assert [t.entity_id for t in got] == ["person.milli", "device_tracker.phone"]
        assert reaches == [], "a phone with no address is no device on the network"
        assert ha_mock.history_requests[0]["entity_ids"] == ["device_tracker.phone"]
        assert ha_mock.history_requests[0]["no_attributes"] is False, (
            "the departure row carries when a scan last reached the device"
        )
    finally:
        await client.close()


def test_a_walk_inside_the_scanners_grace_shows_as_a_gap_in_its_reaches():
    vidar = tracker_from_state(
        {
            "entity_id": "device_tracker.nmap_tracker_56_c9_55_3e_5b_be",
            "state": "home",
            "attributes": {"source_type": "router", "mac": "56:c9:55:3e:5b:be"},
        }
    )

    def scan(hh, mm, ss, state="home"):
        at = f"2026-10-20T{hh:02d}:{mm:02d}:{ss:02d}+02:00"
        return {"s": state, "a": {"last_time_reachable": at, "ip": "192.168.184.175"}, "lu": _ts(hh, mm, ss)}

    rows = [scan(14, 38, 22), scan(14, 40, 24), scan(14, 42, 23), scan(14, 44, 24), scan(15, 18, 24), scan(15, 20, 24)]
    got = history_reaches(vidar, rows, _since())
    assert [(s.mac, s.since.strftime("%H:%M:%S"), s.at.strftime("%H:%M:%S")) for s in got] == [
        ("56:C9:55:3E:5B:BE", "12:38:22", "12:44:24"),
        ("56:C9:55:3E:5B:BE", "13:18:24", "13:20:24"),
    ], "one sighting per stretch, first scan to last, so the half hour out the door is a gap"
    assert got[0].ip == "192.168.184.175"
    app = tracker_from_state({"entity_id": "device_tracker.app", "state": "home", "attributes": {"source_type": "gps"}})
    assert history_reaches(app, rows, _since()) == [], "a phone's location is no device on the network"


async def test_fetch_sends_siren_features_buttons_and_config_switch_values(ha_mock):
    ha_mock.devices = [{"id": "dev-front", "name": "Framsidan", "name_by_user": None, "area_id": None}]
    ha_mock.entities = [
        {
            "entity_id": "siren.framsidan_siren",
            "platform": "reolink",
            "translation_key": "siren",
            "device_id": "dev-front",
        },
        {
            "entity_id": "button.framsidan_forsiren",
            "platform": "reolink",
            "translation_key": "pre_siren",
            "device_id": "dev-front",
        },
        {
            "entity_id": "switch.framsidan_siren_vid_handelse",
            "platform": "reolink",
            "translation_key": "siren_on_event",
            "entity_category": "config",
            "device_id": "dev-front",
        },
        {"entity_id": "switch.hall_plug", "platform": "zha"},
    ]
    ha_mock.states = {
        "siren.framsidan_siren": {"state": "off", "attributes": {"supported_features": 27}},
        "button.framsidan_forsiren": {"state": "unknown", "attributes": {}},
        "switch.framsidan_siren_vid_handelse": {"state": "on", "attributes": {}},
        "switch.hall_plug": {"state": "off", "attributes": {}},
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = {e.entity_id: e for e in await client.fetch_entities()}
        assert entities["siren.framsidan_siren"].attributes == {"supported_features": 27}
        button = entities["button.framsidan_forsiren"]
        assert button.domain == "button"
        assert button.translation_key == "pre_siren"
        assert entities["switch.framsidan_siren_vid_handelse"].value == "on"
        assert entities["switch.hall_plug"].value is None
    finally:
        await client.close()


async def test_a_lock_is_synced_and_its_state_says_who_turned_it(ha_mock):
    ha_mock.devices = [{"id": "dev-door", "name": "Ytterdörr", "name_by_user": None, "area_id": None}]
    ha_mock.entities = [
        {"entity_id": "lock.ytterdorr", "platform": "yale", "device_id": "dev-door"},
        {
            "entity_id": "binary_sensor.ytterdorr_dorr",
            "platform": "yale",
            "device_class": "door",
            "device_id": "dev-door",
        },
    ]
    ha_mock.states = {
        "lock.ytterdorr": {
            "state": "unlocked",
            "attributes": {"changed_by": "Vidar", "battery_level": 92, "supported_features": 1},
        },
        "binary_sensor.ytterdorr_dorr": {"state": "off", "attributes": {"device_class": "door"}},
    }
    client = HaClient(supervisor_token="test-token", ws_url=ha_mock.url)
    await client.connect()
    try:
        entities = {e.entity_id: e for e in await client.fetch_entities()}
        assert entities["lock.ytterdorr"].domain == "lock"
        assert entities["lock.ytterdorr"].device_id == "dev-door"
        states = {row["entity_id"]: row for row in await client.fetch_states()}
        assert states["lock.ytterdorr"]["state"] == "unlocked"
        assert states["lock.ytterdorr"]["attributes"] == {"changed_by": "Vidar", "battery_level": 92}
    finally:
        await client.close()
