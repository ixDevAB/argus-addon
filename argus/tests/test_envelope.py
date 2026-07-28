import json

import pytest
from pydantic import ValidationError

from argus_addon.envelope import (
    Ack,
    Cmd,
    EntityList,
    EnvelopeAdapter,
    Heartbeat,
    Hello,
    PairCode,
    PairEnvelopeAdapter,
    PairToken,
    State,
)


def test_heartbeat_roundtrip():
    raw = {"type": "hb", "ts": "2026-05-28T12:00:00+00:00"}
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, Heartbeat)
    assert parsed.ts.isoformat().startswith("2026-05-28T12:00:00")


def test_hello_roundtrip():
    raw = {"type": "hello", "addon_version": "0.1.0", "ha_version": "2026.5.0"}
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, Hello)
    assert parsed.addon_version == "0.1.0"
    assert parsed.ha_version == "2026.5.0"


def test_entity_list_roundtrip():
    raw = {
        "type": "entity_list",
        "entities": [
            {
                "entity_id": "binary_sensor.kitchen_motion",
                "device_class": "motion",
                "domain": "binary_sensor",
                "friendly_name": "Kitchen Motion",
            },
            {"entity_id": "switch.siren_living", "device_class": None, "domain": "switch"},
        ],
    }
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, EntityList)
    assert len(parsed.entities) == 2
    assert parsed.entities[0].entity_id == "binary_sensor.kitchen_motion"
    assert parsed.entities[0].device_class == "motion"
    assert parsed.entities[1].device_class is None


def test_state_roundtrip():
    raw = {
        "type": "state",
        "entity_id": "binary_sensor.kitchen_motion",
        "state": "on",
        "at": "2026-05-28T12:00:00+00:00",
    }
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, State)
    assert parsed.state == "on"


def test_cmd_roundtrip():
    raw = {
        "type": "cmd",
        "id": "0190a000-7000-7000-8000-000000000001",
        "op": "call_service",
        "args": {"domain": "switch", "service": "turn_on", "service_data": {"entity_id": "switch.siren_living"}},
    }
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, Cmd)
    assert parsed.op == "call_service"
    assert parsed.args["service"] == "turn_on"


def test_ack_roundtrip():
    raw = {"type": "ack", "id": "0190a000-7000-7000-8000-000000000001"}
    parsed = EnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, Ack)
    assert parsed.duplicate is None
    raw_dup = {"type": "ack", "id": "0190a000-7000-7000-8000-000000000001", "duplicate": True}
    parsed_dup = EnvelopeAdapter.validate_python(raw_dup)
    assert isinstance(parsed_dup, Ack)
    assert parsed_dup.duplicate is True


def test_invalid_hb_missing_ts():
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python({"type": "hb"})


def test_invalid_cmd_missing_required_fields():
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python({"type": "cmd"})


def test_invalid_unknown_type():
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python({"type": "wat", "ts": "2026-05-28T12:00:00+00:00"})


def test_invalid_entity_list_domain():
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python(
            {
                "type": "entity_list",
                "entities": [{"entity_id": "climate.kitchen", "device_class": None, "domain": "climate"}],
            }
        )


def test_accepts_light_entity_list_domain():
    env = EnvelopeAdapter.validate_python(
        {
            "type": "entity_list",
            "entities": [{"entity_id": "light.kitchen", "device_class": None, "domain": "light"}],
        }
    )
    assert env.entities[0].domain == "light"


def test_cmd_invalid_op():
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python(
            {
                "type": "cmd",
                "id": "0190a000-7000-7000-8000-000000000001",
                "op": "execute_shell",
                "args": {},
            }
        )


def test_pair_code_roundtrip():
    raw = {
        "type": "pair_code",
        "session_id": "01939999-0000-7000-8000-000000000001",
        "code": "012345",
        "expires_at": "2026-05-28T12:10:00+00:00",
    }
    parsed = PairEnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, PairCode)
    assert parsed.code == "012345"
    assert parsed.session_id == "01939999-0000-7000-8000-000000000001"


def test_pair_token_roundtrip():
    raw = {"type": "pair_token", "install_token": "install-token-secret"}
    parsed = PairEnvelopeAdapter.validate_python(raw)
    assert isinstance(parsed, PairToken)
    assert parsed.install_token == "install-token-secret"


def test_pair_union_is_separate_from_alarm_union():
    # Pair frames are rejected by the alarm Envelope adapter.
    with pytest.raises(ValidationError):
        EnvelopeAdapter.validate_python({"type": "pair_token", "install_token": "x"})
    # Alarm frames are rejected by the pair adapter.
    with pytest.raises(ValidationError):
        PairEnvelopeAdapter.validate_python({"type": "hb", "ts": "2026-05-28T12:00:00+00:00"})


def test_envelope_json_serialization():
    hb = Heartbeat.model_validate({"type": "hb", "ts": "2026-05-28T12:00:00+00:00"})
    payload = json.loads(hb.model_dump_json())
    assert payload["type"] == "hb"
    assert "ts" in payload


async def test_cloud_mock_records_hello(cloud_mock):
    import websockets

    async with websockets.connect(cloud_mock.url) as ws:
        await ws.send(json.dumps({"type": "hello", "addon_version": "0.1.0", "ha_version": "2026.5.0"}))
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "hello")
        assert env["addon_version"] == "0.1.0"
        assert env["ha_version"] == "2026.5.0"


async def test_ha_mock_returns_entity_registry(ha_mock):
    import aiohttp

    async with aiohttp.ClientSession() as session, session.ws_connect(ha_mock.url) as ws:
        first = await ws.receive_json()
        assert first["type"] == "auth_required"
        await ws.send_json({"type": "auth", "access_token": "test-supervisor-token"})
        auth_ok = await ws.receive_json()
        assert auth_ok["type"] == "auth_ok"
        await ws.send_json({"id": 1, "type": "config/entity_registry/list"})
        reply = await ws.receive_json()
        assert reply["success"] is True
        ids = {e["entity_id"] for e in reply["result"]}
        assert "binary_sensor.kitchen_motion" in ids
        assert "switch.siren_living" in ids
        motion = next(e for e in reply["result"] if e["entity_id"] == "binary_sensor.kitchen_motion")
        assert motion["device_class"] == "motion"
    assert ha_mock.auth_token == "test-supervisor-token"
