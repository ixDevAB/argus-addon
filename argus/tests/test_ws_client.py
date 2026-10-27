import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

from argus_addon import ws_client
from argus_addon.envelope import EntityRef, State
from argus_addon.idempotency import Idempotency


class FakeHaClient:
    def link_up(self):
        return True

    def __init__(self):
        self.calls: list[dict] = []
        self._version = "2026.5.0"
        self.entities: list = []
        self.states: list = []
        self.trackers: list = []
        self.link_restored = asyncio.Event()

    def version(self) -> str:
        return self._version

    async def fetch_entities(self):
        return list(self.entities)

    async def fetch_states(self):
        return list(self.states)

    async def fetch_trackers(self):
        return list(self.trackers)

    async def call_service(self, domain, service, service_data):
        self.calls.append({"domain": domain, "service": service, "service_data": service_data})
        if getattr(self, "service_error", None):
            raise RuntimeError(self.service_error)

    async def webrtc_offer(self, entity_id, sdp):
        self.calls.append({"webrtc_offer": entity_id, "sdp": sdp})
        if entity_id == "camera.silent":
            raise TimeoutError
        await asyncio.sleep(0.2)
        return {"session_id": "HA-1", "sdp": "v=0\r\na=answer\r\n"}

    async def webrtc_close(self, session_id):
        self.calls.append({"webrtc_close": session_id})
        return True


async def _start_runner(
    cloud_mock,
    tmp_path,
    *,
    max_backoff=1.0,
    initial_backoff=0.05,
    ha_client=None,
    entities=None,
    entity_fetch_timeout=10.0,
    entity_retry_interval=5.0,
):
    token_path = tmp_path / "token.txt"
    token_path.write_text("test-token-xyz")
    if ha_client is None:
        ha_client = FakeHaClient()
    if entities is not None:
        ha_client.entities = entities
    send_queue: asyncio.Queue = asyncio.Queue()
    idempotency = Idempotency(":memory:")
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        ws_client.run(
            token_path=token_path,
            cloud_url=cloud_mock.url,
            ha_client=ha_client,
            send_queue=send_queue,
            idempotency=idempotency,
            use_tls=False,
            max_backoff=max_backoff,
            initial_backoff=initial_backoff,
            heartbeat_interval=5.0,
            entity_fetch_timeout=entity_fetch_timeout,
            entity_retry_interval=entity_retry_interval,
            stop_event=stop_event,
        )
    )
    return task, ha_client, idempotency, send_queue, stop_event, token_path


async def _stop(task, stop_event):
    stop_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


async def test_the_token_goes_in_a_header_never_the_url(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        path, authorization = cloud_mock.addon_requests[-1]
        assert "test-token-xyz" not in path
        assert authorization == "Bearer test-token-xyz"
    finally:
        await _stop(task, stop)


async def test_handshake_sends_hello(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        assert env["addon_version"] == "0.1.0"
        assert env["ha_version"] == "2026.5.0"
    finally:
        await _stop(task, stop)


async def test_unpair_cmd_acks_drops_the_token_and_returns(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, token_path = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {"type": "cmd", "id": "01939999-0000-7000-8000-00000000aa01", "op": "unpair", "args": {}}
        await cloud_mock.send_to_client(cmd)
        await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        await asyncio.wait_for(task, timeout=3.0)
        assert not token_path.exists()
    finally:
        await _stop(task, stop)


async def test_rotated_token_is_saved_acked_and_used_on_the_next_connect(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, token_path = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-00000000aa02",
            "op": "rotate_token",
            "args": {"install_token": "fresh-token-abc"},
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack.get("error") is None
        for _ in range(100):
            if cloud_mock.addon_requests[-1][1] == "Bearer fresh-token-abc":
                break
            await asyncio.sleep(0.03)
        assert cloud_mock.addon_requests[-1][1] == "Bearer fresh-token-abc"
        assert token_path.read_text() == "fresh-token-abc"
        assert token_path.stat().st_mode & 0o777 == 0o600
        assert not token_path.with_name(token_path.name + ".new").exists()
        assert not task.done()
    finally:
        await _stop(task, stop)


async def test_rotation_without_a_token_is_refused_and_keeps_the_current_one(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, token_path = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {"type": "cmd", "id": "01939999-0000-7000-8000-00000000aa03", "op": "rotate_token", "args": {}}
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack["error"] == "no install_token"
        assert token_path.read_text() == "test-token-xyz"
        assert len(cloud_mock.addon_requests) == 1
    finally:
        await _stop(task, stop)


async def test_rejected_token_drops_the_token_and_returns(cloud_mock, tmp_path):
    cloud_mock.reject_status = 401
    task, _ha, _idem, _q, stop, token_path = await _start_runner(cloud_mock, tmp_path)
    try:
        await asyncio.wait_for(task, timeout=3.0)
        assert not token_path.exists()
    finally:
        await _stop(task, stop)


async def test_other_handshake_rejections_keep_the_token_and_retry(cloud_mock, tmp_path):
    cloud_mock.reject_status = 503
    task, _ha, _idem, _q, stop, token_path = await _start_runner(cloud_mock, tmp_path)
    try:
        await asyncio.sleep(0.3)
        assert not task.done()
        assert token_path.exists()
    finally:
        await _stop(task, stop)


async def test_cmd_call_service_relays_to_ha(cloud_mock, tmp_path):
    task, ha, idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-000000000001",
            "op": "call_service",
            "args": {
                "domain": "switch",
                "service": "turn_on",
                "service_data": {"entity_id": "switch.siren_living"},
            },
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack.get("duplicate") in (None, False)
        assert len(ha.calls) == 1
        assert ha.calls[0]["domain"] == "switch"
        assert ha.calls[0]["service"] == "turn_on"
        assert idem.seen(cmd["id"]) is True
    finally:
        await _stop(task, stop)


async def test_cmd_call_service_ha_refused_is_acked_with_its_reason(cloud_mock, tmp_path):
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    ha.service_error = "service_not_found: Service siren.turn_on not found"
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-000000000009",
            "op": "call_service",
            "args": {"domain": "siren", "service": "turn_on", "entity_id": "siren.hall"},
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack.get("error") == "service_not_found: Service siren.turn_on not found"
    finally:
        await _stop(task, stop)


async def test_a_state_held_while_the_link_was_down_goes_out_with_its_age(cloud_mock, tmp_path):
    task, _ha, _idem, queue, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        held = datetime.now(UTC) - timedelta(minutes=10)
        await queue.put(State(type="state", entity_id="binary_sensor.hall_motion", state="on", at=held))
        frame = await cloud_mock.wait_for(
            lambda e: e.get("type") == "state" and e.get("entity_id") == "binary_sensor.hall_motion", timeout=3.0
        )
        assert 590_000 <= frame["age_ms"] <= 620_000
    finally:
        await _stop(task, stop)


async def test_idempotent_command_replay(cloud_mock, tmp_path):
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-000000000002",
            "op": "call_service",
            "args": {
                "domain": "switch",
                "service": "turn_on",
                "service_data": {"entity_id": "switch.siren_living"},
            },
        }
        await cloud_mock.send_to_client(cmd)
        await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"] and not e.get("duplicate"),
            timeout=3.0,
        )
        await cloud_mock.send_to_client(cmd)
        await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"] and e.get("duplicate") is True,
            timeout=3.0,
        )
        assert len(ha.calls) == 1
    finally:
        await _stop(task, stop)


async def test_reconnect_with_jitter(cloud_mock, tmp_path, monkeypatch):
    jitter_calls: list[tuple[float, float]] = []

    real_uniform = __import__("random").uniform

    def fake_uniform(a, b):
        jitter_calls.append((a, b))
        return real_uniform(0, min(b, 0.05))

    monkeypatch.setattr("argus_addon.ws_client.random.uniform", fake_uniform)

    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, initial_backoff=0.2, max_backoff=1.0)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        await cloud_mock.drop_connections()

        async def wait_for_second_hello(deadline: float):
            while True:
                hellos = sum(1 for e in cloud_mock.received if e.get("type") == "hello")
                if hellos >= 2:
                    return
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"no second hello in time; received hellos={hellos}")
                await asyncio.sleep(0.05)

        deadline = asyncio.get_event_loop().time() + 3.0
        await wait_for_second_hello(deadline)
        assert len(jitter_calls) >= 1
        for a, b in jitter_calls:
            assert a == 0
            assert b <= 1.0
    finally:
        await _stop(task, stop)


async def test_entity_list_sent_on_connect(cloud_mock, tmp_path):
    ents = [
        EntityRef(
            entity_id="binary_sensor.front_door",
            device_class="door",
            domain="binary_sensor",
            friendly_name="Front Door",
        )
    ]
    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, entities=ents)
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "entity_list", timeout=3.0)
        ids = [x["entity_id"] for x in env["entities"]]
        assert "binary_sensor.front_door" in ids
    finally:
        await _stop(task, stop)


async def test_states_snapshot_sent_on_connect(cloud_mock, tmp_path):
    ha = FakeHaClient()
    ha.states = [{"entity_id": "binary_sensor.altandorr", "state": "on", "attributes": {"device_class": "door"}}]
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, ha_client=ha)
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "states", timeout=3.0)
        assert env["states"][0]["entity_id"] == "binary_sensor.altandorr"
        assert env["states"][0]["state"] == "on"
    finally:
        await _stop(task, stop)


async def test_presence_sent_in_full_on_connect(cloud_mock, tmp_path):
    from datetime import UTC, datetime

    from argus_addon.envelope import Tracker

    ha = FakeHaClient()
    ha.trackers = [
        Tracker(
            entity_id="person.alexander",
            kind="person",
            name="Alexander",
            state="home",
            device_trackers=["device_tracker.alexanders_iphone"],
            changed_at=datetime(2026, 10, 20, 7, 0, tzinfo=UTC),
        )
    ]
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, ha_client=ha)
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "presence", timeout=3.0)
        assert env["full"] is True
        assert env["trackers"][0]["entity_id"] == "person.alexander"
        assert env["trackers"][0]["device_trackers"] == ["device_tracker.alexanders_iphone"]
    finally:
        await _stop(task, stop)


async def test_states_snapshot_resent_when_ha_link_is_restored(cloud_mock, tmp_path):
    ha = FakeHaClient()
    ha.states = [
        {"entity_id": "binary_sensor.hall_upp", "state": "unavailable", "attributes": {"device_class": "door"}}
    ]
    task, _, _, _, stop_event, _ = await _start_runner(cloud_mock, tmp_path, ha_client=ha)
    try:
        first = await cloud_mock.wait_for(lambda e: e.get("type") == "states", timeout=3.0)
        assert first["states"][0]["state"] == "unavailable"
        cloud_mock.received.clear()
        ha.states = [{"entity_id": "binary_sensor.hall_upp", "state": "off", "attributes": {"device_class": "door"}}]
        ha.link_restored.set()
        again = await cloud_mock.wait_for(lambda e: e.get("type") == "states", timeout=3.0)
        assert again["states"][0]["state"] == "off"
        assert not ha.link_restored.is_set()
    finally:
        await _stop(task, stop_event)


async def test_resync_cmd_refetches_and_resends_entity_list(cloud_mock, tmp_path):
    ha = FakeHaClient()
    ha.entities = [
        EntityRef(entity_id="binary_sensor.front_door", device_class="door", domain="binary_sensor"),
    ]
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, ha_client=ha)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "entity_list", timeout=3.0)
        # HA registry changed since connect: a new window sensor now exists.
        ha.entities = [
            EntityRef(entity_id="binary_sensor.balcony", device_class="window", domain="binary_sensor"),
        ]
        cmd = {"type": "cmd", "id": "01939999-0000-7000-8000-0000000000aa", "op": "resync", "args": {}}
        await cloud_mock.send_to_client(cmd)
        env = await cloud_mock.wait_for(
            lambda e: (
                e.get("type") == "entity_list"
                and any(x["entity_id"] == "binary_sensor.balcony" for x in e.get("entities", []))
            ),
            timeout=3.0,
        )
        assert [x["entity_id"] for x in env["entities"]] == ["binary_sensor.balcony"]
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack.get("error") is None
    finally:
        await _stop(task, stop)


async def test_slow_entity_fetch_does_not_block_link(cloud_mock, tmp_path):
    # A hanging HA entity fetch must NOT starve the heartbeat / read loop: the
    # cloud link must still come up and relay commands.
    hang = asyncio.Event()

    class HangingHaClient(FakeHaClient):
        async def fetch_entities(self):
            await hang.wait()
            return []

    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path, ha_client=HangingHaClient())
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-000000000099",
            "op": "call_service",
            "args": {
                "domain": "switch",
                "service": "turn_on",
                "service_data": {"entity_id": "switch.siren_living"},
            },
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack.get("error") is None
        assert len(ha.calls) == 1
    finally:
        hang.set()
        await _stop(task, stop)


async def test_entity_fetch_retries_until_success(cloud_mock, tmp_path):
    attempts = {"n": 0}

    class FlakyHaClient(FakeHaClient):
        async def fetch_entities(self):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("ha not ready")
            return [
                EntityRef(
                    entity_id="binary_sensor.kitchen",
                    device_class="motion",
                    domain="binary_sensor",
                )
            ]

    task, _ha, _idem, _q, stop, _tp = await _start_runner(
        cloud_mock, tmp_path, ha_client=FlakyHaClient(), entity_retry_interval=0.05
    )
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "entity_list", timeout=3.0)
        ids = [x["entity_id"] for x in env["entities"]]
        assert "binary_sensor.kitchen" in ids
        assert attempts["n"] >= 2
    finally:
        await _stop(task, stop)


async def test_webrtc_offer_acks_with_result_without_blocking_other_commands(cloud_mock, tmp_path):
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        offer = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-0000000000b1",
            "op": "webrtc_offer",
            "args": {"entity_id": "camera.living_room", "sdp": "v=0\r\n"},
        }
        siren = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-0000000000b2",
            "op": "call_service",
            "args": {"domain": "siren", "service": "turn_on", "entity_id": "siren.cam"},
        }
        await cloud_mock.send_to_client(offer)
        await cloud_mock.send_to_client(siren)
        siren_ack = await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == siren["id"], timeout=3.0
        )
        offer_acked = any(e.get("type") == "ack" and e.get("id") == offer["id"] for e in cloud_mock.received)
        assert siren_ack["error"] is None
        assert offer_acked is False
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == offer["id"], timeout=3.0)
        assert ack["result"] == {"session_id": "HA-1", "sdp": "v=0\r\na=answer\r\n"}
        assert ack["error"] is None
        close = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-0000000000b3",
            "op": "webrtc_close",
            "args": {"session_id": "HA-1"},
        }
        await cloud_mock.send_to_client(close)
        close_ack = await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == close["id"], timeout=3.0
        )
        assert close_ack["result"] == {"closed": True}
        assert {"webrtc_close": "HA-1"} in ha.calls
    finally:
        await _stop(task, stop)


async def test_webrtc_offer_rejects_non_camera_entity(cloud_mock, tmp_path):
    task, ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-0000000000b4",
            "op": "webrtc_offer",
            "args": {"entity_id": "switch.siren", "sdp": "v=0\r\n"},
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert "camera entity_id" in ack["error"]
        assert ack["result"] is None
        assert not any("webrtc_offer" in c for c in ha.calls)
    finally:
        await _stop(task, stop)


async def test_state_forwarder_passes_number_and_select_settings():
    from argus_addon.__main__ import _build_state_forwarder

    queue: asyncio.Queue = asyncio.Queue()
    forward = _build_state_forwarder(queue)
    for entity_id, state in (
        ("number.living_room_pir_kanslighet_3", "90"),
        ("select.living_room_dag_natt_lage_3", "auto"),
    ):
        await forward(
            {
                "type": "event",
                "event": {
                    "event_type": "state_changed",
                    "data": {
                        "entity_id": entity_id,
                        "new_state": {"entity_id": entity_id, "state": state, "attributes": {}},
                    },
                },
            }
        )
    assert queue.qsize() == 2
    first = queue.get_nowait()
    assert first.entity_id == "number.living_room_pir_kanslighet_3"
    assert first.state == "90"


async def test_state_forwarder_passes_battery_sensors_and_drops_other_sensors():
    from argus_addon.__main__ import _build_state_forwarder

    queue: asyncio.Queue = asyncio.Queue()
    forward = _build_state_forwarder(queue)
    await forward(
        {
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": "sensor.camera1_batteri",
                    "new_state": {
                        "entity_id": "sensor.camera1_batteri",
                        "state": "12",
                        "attributes": {"device_class": "battery"},
                        "last_updated": "2026-10-14T12:00:00+00:00",
                    },
                },
            },
        }
    )
    await forward(
        {
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": "sensor.camera1_temperature",
                    "new_state": {"entity_id": "sensor.camera1_temperature", "state": "21", "attributes": {}},
                },
            },
        }
    )
    env = queue.get_nowait()
    assert env.entity_id == "sensor.camera1_batteri"
    assert env.state == "12"
    assert env.attributes == {"device_class": "battery"}
    assert queue.empty()


async def test_a_failure_without_a_message_still_acks_a_reason(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "01939999-0000-7000-8000-0000000000b5",
            "op": "webrtc_offer",
            "args": {"entity_id": "camera.silent", "sdp": "v=0\r\n"},
        }
        await cloud_mock.send_to_client(cmd)
        ack = await cloud_mock.wait_for(lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"], timeout=3.0)
        assert ack["error"] == "TimeoutError"
        assert ack["result"] is None
    finally:
        await _stop(task, stop)


def _presence_event(entity_id, old, new, attributes):
    return {
        "type": "event",
        "event": {
            "event_type": "state_changed",
            "data": {
                "entity_id": entity_id,
                "old_state": {"entity_id": entity_id, "state": old, "attributes": attributes},
                "new_state": {
                    "entity_id": entity_id,
                    "state": new,
                    "attributes": attributes,
                    "last_changed": "2026-10-20T07:00:00+00:00",
                },
            },
        },
    }


async def test_state_forwarder_sends_presence_changes_and_skips_position_updates():
    from argus_addon.__main__ import _build_state_forwarder

    queue: asyncio.Queue = asyncio.Queue()
    forward = _build_state_forwarder(queue)
    phone = {"friendly_name": "Alexanders iPhone", "source_type": "router"}
    await forward(_presence_event("device_tracker.alexanders_iphone", "home", "home", {**phone, "latitude": 59.3}))
    assert queue.empty()
    await forward(_presence_event("device_tracker.alexanders_iphone", "home", "not_home", phone))
    sent = queue.get_nowait()
    assert sent.type == "presence"
    assert sent.full is False
    assert sent.trackers[0].kind == "device_tracker"
    assert sent.trackers[0].state == "not_home"
    assert sent.trackers[0].source_type == "router"
    assert sent.trackers[0].name == "Alexanders iPhone"


async def test_each_scan_that_reaches_a_phone_at_home_is_a_sighting():
    from argus_addon.__main__ import _build_state_forwarder

    queue: asyncio.Queue = asyncio.Queue()
    forward = _build_state_forwarder(queue)
    phone = {"source_type": "router", "mac": "56:c9:55:3e:5b:be", "ip": "192.168.184.175"}
    event = _presence_event(
        "device_tracker.nmap_tracker_56_c9_55_3e_5b_be",
        "home",
        "home",
        {**phone, "last_time_reachable": "2026-10-21T14:44:24+02:00"},
    )
    event["event"]["data"]["old_state"]["attributes"] = {**phone, "last_time_reachable": "2026-10-21T14:42:23+02:00"}
    await forward(event)
    sent = queue.get_nowait()
    assert sent.type == "sightings"
    assert [(s.mac, s.ip, s.at.isoformat()) for s in sent.devices] == [
        ("56:C9:55:3E:5B:BE", "192.168.184.175", "2026-10-21T12:44:24+00:00")
    ]
    assert queue.empty(), "the same state is no presence news"
    away = _presence_event(
        "device_tracker.nmap_tracker_56_c9_55_3e_5b_be",
        "home",
        "not_home",
        {**phone, "last_time_reachable": "2026-10-21T14:44:24+02:00"},
    )
    await forward(away)
    assert queue.get_nowait().type == "presence", "gone is no sighting"
    assert queue.empty()
