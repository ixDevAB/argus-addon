import asyncio
import contextlib

from argus_addon import ws_client
from argus_addon.envelope import EntityRef
from argus_addon.idempotency import Idempotency


class FakeHaClient:
    def __init__(self):
        self.calls: list[dict] = []
        self._version = "2026.5.0"
        self.entities: list = []
        self.states: list = []

    def version(self) -> str:
        return self._version

    async def fetch_entities(self):
        return list(self.entities)

    async def fetch_states(self):
        return list(self.states)

    async def call_service(self, domain, service, service_data):
        self.calls.append({"domain": domain, "service": service, "service_data": service_data})


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


async def test_handshake_sends_hello(cloud_mock, tmp_path):
    task, _ha, _idem, _q, stop, _tp = await _start_runner(cloud_mock, tmp_path)
    try:
        env = await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        assert env["addon_version"] == "0.1.0"
        assert env["ha_version"] == "2026.5.0"
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
