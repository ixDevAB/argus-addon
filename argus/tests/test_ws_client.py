import asyncio
import contextlib

from argus_addon import ws_client
from argus_addon.idempotency import Idempotency


class FakeHaClient:
    def __init__(self):
        self.calls: list[dict] = []
        self._version = "2026.5.0"
        self.entities: list = []

    def version(self) -> str:
        return self._version

    async def fetch_entities(self):
        return list(self.entities)

    async def call_service(self, domain, service, service_data):
        self.calls.append({"domain": domain, "service": service, "service_data": service_data})


async def _start_runner(cloud_mock, tmp_path, *, max_backoff=1.0, initial_backoff=0.05):
    token_path = tmp_path / "token.txt"
    token_path.write_text("test-token-xyz")
    ha_client = FakeHaClient()
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
