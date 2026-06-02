import asyncio
import contextlib

from argus_addon import ws_client
from argus_addon.idempotency import Idempotency


class FakeHaClient:
    def __init__(self):
        self.calls: list[dict] = []
        self._version = "2026.5.0"

    def version(self) -> str:
        return self._version

    async def call_service(self, domain, service, service_data):
        self.calls.append({"domain": domain, "service": service, "service_data": service_data})


async def _start_runner(cloud_mock, tmp_path):
    token_path = tmp_path / "token.txt"
    token_path.write_text("dry-run-test-token")
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
            max_backoff=1.0,
            initial_backoff=0.05,
            heartbeat_interval=5.0,
            stop_event=stop_event,
        )
    )
    return task, ha_client, idempotency, stop_event


async def _stop(task, stop_event):
    stop_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


async def test_dry_run_relay_then_replay_dedupes_at_addon(cloud_mock, tmp_path):
    """REL-02 end-to-end through addon code path: re-sent cmd with same id is acked but not re-executed."""
    task, ha, _idem, stop = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "019e8336-0000-7000-8000-000000000001",
            "op": "call_service",
            "args": {
                "domain": "switch",
                "service": "turn_on",
                "service_data": {"entity_id": "switch.siren_living"},
            },
        }
        await cloud_mock.send_to_client(cmd)
        first_ack = await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"] and not e.get("duplicate"),
            timeout=3.0,
        )
        assert first_ack.get("duplicate") in (None, False)
        assert len(ha.calls) == 1
        assert ha.calls[0]["domain"] == "switch"
        assert ha.calls[0]["service"] == "turn_on"

        # REL-02: cloud replays the same cmd id; addon must dedupe locally
        await cloud_mock.send_to_client(cmd)
        replay_ack = await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"] and e.get("duplicate") is True,
            timeout=3.0,
        )
        assert replay_ack["duplicate"] is True
        assert len(ha.calls) == 1, "duplicate cmd id must NOT re-execute the HA service call"
    finally:
        await _stop(task, stop)


async def test_dry_run_relay_call_service_args_propagate(cloud_mock, tmp_path):
    """Add-on faithfully relays domain/service/service_data from cloud to HA."""
    task, ha, _idem, stop = await _start_runner(cloud_mock, tmp_path)
    try:
        await cloud_mock.wait_for(lambda e: e.get("type") == "hello", timeout=3.0)
        cmd = {
            "type": "cmd",
            "id": "019e8336-0000-7000-8000-000000000002",
            "op": "call_service",
            "args": {
                "domain": "switch",
                "service": "turn_off",
                "service_data": {"entity_id": "switch.siren_living", "color": "red"},
            },
        }
        await cloud_mock.send_to_client(cmd)
        await cloud_mock.wait_for(
            lambda e: e.get("type") == "ack" and e.get("id") == cmd["id"],
            timeout=3.0,
        )
        assert ha.calls[0]["service"] == "turn_off"
        assert ha.calls[0]["service_data"]["entity_id"] == "switch.siren_living"
        assert ha.calls[0]["service_data"]["color"] == "red"
    finally:
        await _stop(task, stop)
