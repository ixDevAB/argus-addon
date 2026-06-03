import asyncio
import contextlib
import json
import stat
from datetime import UTC, datetime, timedelta

from structlog.testing import capture_logs

from argus_addon import pair_client
from argus_addon.pair_client import CodeHolder, provisional_url


def test_provisional_url_swaps_final_segment():
    assert provisional_url("wss://ws.argus.ixdev.se/ws/addon") == "wss://ws.argus.ixdev.se/ws/pair"
    assert provisional_url("ws://127.0.0.1:8000/ws/addon") == "ws://127.0.0.1:8000/ws/pair"


async def _stop(task, stop_event):
    stop_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


async def test_pairing_connects_pair_path_exposes_code_then_persists(cloud_mock, tmp_path):
    token_path = tmp_path / "token.txt"
    code_holder = CodeHolder()
    stop_event = asyncio.Event()

    expires_at = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    cloud_mock.pair_code = {
        "type": "pair_code",
        "session_id": "01939999-0000-7000-8000-000000000001",
        "code": "012345",
        "expires_at": expires_at,
    }
    install_token = "install-token-secret-xyz"

    # cloud_url points /ws/addon at the mock; pairing derives /ws/pair from it.
    cloud_url = f"{cloud_mock.url}/ws/addon"

    with capture_logs() as logs:
        task = asyncio.create_task(
            pair_client.run_pairing(
                token_path=token_path,
                cloud_url=cloud_url,
                code_holder=code_holder,
                use_tls=False,
                max_backoff=1.0,
                initial_backoff=0.05,
                heartbeat_interval=5.0,
                stop_event=stop_event,
            )
        )
        try:
            # Connected on the provisional /ws/pair path (no token segment).
            await cloud_mock.wait_for_pair_connection(timeout=3.0)
            assert cloud_mock.pair_paths[-1].endswith("/ws/pair")

            # Code is exposed via the holder.
            async def wait_code(deadline):
                while code_holder.code is None:
                    if asyncio.get_event_loop().time() > deadline:
                        raise TimeoutError("code never exposed")
                    await asyncio.sleep(0.02)

            await wait_code(asyncio.get_event_loop().time() + 3.0)
            assert code_holder.code == "012345"
            assert code_holder.session_id == "01939999-0000-7000-8000-000000000001"

            # Deliver the install token; run_pairing should persist it and return.
            await cloud_mock.send_to_pair({"type": "pair_token", "install_token": install_token})
            await asyncio.wait_for(task, timeout=3.0)
        finally:
            if not task.done():
                await _stop(task, stop_event)

    # Token persisted at 0600 and code cleared.
    assert token_path.read_text() == install_token
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    assert code_holder.code is None

    # The install token is never logged anywhere.
    blob = json.dumps(logs)
    assert install_token not in blob
    # The plaintext code IS logged (per spec); the token is not.
    assert any(entry.get("event") == "pairing code" and entry.get("code") == "012345" for entry in logs)


async def test_pairing_replaces_stale_code_on_new_frame(cloud_mock, tmp_path):
    token_path = tmp_path / "token.txt"
    code_holder = CodeHolder()
    stop_event = asyncio.Event()
    expires_at = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    cloud_mock.pair_code = {
        "type": "pair_code",
        "session_id": "s1",
        "code": "111111",
        "expires_at": expires_at,
    }
    cloud_url = f"{cloud_mock.url}/ws/addon"
    task = asyncio.create_task(
        pair_client.run_pairing(
            token_path=token_path,
            cloud_url=cloud_url,
            code_holder=code_holder,
            use_tls=False,
            max_backoff=1.0,
            initial_backoff=0.05,
            heartbeat_interval=5.0,
            stop_event=stop_event,
        )
    )
    try:
        await cloud_mock.wait_for_pair_connection(timeout=3.0)

        async def wait_code(value, deadline):
            while code_holder.code != value:
                if asyncio.get_event_loop().time() > deadline:
                    raise TimeoutError(f"code never became {value}, is {code_holder.code}")
                await asyncio.sleep(0.02)

        await wait_code("111111", asyncio.get_event_loop().time() + 3.0)
        await cloud_mock.send_to_pair(
            {"type": "pair_code", "session_id": "s2", "code": "222222", "expires_at": expires_at}
        )
        await wait_code("222222", asyncio.get_event_loop().time() + 3.0)
        assert code_holder.session_id == "s2"
    finally:
        await _stop(task, stop_event)
