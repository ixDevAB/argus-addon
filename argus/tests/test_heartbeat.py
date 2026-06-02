import asyncio
import contextlib
import json

import pytest

from argus_addon.heartbeat import heartbeat


class FakeWs:
    def __init__(self):
        self.sent: list[str] = []

    async def send(self, payload):
        self.sent.append(payload)


async def test_heartbeat_cadence():
    ws = FakeWs()
    task = asyncio.create_task(heartbeat(ws, interval=0.05))
    await asyncio.sleep(0.2)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert len(ws.sent) >= 3
    for payload in ws.sent:
        env = json.loads(payload)
        assert env["type"] == "hb"
        assert "ts" in env


async def test_heartbeat_stops_on_cancel():
    ws = FakeWs()
    task = asyncio.create_task(heartbeat(ws, interval=0.05))
    await asyncio.sleep(0.12)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    before = len(ws.sent)
    await asyncio.sleep(0.15)
    assert len(ws.sent) == before
