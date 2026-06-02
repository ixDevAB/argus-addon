import asyncio
import json
from datetime import UTC, datetime

HEARTBEAT_INTERVAL = 15.0


async def heartbeat(ws, interval: float = HEARTBEAT_INTERVAL):
    while True:
        await asyncio.sleep(interval)
        payload = {"type": "hb", "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z")}
        await ws.send(json.dumps(payload))
