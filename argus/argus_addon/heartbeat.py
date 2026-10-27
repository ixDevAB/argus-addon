import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime

HEARTBEAT_INTERVAL = 15.0


async def heartbeat(ws, interval: float = HEARTBEAT_INTERVAL, ha_link_up: Callable[[], bool] | None = None):
    """Tell the cloud the add-on is alive and, when it knows, whether Home Assistant is reachable.

    A socket without Home Assistant sees no door and no motion, so the cloud must not read it as a
    watched home.
    """
    while True:
        await asyncio.sleep(interval)
        payload = {"type": "hb", "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z")}
        if ha_link_up is not None:
            payload["ha"] = "up" if ha_link_up() else "down"
        await ws.send(json.dumps(payload))
