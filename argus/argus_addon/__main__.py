import asyncio
import os
from pathlib import Path

import structlog
from aiohttp import web

from argus_addon import ingress, ws_client
from argus_addon.ha_client import HaClient
from argus_addon.idempotency import Idempotency

log = structlog.get_logger(__name__)


DEFAULT_TOKEN_PATH = Path("/data/token.txt")
DEFAULT_IDEMPOTENCY_PATH = Path("/data/idempotency.db")
DEFAULT_CLOUD_URL = "wss://ws.argus.ixdev.se/ws/addon"
DEFAULT_HA_WS_URL = "ws://supervisor/core/websocket"


async def main():
    token_path = Path(os.environ.get("ARGUS_TOKEN_PATH", str(DEFAULT_TOKEN_PATH)))
    idempotency_path = Path(os.environ.get("ARGUS_IDEMPOTENCY_PATH", str(DEFAULT_IDEMPOTENCY_PATH)))
    cloud_url = os.environ.get("ARGUS_CLOUD_URL", DEFAULT_CLOUD_URL)
    supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")
    ha_ws_url = os.environ.get("ARGUS_HA_WS_URL", DEFAULT_HA_WS_URL)

    idempotency = Idempotency(idempotency_path)
    ha_client = HaClient(supervisor_token=supervisor_token, ws_url=ha_ws_url)
    if supervisor_token:
        try:
            await ha_client.connect()
        except Exception as exc:
            log.warning("ha_client connect failed at startup", error=str(exc))

    send_queue: asyncio.Queue = asyncio.Queue()

    app = ingress.build_app(token_path)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8099)
    await site.start()
    log.info("ingress listening", host="0.0.0.0", port=8099)

    try:
        await ws_client.run(
            token_path=token_path,
            cloud_url=cloud_url,
            ha_client=ha_client,
            send_queue=send_queue,
            idempotency=idempotency,
        )
    finally:
        await runner.cleanup()
        await ha_client.close()


if __name__ == "__main__":
    asyncio.run(main())
