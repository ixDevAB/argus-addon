import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from aiohttp import web

from argus_addon import ingress, pair_client, ws_client
from argus_addon.envelope import State
from argus_addon.ha_client import HaClient
from argus_addon.idempotency import Idempotency
from argus_addon.pair_client import CodeHolder

log = structlog.get_logger(__name__)


DEFAULT_TOKEN_PATH = Path("/data/token.txt")
DEFAULT_IDEMPOTENCY_PATH = Path("/data/idempotency.db")
DEFAULT_CLOUD_URL = "wss://ws.argus.ixdev.se/ws/addon"
DEFAULT_HA_WS_URL = "ws://supervisor/core/websocket"


def _has_token(token_path: Path) -> bool:
    return token_path.exists() and bool(token_path.read_text().strip())


def _build_state_forwarder(send_queue: asyncio.Queue):
    async def on_event(data: dict[str, Any]) -> None:
        event = data.get("event") or {}
        if event.get("event_type") != "state_changed":
            return
        event_data = event.get("data") or {}
        new_state = event_data.get("new_state") or {}
        entity_id = new_state.get("entity_id") or event_data.get("entity_id")
        state_value = new_state.get("state")
        if not entity_id or state_value is None:
            return
        domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
        if domain not in {"binary_sensor", "switch"}:
            return
        last_updated = new_state.get("last_updated") or new_state.get("last_changed")
        if isinstance(last_updated, str):
            try:
                at = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
            except ValueError:
                at = datetime.now(UTC)
        else:
            at = datetime.now(UTC)
        raw_attrs = new_state.get("attributes") or {}
        forwarded_attrs = {
            k: v for k, v in raw_attrs.items() if k in {"battery_level", "device_class", "friendly_name"}
        }
        envelope = State(
            type="state",
            entity_id=entity_id,
            state=state_value,
            at=at,
            attributes=forwarded_attrs or None,
        )
        await send_queue.put(envelope)

    return on_event


async def main():
    token_path = Path(os.environ.get("ARGUS_TOKEN_PATH", str(DEFAULT_TOKEN_PATH)))
    idempotency_path = Path(os.environ.get("ARGUS_IDEMPOTENCY_PATH", str(DEFAULT_IDEMPOTENCY_PATH)))
    cloud_url = os.environ.get("ARGUS_CLOUD_URL", DEFAULT_CLOUD_URL)
    supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")
    ha_ws_url = os.environ.get("ARGUS_HA_WS_URL", DEFAULT_HA_WS_URL)

    idempotency = Idempotency(idempotency_path)
    send_queue: asyncio.Queue = asyncio.Queue()
    ha_client = HaClient(
        supervisor_token=supervisor_token,
        ws_url=ha_ws_url,
        on_event=_build_state_forwarder(send_queue),
    )
    if supervisor_token:
        try:
            await ha_client.connect()
            await ha_client.subscribe_events()
        except Exception as exc:
            log.warning("ha_client connect failed at startup", error=str(exc))

    code_holder = CodeHolder()
    app = ingress.build_app(token_path, code_holder)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8099)
    await site.start()
    log.info("ingress listening", host="0.0.0.0", port=8099)

    try:
        if not _has_token(token_path):
            log.info("no install token, entering pairing mode")
            await pair_client.run_pairing(
                token_path=token_path,
                cloud_url=cloud_url,
                code_holder=code_holder,
            )
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
