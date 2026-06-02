import asyncio
import contextlib
from typing import Any

import aiohttp
import structlog

from argus_addon.envelope import EntityRef

log = structlog.get_logger(__name__)


class HaClient:
    def __init__(self, supervisor_token: str, ws_url: str = "ws://supervisor/core/websocket"):
        self.supervisor_token = supervisor_token
        self.ws_url = ws_url
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._ha_version: str | None = None

    async def connect(self, ws_url: str | None = None) -> None:
        if ws_url is not None:
            self.ws_url = ws_url
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.ws_url)
        first = await self._ws.receive_json()
        if first.get("type") != "auth_required":
            raise RuntimeError(f"unexpected first frame: {first}")
        self._ha_version = first.get("ha_version")
        await self._ws.send_json({"type": "auth", "access_token": self.supervisor_token})
        auth_reply = await self._ws.receive_json()
        if auth_reply.get("type") != "auth_ok":
            raise RuntimeError(f"auth failed: {auth_reply}")
        self._ha_version = auth_reply.get("ha_version", self._ha_version)
        self._reader_task = asyncio.create_task(self._reader())

    def version(self) -> str:
        return self._ha_version or "unknown"

    async def _reader(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = msg.json()
                msg_id = data.get("id")
                if msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        fut.set_result(data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("ha_client reader error", error=str(exc))

    async def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        assert self._ws is not None
        async with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            fut: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
            self._pending[msg_id] = fut
            payload = {"id": msg_id, **payload}
            await self._ws.send_json(payload)
        return await fut

    async def fetch_entities(self) -> list[EntityRef]:
        reply = await self._request({"type": "config/entity_registry/list"})
        result = reply.get("result", [])
        entities: list[EntityRef] = []
        for row in result:
            entity_id = row.get("entity_id", "")
            domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
            if domain not in {"binary_sensor", "switch"}:
                continue
            entities.append(
                EntityRef(
                    entity_id=entity_id,
                    device_class=row.get("device_class"),
                    domain=domain,
                    friendly_name=row.get("friendly_name"),
                )
            )
        return entities

    async def subscribe_events(self) -> None:
        await self._request({"type": "subscribe_events", "event_type": "state_changed"})

    async def call_service(self, domain: str, service: str, service_data: dict[str, Any]) -> dict[str, Any]:
        reply = await self._request(
            {
                "type": "call_service",
                "domain": domain,
                "service": service,
                "service_data": service_data,
            }
        )
        return reply

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._session is not None and not self._session.closed:
            await self._session.close()
