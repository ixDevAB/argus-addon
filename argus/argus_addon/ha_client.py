import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import structlog

from argus_addon.envelope import EntityRef

log = structlog.get_logger(__name__)

REQUEST_TIMEOUT = 10.0
WS_HEARTBEAT = 25.0


class HaClient:
    def __init__(
        self,
        supervisor_token: str,
        ws_url: str = "ws://supervisor/core/websocket",
        on_event: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ):
        self.supervisor_token = supervisor_token
        self.ws_url = ws_url
        self._on_event = on_event
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._ha_version: str | None = None
        # Seed the state_changed subscription so every (re)connect re-subscribes automatically,
        # without a separate startup connect step. connect() replays this set after auth.
        self._subscribed_event_types: set[str] = {"state_changed"} if on_event is not None else set()
        self._closing = False

    def _ws_live(self) -> bool:
        return self._ws is not None and not self._ws.closed

    async def connect(self, ws_url: str | None = None) -> None:
        if ws_url is not None:
            self.ws_url = ws_url
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.ws_url, heartbeat=WS_HEARTBEAT)
        first = await self._ws.receive_json()
        if first.get("type") != "auth_required":
            raise RuntimeError(f"unexpected first frame: {first}")
        self._ha_version = first.get("ha_version")
        await self._ws.send_json({"type": "auth", "access_token": self.supervisor_token})
        auth_reply = await self._ws.receive_json()
        if auth_reply.get("type") != "auth_ok":
            raise RuntimeError(f"auth failed: {auth_reply}")
        self._ha_version = auth_reply.get("ha_version", self._ha_version)
        self._next_id = 1
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionResetError("ha_client reconnected"))
        self._pending.clear()
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
        self._reader_task = asyncio.create_task(self._reader())
        for event_type in list(self._subscribed_event_types):
            await self._send_subscribe(event_type)

    async def _ensure_connected(self) -> None:
        if self._closing:
            raise ConnectionResetError("ha_client is closing")
        if self._ws_live():
            return
        async with self._connect_lock:
            if self._ws_live():
                return
            log.info("ha_client reconnecting", url=self.ws_url)
            await self.connect()

    async def _reset_ws(self) -> None:
        ws = self._ws
        self._ws = None
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
        if ws is not None and not ws.closed:
            with contextlib.suppress(Exception):
                await ws.close()

    def version(self) -> str:
        return self._ha_version or "unknown"

    async def _reader(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = msg.json()
                if data.get("type") == "event" and self._on_event is not None:
                    try:
                        await self._on_event(data)
                    except Exception as exc:
                        log.warning("ha_client on_event raised", error=str(exc))
                    continue
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
        await self._ensure_connected()
        assert self._ws is not None
        async with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            fut: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
            self._pending[msg_id] = fut
            payload = {"id": msg_id, **payload}
            try:
                await self._ws.send_json(payload)
            except (ConnectionResetError, RuntimeError, aiohttp.ClientError) as exc:
                self._pending.pop(msg_id, None)
                if not fut.done():
                    fut.cancel()
                await self._reset_ws()
                raise ConnectionResetError(f"ha_client send failed: {exc}") from exc
        try:
            return await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)
        except TimeoutError as exc:
            self._pending.pop(msg_id, None)
            if not fut.done():
                fut.cancel()
            raise TimeoutError(f"ha_client request timed out after {REQUEST_TIMEOUT}s: {payload.get('type')}") from exc

    async def _send_subscribe(self, event_type: str) -> None:
        await self._request({"type": "subscribe_events", "event_type": event_type})

    async def fetch_entities(self) -> list[EntityRef]:
        registry_reply = await self._request({"type": "config/entity_registry/list"})
        registry_rows = registry_reply.get("result", []) or []

        device_to_area: dict[str, str | None] = {}
        try:
            device_reply = await self._request({"type": "config/device_registry/list"})
            for drow in device_reply.get("result", []) or []:
                did = drow.get("id")
                if did:
                    device_to_area[did] = drow.get("area_id")
        except Exception as exc:
            log.warning("device_registry fetch failed", error=str(exc))

        area_name: dict[str, str] = {}
        try:
            area_reply = await self._request({"type": "config/area_registry/list"})
            for arow in area_reply.get("result", []) or []:
                aid = arow.get("area_id")
                name = arow.get("name")
                if aid and name:
                    area_name[aid] = name
        except Exception as exc:
            log.warning("area_registry fetch failed", error=str(exc))

        states_by_id: dict[str, dict[str, Any]] = {}
        try:
            states_reply = await self._request({"type": "get_states"})
            for srow in states_reply.get("result", []) or []:
                eid = srow.get("entity_id", "")
                if eid:
                    states_by_id[eid] = srow
        except Exception as exc:
            log.warning("get_states failed; falling back to registry only", error=str(exc))

        entities: list[EntityRef] = []
        for row in registry_rows:
            entity_id = row.get("entity_id", "")
            domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
            if domain not in {"binary_sensor", "switch"}:
                continue
            state = states_by_id.get(entity_id, {})
            attrs = state.get("attributes", {}) if isinstance(state, dict) else {}
            device_class = row.get("device_class") or row.get("original_device_class") or attrs.get("device_class")
            friendly_name = row.get("name") or row.get("friendly_name") or attrs.get("friendly_name")
            area_id = row.get("area_id") or device_to_area.get(row.get("device_id") or "")
            area = area_name.get(area_id) if area_id else None
            entities.append(
                EntityRef(
                    entity_id=entity_id,
                    device_class=device_class,
                    domain=domain,
                    friendly_name=friendly_name,
                    area=area,
                )
            )
        return entities

    async def subscribe_events(self) -> None:
        self._subscribed_event_types.add("state_changed")
        await self._send_subscribe("state_changed")

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
        self._closing = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._session is not None and not self._session.closed:
            await self._session.close()
