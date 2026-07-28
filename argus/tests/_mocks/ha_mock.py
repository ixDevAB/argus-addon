import asyncio
import json
from typing import Any

from aiohttp import WSMsgType, web

DEFAULT_ENTITIES = [
    {
        "entity_id": "binary_sensor.kitchen_motion",
        "device_class": "motion",
        "platform": "zha",
        "friendly_name": "Kitchen Motion",
    },
    {
        "entity_id": "switch.siren_living",
        "device_class": None,
        "platform": "zha",
        "friendly_name": "Siren Living",
    },
    {
        "entity_id": "light.hall",
        "device_class": None,
        "platform": "hue",
        "friendly_name": "Hall Light",
    },
]


class HaMock:
    def __init__(self, entities: list[dict[str, Any]] | None = None):
        self.entities = entities if entities is not None else list(DEFAULT_ENTITIES)
        # entity_id -> {"state": str, "attributes": {...}}; served by get_states.
        self.states: dict[str, dict[str, Any]] = {}
        # device_registry rows: [{"id","name","name_by_user","area_id"}, ...]
        self.devices: list[dict[str, Any]] = []
        self.service_calls: list[dict[str, Any]] = []
        self.auth_token: str | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._port: int = 0
        self._sockets: list[web.WebSocketResponse] = []
        self._lock = asyncio.Lock()

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/core/websocket", self._handler)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        sock = self._runner.addresses[0]
        self._port = sock[1]
        return self.url

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self._port}/core/websocket"

    async def _handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async with self._lock:
            self._sockets.append(ws)
        await ws.send_json({"type": "auth_required", "ha_version": "2026.5.0"})
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            if data.get("type") == "auth":
                self.auth_token = data.get("access_token")
                if self.auth_token:
                    await ws.send_json({"type": "auth_ok", "ha_version": "2026.5.0"})
                else:
                    await ws.send_json({"type": "auth_invalid", "message": "missing token"})
                    await ws.close()
                    return ws
                continue
            msg_id = data.get("id")
            kind = data.get("type")
            if kind == "config/entity_registry/list":
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": list(self.entities)})
            elif kind == "config/device_registry/list":
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": list(self.devices)})
            elif kind == "get_states":
                result = [
                    {"entity_id": eid, "state": s.get("state"), "attributes": s.get("attributes", {})}
                    for eid, s in self.states.items()
                ]
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": result})
            elif kind == "subscribe_events":
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
            elif kind == "call_service":
                self.service_calls.append(
                    {
                        "domain": data.get("domain"),
                        "service": data.get("service"),
                        "service_data": data.get("service_data", {}),
                    }
                )
                await ws.send_json(
                    {"id": msg_id, "type": "result", "success": True, "result": {"context": {"id": "ctx-mock"}}}
                )
            else:
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
        return ws

    async def push_state_event(self, entity_id: str, new_state: str) -> None:
        payload = {
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": entity_id,
                    "new_state": {
                        "entity_id": entity_id,
                        "state": new_state,
                        "last_updated": "2026-05-28T12:00:00+00:00",
                    },
                },
            },
        }
        async with self._lock:
            targets = list(self._sockets)
        for ws in targets:
            if not ws.closed:
                await ws.send_json(payload)

    async def drop_connections(self) -> None:
        async with self._lock:
            targets = list(self._sockets)
            self._sockets.clear()
        for ws in targets:
            if not ws.closed:
                await ws.close()

    async def close(self) -> None:
        async with self._lock:
            targets = list(self._sockets)
            self._sockets.clear()
        for ws in targets:
            if not ws.closed:
                await ws.close()
        if self._site is not None:
            await self._site.stop()
        if self._runner is not None:
            await self._runner.cleanup()
