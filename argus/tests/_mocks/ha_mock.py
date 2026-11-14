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
        # entity_id -> compressed history rows ({"s", "lu", "lc"}); served by history/history_during_period.
        self.history: dict[str, list[dict[str, Any]]] = {}
        self.history_requests: list[dict[str, Any]] = []
        # device_registry rows: [{"id","name","name_by_user","area_id"}, ...]
        self.devices: list[dict[str, Any]] = []
        # integration domain -> iot_class; served by manifest/list.
        self.iot_classes: dict[str, str] = {"zha": "local_push", "hue": "local_push", "reolink": "local_push"}
        self.service_calls: list[dict[str, Any]] = []
        # When set, call_service answers success false with this error, as HA does for a refused call.
        self.service_error: dict[str, Any] | None = None
        self.subscribe_error: dict[str, Any] | None = None
        # config/entity_registry/update payloads, in order.
        self.registry_updates: list[dict[str, Any]] = []
        # WebRTC: offers received, subscriptions unsubscribed, and the scripted reply.
        self.webrtc_offers: list[dict[str, Any]] = []
        self.unsubscribed: list[int] = []
        self.webrtc_answer: str = (
            "v=0\r\no=- 2 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=sendonly\r\n"
        )
        self.webrtc_candidates: list[dict[str, Any]] = [
            {"candidate": "candidate:1 1 udp 2130706431 192.168.1.10 45645 typ host", "sdpMLineIndex": 0},
            {
                "candidate": "candidate:2 1 udp 1694498815 98.128.181.109 59174 typ srflx raddr 0.0.0.0 rport 59398",
                "sdpMLineIndex": 0,
            },
        ]
        self.webrtc_error: dict[str, Any] | None = None
        self.webrtc_answer_delay: float = 0.05
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
            elif kind == "config/entity_registry/update":
                self.registry_updates.append({k: v for k, v in data.items() if k not in ("id", "type")})
                for row in self.entities:
                    if row.get("entity_id") == data.get("entity_id") and "disabled_by" in data:
                        row["disabled_by"] = data["disabled_by"]
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": {}})
            elif kind == "config/entity_registry/get":
                row = next((r for r in self.entities if r.get("entity_id") == data.get("entity_id")), None)
                await ws.send_json({"id": msg_id, "type": "result", "success": row is not None, "result": row})
            elif kind == "config/device_registry/list":
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": list(self.devices)})
            elif kind == "manifest/list":
                wanted = data.get("integrations") or list(self.iot_classes)
                result = [{"domain": d, "iot_class": self.iot_classes[d]} for d in wanted if d in self.iot_classes]
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": result})
            elif kind == "get_states":
                result = [
                    {"entity_id": eid, "state": s.get("state"), "attributes": s.get("attributes", {})}
                    for eid, s in self.states.items()
                ]
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": result})
            elif kind == "history/history_during_period":
                self.history_requests.append({k: v for k, v in data.items() if k not in ("id", "type")})
                result = {eid: self.history[eid] for eid in data.get("entity_ids", []) if eid in self.history}
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": result})
            elif kind == "subscribe_events":
                if self.subscribe_error is not None:
                    await ws.send_json(
                        {"id": msg_id, "type": "result", "success": False, "error": self.subscribe_error}
                    )
                else:
                    await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
            elif kind == "unsubscribe_events":
                self.unsubscribed.append(data.get("subscription"))
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
            elif kind == "camera/webrtc/offer":
                self.webrtc_offers.append({"entity_id": data.get("entity_id"), "offer": data.get("offer")})
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
                await ws.send_json(
                    {"id": msg_id, "type": "event", "event": {"type": "session", "session_id": "01MOCKSESSION"}}
                )
                if self.webrtc_error is not None:
                    await ws.send_json({"id": msg_id, "type": "event", "event": {"type": "error", **self.webrtc_error}})
                    continue
                await asyncio.sleep(self.webrtc_answer_delay)
                await ws.send_json(
                    {"id": msg_id, "type": "event", "event": {"type": "answer", "answer": self.webrtc_answer}}
                )
                for cand in self.webrtc_candidates:
                    await ws.send_json(
                        {"id": msg_id, "type": "event", "event": {"type": "candidate", "candidate": cand}}
                    )
            elif kind == "call_service":
                self.service_calls.append(
                    {
                        "domain": data.get("domain"),
                        "service": data.get("service"),
                        "service_data": data.get("service_data", {}),
                    }
                )
                if self.service_error is not None:
                    await ws.send_json({"id": msg_id, "type": "result", "success": False, "error": self.service_error})
                else:
                    await ws.send_json(
                        {"id": msg_id, "type": "result", "success": True, "result": {"context": {"id": "ctx-mock"}}}
                    )
            else:
                await ws.send_json({"id": msg_id, "type": "result", "success": True, "result": None})
        return ws

    async def push_event(self, event: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._sockets)
        for ws in targets:
            if not ws.closed:
                await ws.send_json({"type": "event", "event": event})

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
