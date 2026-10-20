import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import aiohttp
import structlog

from argus_addon.envelope import DeviceRef, EntityRef, Tracker

log = structlog.get_logger(__name__)

REQUEST_TIMEOUT = 10.0
WS_HEARTBEAT = 25.0

# Backoff for re-establishing the HA link after it drops (Core restart).
RECONNECT_INITIAL_S = 0.5
RECONNECT_MAX_S = 30.0

# Domains Argus ingests: contact/motion triggers (binary_sensor), outputs —
# siren (switch relays + HA's dedicated siren domain, e.g. camera sirens) and lights
# (light fixtures + switch plugs used as deterrents) — and cameras (live-stream
# sources; their state carries availability). Argus decides which to surface/hide
# downstream; the add-on is a dumb pipe.
SYNCED_DOMAINS = {"binary_sensor", "switch", "siren", "light", "camera"}

# Who is home: HA's persons, and the device trackers behind them and the router's Wi-Fi list.
# They travel as presence frames, never as sensor states.
PRESENCE_DOMAINS = {"person", "device_tracker"}

# Battery levels for devices whose battery is its own entity (Reolink cameras report
# sensor.<cam>_batteri). Only sensor.* rows with this device_class are synced; the sensor
# domain is otherwise far too noisy to forward.
BATTERY_DEVICE_CLASS = "battery"


def is_battery_sensor(entity_id: str, device_class: str | None) -> bool:
    return entity_id.startswith("sensor.") and device_class == BATTERY_DEVICE_CLASS


# Charging state for those same devices (Reolink: sensor.<cam>_batteritillstand, an enum of
# discharging / charging / chargecomplete). Identified by HA's translation_key in the registry
# and, on state events that carry no translation_key, by the enum's options.
BATTERY_STATE_TRANSLATION_KEY = "battery_state"
CHARGING_OPTION = "charging"


def is_battery_state_sensor(entity_id: str, translation_key: str | None, options: Any) -> bool:
    if not entity_id.startswith("sensor."):
        return False
    if translation_key == BATTERY_STATE_TRANSLATION_KEY:
        return True
    return isinstance(options, list) and CHARGING_OPTION in options


# How long after HA's WebRTC answer we keep collecting trickled ICE candidates before
# folding them into the answer. In the field the server-reflexive candidate arrives
# within ~50 ms of the answer; the window is the upper bound, not the typical wait.
WEBRTC_CANDIDATE_WINDOW = 1.5
# Home Assistant answers one camera in a few seconds, but a second offer to the same Reolink hub
# can wait for the first camera to wake; the cloud waits a little longer than this.
WEBRTC_ANSWER_TIMEOUT = 25.0

# Writable config entities (camera sensitivity, day/night mode, ...). Synced in the
# entity_list with their current value + bounds/options and device grouping, but NOT
# streamed as live state events — Argus reads them on demand / at resync.
CONFIG_DOMAINS = {"number", "select"}


def is_scanner_tracker(row: dict[str, Any]) -> bool:
    """A device tracker a network scanner made, as its registry row describes it."""
    return str(row.get("entity_id", "")).startswith("device_tracker.") and row.get("disabled_by") == "integration"


def _aware(text: Any) -> datetime | None:
    """An ISO time with its offset; None for anything else, since a time without a zone is a guess."""
    if not isinstance(text, str):
        return None
    try:
        at = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return at if at.tzinfo is not None else None


def tracker_from_state(row: dict[str, Any]) -> Tracker | None:
    """The tracker a person.* or device_tracker.* state describes; None for anything else."""
    entity_id = row.get("entity_id", "")
    kind = entity_id.split(".", 1)[0] if "." in entity_id else ""
    state = row.get("state")
    if kind not in PRESENCE_DOMAINS or not isinstance(state, str):
        return None
    attrs = row.get("attributes") or {}
    changed = row.get("last_changed") or row.get("last_updated")
    try:
        changed_at = (
            datetime.fromisoformat(changed.replace("Z", "+00:00")) if isinstance(changed, str) else datetime.now(UTC)
        )
    except ValueError:
        changed_at = datetime.now(UTC)
    trackers = attrs.get("device_trackers")
    name = attrs.get("friendly_name")
    source = attrs.get("source_type")
    mac = attrs.get("mac")
    return Tracker(
        entity_id=entity_id,
        kind=kind,
        name=name if isinstance(name, str) else None,
        state=state,
        source_type=source if isinstance(source, str) else None,
        device_trackers=[t for t in trackers if isinstance(t, str)] if isinstance(trackers, list) else None,
        mac=mac.upper() if isinstance(mac, str) and mac else None,
        changed_at=changed_at,
        last_seen_at=_aware(attrs.get("last_time_reachable")),
    )


def merge_candidates(sdp: str, candidates: list[dict[str, Any]]) -> str:
    """Fold trickled ICE candidates into an SDP answer, per media section, with end-of-candidates.

    HA sends each candidate as {"candidate": "candidate:...", "sdpMLineIndex": n}. The answer SDP
    is split on its `m=` lines and each candidate is appended to the section it names (section 0
    when unspecified). Sections that received candidates also get `a=end-of-candidates` so the
    viewer knows gathering is complete.
    """
    newline = "\r\n" if "\r\n" in sdp else "\n"
    lines = [line for line in sdp.split(newline)]
    while lines and lines[-1] == "":
        lines.pop()
    section_starts = [i for i, line in enumerate(lines) if line.startswith("m=")]
    if not section_starts:
        return sdp
    by_section: dict[int, list[str]] = {}
    for cand in candidates:
        text = cand.get("candidate") if isinstance(cand, dict) else None
        if not isinstance(text, str) or not text:
            continue
        line = text if text.startswith("a=") else f"a={text}"
        index = cand.get("sdpMLineIndex")
        section = index if isinstance(index, int) and 0 <= index < len(section_starts) else 0
        by_section.setdefault(section, []).append(line)
    if not by_section:
        return sdp
    out: list[str] = []
    for section, start in enumerate(section_starts):
        end = section_starts[section + 1] if section + 1 < len(section_starts) else len(lines)
        if section == 0:
            out.extend(lines[:start])
        out.extend(lines[start:end])
        extra = by_section.get(section)
        if extra:
            out.extend(extra)
            out.append("a=end-of-candidates")
    return newline.join(out) + newline


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
        self._reconnect_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        # Set when fetch_entities enabled registry entities that HA will materialise shortly.
        self.pending_resync = False
        # Set each time the link to Core comes back after dropping. Anything that changed while
        # we were away (an entity going unavailable and back during a Core restart) was never
        # seen as an event, so the cloud needs a fresh state snapshot.
        self.link_restored = asyncio.Event()
        # entity_id -> registry unique_id, kept for talk (a Reolink camera's hub + camera uid).
        self._unique_ids: dict[str, str] = {}
        # The devices behind the last fetched entity list; sent alongside it.
        self.devices: list[DeviceRef] = []
        self._connect_lock = asyncio.Lock()
        # Tasks the reader starts and must not await, kept so they are not collected mid-flight.
        self._background: set[asyncio.Task[None]] = set()
        self._ha_version: str | None = None
        # Seed the state_changed subscription so every (re)connect re-subscribes automatically,
        # without a separate startup connect step. connect() replays this set after auth.
        self._subscribed_event_types: set[str] = (
            {"state_changed", "entity_registry_updated"} if on_event is not None else set()
        )
        # Subscription id -> queue of event frames for that subscription (None = drain and drop).
        # Used for camera/webrtc/offer, whose answer and candidates arrive as events.
        self._subscriptions: dict[int, asyncio.Queue[dict[str, Any]] | None] = {}
        # HA WebRTC session id -> the offer subscription id that owns it. Unsubscribing is how
        # HA closes the session (there is no separate close command).
        self._webrtc_sessions: dict[str, int] = {}
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
        # A new HA connection has no subscriptions; any WebRTC sessions died with the old one.
        self._subscriptions.clear()
        self._webrtc_sessions.clear()
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

    def unique_id_of(self, entity_id: str) -> str | None:
        return self._unique_ids.get(entity_id)

    async def _reader(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                data = msg.json()
                if data.get("type") == "event":
                    self._watch_registry(data.get("event") or {})
                    sub_id = data.get("id")
                    if sub_id in self._subscriptions:
                        queue = self._subscriptions[sub_id]
                        if queue is not None:
                            queue.put_nowait(data.get("event") or {})
                        continue
                    if self._on_event is not None:
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
        # The link dropped (Core restart, supervisor hiccup). Nothing else re-establishes the
        # state_changed subscription unless a command happens to be relayed, and the alarm cannot
        # wait for that, so reconnect here until it sticks.
        if not self._closing and self._reader_task is asyncio.current_task():
            log.warning("ha_client link lost; reconnecting")
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        delay = RECONNECT_INITIAL_S
        while not self._closing:
            await asyncio.sleep(delay)
            try:
                await self._ensure_connected()
                log.info("ha_client link restored")
                self.link_restored.set()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("ha_client reconnect failed", error=str(exc), retry_in=delay)
                delay = min(delay * 2, RECONNECT_MAX_S)

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

    async def _subscribe(self, payload: dict[str, Any]) -> tuple[int, asyncio.Queue[dict[str, Any]]]:
        """Send a subscription-style command; returns its id and the queue its events land in.

        The queue is registered before the frame goes out so an event can never race ahead of it.
        """
        await self._ensure_connected()
        assert self._ws is not None
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        async with self._lock:
            msg_id = self._next_id
            self._next_id += 1
            fut: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
            self._pending[msg_id] = fut
            self._subscriptions[msg_id] = queue
            try:
                await self._ws.send_json({"id": msg_id, **payload})
            except (ConnectionResetError, RuntimeError, aiohttp.ClientError) as exc:
                self._pending.pop(msg_id, None)
                self._subscriptions.pop(msg_id, None)
                if not fut.done():
                    fut.cancel()
                await self._reset_ws()
                raise ConnectionResetError(f"ha_client send failed: {exc}") from exc
        try:
            reply = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)
        except TimeoutError as exc:
            self._pending.pop(msg_id, None)
            self._subscriptions.pop(msg_id, None)
            raise TimeoutError(
                f"ha_client subscribe timed out after {REQUEST_TIMEOUT}s: {payload.get('type')}"
            ) from exc
        if not reply.get("success", False):
            self._subscriptions.pop(msg_id, None)
            err = reply.get("error") or {}
            raise RuntimeError(f"{err.get('code', 'ha_error')}: {err.get('message', 'subscription refused')}")
        return msg_id, queue

    async def _unsubscribe(self, sub_id: int) -> None:
        self._subscriptions.pop(sub_id, None)
        with contextlib.suppress(Exception):
            await self._request({"type": "unsubscribe_events", "subscription": sub_id})

    async def webrtc_offer(
        self,
        entity_id: str,
        sdp: str,
        *,
        answer_timeout: float = WEBRTC_ANSWER_TIMEOUT,
        candidate_window: float = WEBRTC_CANDIDATE_WINDOW,
    ) -> dict[str, Any]:
        """Negotiate a WebRTC session with a Home Assistant camera.

        HA answers through the offer subscription: a `session` event, then `answer`, then trickled
        `candidate` events (its server-reflexive address arrives shortly after the answer). The
        cloud wants one round trip, so the candidates seen inside `candidate_window` are folded into
        the answer SDP. The subscription stays open: unsubscribing is what closes HA's session, and
        that happens in `webrtc_close`.
        """
        sub_id, queue = await self._subscribe({"type": "camera/webrtc/offer", "entity_id": entity_id, "offer": sdp})
        session_id: str | None = None
        answer: str | None = None
        candidates: list[dict[str, Any]] = []
        loop = asyncio.get_event_loop()
        deadline = loop.time() + answer_timeout
        try:
            while answer is None:
                remaining = deadline - loop.time()
                try:
                    if remaining <= 0:
                        raise TimeoutError
                    event = await asyncio.wait_for(queue.get(), timeout=remaining)
                except TimeoutError:
                    raise TimeoutError(
                        f"camera_no_answer: no WebRTC answer from {entity_id} within {answer_timeout}s"
                    ) from None
                kind = event.get("type")
                if kind == "session":
                    session_id = event.get("session_id")
                elif kind == "answer":
                    answer = event.get("answer")
                elif kind == "candidate":
                    candidates.append(event.get("candidate") or {})
                elif kind == "error":
                    raise RuntimeError(f"{event.get('code', 'webrtc_error')}: {event.get('message', '')}")
            window_end = loop.time() + candidate_window
            while True:
                remaining = window_end - loop.time()
                if remaining <= 0:
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if event.get("type") == "candidate":
                    candidates.append(event.get("candidate") or {})
                elif event.get("type") == "error":
                    raise RuntimeError(f"{event.get('code', 'webrtc_error')}: {event.get('message', '')}")
        except BaseException:
            await self._unsubscribe(sub_id)
            raise
        # Late candidates after the window are dropped (None queue): the answer already shipped.
        self._subscriptions[sub_id] = None
        if session_id:
            self._webrtc_sessions[session_id] = sub_id
        log.info("webrtc offer answered", entity_id=entity_id, session_id=session_id, candidates=len(candidates))
        return {"session_id": session_id, "sdp": merge_candidates(answer or "", candidates)}

    async def webrtc_close(self, session_id: str) -> bool:
        """Close a WebRTC session by unsubscribing its offer subscription. False if unknown."""
        sub_id = self._webrtc_sessions.pop(session_id, None)
        if sub_id is None:
            return False
        await self._unsubscribe(sub_id)
        log.info("webrtc session closed", session_id=session_id)
        return True

    async def fetch_entities(self) -> list[EntityRef]:
        registry_reply = await self._request({"type": "config/entity_registry/list"})
        registry_rows = registry_reply.get("result", []) or []

        # Integrations ship some entities disabled (Reolink's charging state, and a network
        # scanner's tracker for any device HA does not already know, which is every phone and
        # visitor). Argus needs those, so they are switched on here; HA creates the entity ~30 s
        # later, after which a resync picks it up (pending_resync tells the caller to schedule one).
        # Only what the integration disabled is touched, never what someone switched off.
        to_enable = [
            row["entity_id"]
            for row in registry_rows
            if row.get("disabled_by") == "integration"
            and row.get("entity_id")
            and (row.get("translation_key") == BATTERY_STATE_TRANSLATION_KEY or is_scanner_tracker(row))
        ]
        for entity_id in to_enable:
            try:
                await self._request(
                    {"type": "config/entity_registry/update", "entity_id": entity_id, "disabled_by": None}
                )
                self.pending_resync = True
                log.info("enabled registry entity", entity_id=entity_id)
            except Exception as exc:
                log.warning("enable registry entity failed", entity_id=entity_id, error=str(exc))

        device_to_area: dict[str, str | None] = {}
        device_to_name: dict[str, str | None] = {}
        device_rows: dict[str, dict[str, Any]] = {}
        try:
            device_reply = await self._request({"type": "config/device_registry/list"})
            for drow in device_reply.get("result", []) or []:
                did = drow.get("id")
                if did:
                    device_to_area[did] = drow.get("area_id")
                    device_to_name[did] = drow.get("name_by_user") or drow.get("name")
                    device_rows[did] = drow
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
            if entity_id and isinstance(row.get("unique_id"), str):
                self._unique_ids[entity_id] = row["unique_id"]
            domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
            if domain not in SYNCED_DOMAINS and domain not in CONFIG_DOMAINS and domain != "sensor":
                continue
            # A disabled registry entry has no state and refuses every service call and stream
            # (e.g. the two Reolink stream profiles HA leaves off by default), so it is not a
            # candidate for anything in Argus.
            if row.get("disabled_by"):
                continue
            state = states_by_id.get(entity_id, {})
            attrs = state.get("attributes", {}) if isinstance(state, dict) else {}
            device_class = row.get("device_class") or row.get("original_device_class") or attrs.get("device_class")
            translation_key = row.get("translation_key")
            options = (row.get("capabilities") or {}).get("options") or attrs.get("options")
            if (
                domain == "sensor"
                and not is_battery_sensor(entity_id, device_class)
                and not is_battery_state_sensor(entity_id, translation_key, options)
            ):
                continue
            friendly_name = row.get("name") or row.get("friendly_name") or attrs.get("friendly_name")
            device_id = row.get("device_id")
            area_id = row.get("area_id") or device_to_area.get(device_id or "")
            area = area_name.get(area_id) if area_id else None
            value: str | None = None
            config_attributes: dict[str, Any] | None = None
            if domain in CONFIG_DOMAINS or domain == "sensor":
                raw_state = state.get("state") if isinstance(state, dict) else None
                value = raw_state if isinstance(raw_state, str) else None
                if domain == "number":
                    config_attributes = {
                        k: attrs[k] for k in ("min", "max", "step", "mode", "unit_of_measurement") if k in attrs
                    }
                elif domain == "select":
                    options = attrs.get("options")
                    if isinstance(options, list):
                        config_attributes = {"options": options}
            entities.append(
                EntityRef(
                    entity_id=entity_id,
                    device_class=device_class,
                    domain=domain,
                    friendly_name=friendly_name,
                    area=area,
                    entity_category=row.get("entity_category"),
                    device_id=device_id,
                    device_name=device_to_name.get(device_id or ""),
                    value=value,
                    attributes=config_attributes or None,
                    platform=row.get("platform"),
                    translation_key=translation_key,
                )
            )
        # The units behind those entities, plus the hubs they hang off, so the app can show a
        # camera's model and firmware and which hub it belongs to.
        wanted: set[str] = {e.device_id for e in entities if e.device_id}
        for did in list(wanted):
            via = (device_rows.get(did) or {}).get("via_device_id")
            if via:
                wanted.add(via)
        self.devices = [
            DeviceRef(
                id=did,
                name=device_to_name.get(did),
                manufacturer=drow.get("manufacturer"),
                model=drow.get("model"),
                sw_version=drow.get("sw_version"),
                hw_version=drow.get("hw_version"),
                area=area_name.get(drow.get("area_id") or "") if drow.get("area_id") else None,
                via_device_id=drow.get("via_device_id"),
            )
            for did, drow in device_rows.items()
            if did in wanted
        ]
        return entities

    async def fetch_states(self) -> list[dict[str, Any]]:
        reply = await self._request({"type": "get_states"})
        out: list[dict[str, Any]] = []
        for srow in reply.get("result", []) or []:
            entity_id = srow.get("entity_id", "")
            domain = entity_id.split(".", 1)[0] if "." in entity_id else ""
            raw_attrs = srow.get("attributes") or {}
            if (
                domain not in SYNCED_DOMAINS
                and not is_battery_sensor(entity_id, raw_attrs.get("device_class"))
                and not is_battery_state_sensor(entity_id, None, raw_attrs.get("options"))
            ):
                continue
            state = srow.get("state")
            if not isinstance(state, str):
                continue
            attrs = {k: v for k, v in raw_attrs.items() if k in {"battery_level", "device_class", "friendly_name"}}
            out.append({"entity_id": entity_id, "state": state, "attributes": attrs})
        return out

    async def fetch_trackers(self) -> list[Tracker]:
        reply = await self._request({"type": "get_states"})
        out: list[Tracker] = []
        for srow in reply.get("result", []) or []:
            tracker = tracker_from_state(srow)
            if tracker is not None:
                out.append(tracker)
        return out

    async def subscribe_events(self) -> None:
        for event_type in ("state_changed", "entity_registry_updated"):
            self._subscribed_event_types.add(event_type)
            await self._send_subscribe(event_type)

    def _watch_registry(self, event: dict[str, Any]) -> None:
        """A network scanner makes a disabled tracker for each new device it sees; switch it on so
        the owner's Wi-Fi list shows visitors too. Runs as a task, since the reader must not wait."""
        if event.get("event_type") != "entity_registry_updated":
            return
        data = event.get("data") or {}
        entity_id = data.get("entity_id")
        if data.get("action") == "create" and isinstance(entity_id, str) and entity_id.startswith("device_tracker."):
            task = asyncio.create_task(self._enable_new_tracker(entity_id))
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    async def _enable_new_tracker(self, entity_id: str) -> None:
        try:
            reply = await self._request({"type": "config/entity_registry/get", "entity_id": entity_id})
            row = reply.get("result") or {}
            if is_scanner_tracker(row):
                await self._request(
                    {"type": "config/entity_registry/update", "entity_id": entity_id, "disabled_by": None}
                )
                log.info("enabled new device tracker", entity_id=entity_id)
        except Exception as exc:
            log.warning("enable new device tracker failed", entity_id=entity_id, error=str(exc))

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
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._session is not None and not self._session.closed:
            await self._session.close()
