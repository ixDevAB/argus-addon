import asyncio
import json
import random
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog
import websockets

from argus_addon import __version__
from argus_addon.envelope import (
    Ack,
    Cmd,
    EntityList,
    EnvelopeAdapter,
    Hello,
    Presence,
    Sightings,
    StateSnapshotItem,
    StatesSnapshot,
)
from argus_addon.heartbeat import HEARTBEAT_INTERVAL, heartbeat
from argus_addon.idempotency import Idempotency
from argus_addon.sightings import SWEEP_EVERY_S, sweep

log = structlog.get_logger(__name__)


# Home Assistant's own record of who came and went is replayed to the cloud: a day of it on connect,
# the last two hours every ten minutes, so a change the cloud missed or dated late is put right.
REPLAY_ON_CONNECT = timedelta(hours=24)
REPLAY_WINDOW = timedelta(hours=2)
REPLAY_EVERY_S = 600.0
# How many sightings one frame carries; the cloud takes up to 1024.
SIGHTINGS_PER_FRAME = 500

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0

# Commands that block on Home Assistant for seconds and therefore run as background tasks.
SLOW_OPS = {
    "webrtc_offer",
    "webrtc_close",
    "talk_offer",
    "talk_close",
    "record_clip",
    "extend_clip",
    "upload_clip",
    "cancel_clip",
}

# HA enables a registry entity on its next integration reload, about 30 s after the update.
RESYNC_AFTER_ENABLE_S = 45.0


async def run(
    token_path: Path,
    cloud_url: str,
    ha_client,
    send_queue: asyncio.Queue,
    idempotency: Idempotency,
    *,
    max_backoff: float = MAX_BACKOFF,
    initial_backoff: float = INITIAL_BACKOFF,
    use_tls: bool = True,
    heartbeat_interval: float = HEARTBEAT_INTERVAL,
    entity_fetch_timeout: float = 10.0,
    entity_retry_interval: float = 5.0,
    stop_event: asyncio.Event | None = None,
    talk=None,
    recorder=None,
):
    attempt = 0
    while True:
        if stop_event is not None and stop_event.is_set():
            return
        if not token_path.exists():
            await asyncio.sleep(0.1)
            continue
        token = token_path.read_text().strip()
        if not token:
            await asyncio.sleep(0.1)
            continue
        url = f"{cloud_url}/{token}"
        delay = min(initial_backoff * (2**attempt), max_backoff)
        if attempt:
            jitter = random.uniform(0, delay)
            log.info("reconnect", delay=jitter, attempt=attempt)
            await asyncio.sleep(jitter)
        try:
            ssl_ctx = ssl.create_default_context() if use_tls and url.startswith("wss://") else None
            async with websockets.connect(
                url,
                ssl=ssl_ctx,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                log.info("connected", attempt=attempt)
                hello = Hello(type="hello", addon_version=__version__, ha_version=ha_client.version())
                await ws.send(hello.model_dump_json())
                if recorder is not None:
                    # Files recorded while the link was down ask for a fresh upload URL.
                    await recorder.resend_spooled()
                hb_task = asyncio.create_task(heartbeat(ws, heartbeat_interval))
                send_task = asyncio.create_task(_send_loop(ws, send_queue))
                entity_task = asyncio.create_task(
                    _sync_entities(
                        ws,
                        ha_client,
                        timeout=entity_fetch_timeout,
                        retry_interval=entity_retry_interval,
                    )
                )
                restore_task = asyncio.create_task(
                    _resync_after_ha_restore(ws, ha_client, timeout=entity_fetch_timeout)
                )
                replay_task = asyncio.create_task(_replay_loop(ws, ha_client, timeout=entity_fetch_timeout))
                sightings_task = asyncio.create_task(_sightings_loop(ws))
                background: set[asyncio.Task[None]] = set()
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            env = EnvelopeAdapter.validate_python(json.loads(raw))
                        except Exception as exc:
                            log.warning("invalid envelope", error=str(exc))
                            continue
                        if not isinstance(env, Cmd):
                            continue
                        if env.op == "unpair":
                            await ws.send(Ack(type="ack", id=env.id).model_dump_json())
                            token_path.unlink(missing_ok=True)
                            log.warning("unpaired by the server, returning to pairing mode")
                            return
                        if env.op in SLOW_OPS:
                            # A WebRTC negotiation waits seconds for a camera to wake. It must never
                            # sit in front of a siren command, so it runs off the receive loop.
                            task = asyncio.create_task(
                                _handle_cmd(ws, env, ha_client, idempotency, talk=talk, recorder=recorder)
                            )
                            background.add(task)
                            task.add_done_callback(background.discard)
                        else:
                            await _handle_cmd(ws, env, ha_client, idempotency, talk=talk, recorder=recorder)
                finally:
                    hb_task.cancel()
                    send_task.cancel()
                    entity_task.cancel()
                    restore_task.cancel()
                    replay_task.cancel()
                    sightings_task.cancel()
                    for task in background:
                        task.cancel()
            attempt += 1
        except websockets.exceptions.InvalidStatus as exc:
            if exc.response.status_code == 401:
                token_path.unlink(missing_ok=True)
                log.warning("install token rejected, returning to pairing mode")
                return
            log.warning("ws handshake rejected", status=exc.response.status_code)
            attempt += 1
        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("ws closed", error=str(exc))
            attempt += 1
        except Exception as exc:
            log.error("ws_client unexpected", error=str(exc))
            attempt += 1


async def _send_loop(ws, queue: asyncio.Queue):
    while True:
        env = await queue.get()
        if isinstance(env, dict):
            await ws.send(json.dumps(env))
        else:
            await ws.send(env.model_dump_json())


async def _sync_entities(ws, ha_client, *, timeout: float, retry_interval: float) -> None:
    """Push the HA entity list to the cloud, retrying until it lands.

    Runs as a background task — never inline before the heartbeat — so a slow or
    unreachable HA can neither starve the heartbeat (which would drop the cloud
    link on ping-timeout) nor silently leave the Home with zero synced entities
    (the onboarding sensor picker would then be empty with no recovery). The
    fetch is bounded by `timeout`; any failure is logged with its reason and
    retried every `retry_interval` while the link is up.
    """
    while True:
        try:
            entities = await asyncio.wait_for(ha_client.fetch_entities(), timeout)
            entity_list = EntityList(type="entity_list", entities=entities, devices=getattr(ha_client, "devices", []))
            await ws.send(entity_list.model_dump_json())
            log.info("entity_list sent", count=len(entities))
            await _send_states(ws, ha_client, timeout)
            if ha_client.pending_resync:
                # fetch_entities just enabled registry entities; HA materialises them shortly.
                ha_client.pending_resync = False
                log.info("resync scheduled for newly enabled entities", delay=RESYNC_AFTER_ENABLE_S)
                await asyncio.sleep(RESYNC_AFTER_ENABLE_S)
                continue
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "entity_list fetch/send failed; retrying",
                error=str(exc),
                retry_in=retry_interval,
            )
            await asyncio.sleep(retry_interval)


async def _resync_after_ha_restore(ws, ha_client, *, timeout: float) -> None:
    """Resend the state snapshot each time the link to Core comes back.

    A Core restart marks entities unavailable and restores them while this add-on is still
    reconnecting, so those transitions never arrive as events. Without this a door that has
    not moved since the restart stays "silent" in Argus indefinitely.
    """
    restored = getattr(ha_client, "link_restored", None)
    if restored is None:
        return
    while True:
        await restored.wait()
        restored.clear()
        log.info("ha link restored; resending states snapshot")
        await _send_states(ws, ha_client, timeout)


async def _send_presence(ws, ha_client, timeout: float) -> None:
    """Send every person and device tracker, so the cloud knows who is home at connect time.

    Best-effort like the states snapshot: a failure is logged and the next sync tries again.
    """
    try:
        trackers = await asyncio.wait_for(ha_client.fetch_trackers(), timeout)
        await ws.send(Presence(type="presence", full=True, trackers=trackers).model_dump_json())
        log.info("presence sent", count=len(trackers))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("presence snapshot failed", error=str(exc))
        return
    await _send_replay(ws, ha_client, timeout, REPLAY_ON_CONNECT)


async def _send_replay(ws, ha_client, timeout: float, window: timedelta) -> None:
    """Send Home Assistant's record of every person's and device's comings and goings over the
    window. Best-effort: a failure is logged and the next replay covers it."""
    try:
        since = datetime.now(UTC) - window
        trackers = await asyncio.wait_for(ha_client.fetch_trackers(), timeout)
        changes, reaches = await asyncio.wait_for(ha_client.fetch_tracker_history(since, trackers), timeout * 6)
        await ws.send(Presence(type="presence", full=False, trackers=changes, replay_from=since).model_dump_json())
        # The scanner's reaches are sightings too, so a walk out the door shorter than its grace
        # shows as a gap even where the network sweep missed a sleeping phone.
        for i in range(0, len(reaches), SIGHTINGS_PER_FRAME):
            await ws.send(Sightings(type="sightings", devices=reaches[i : i + SIGHTINGS_PER_FRAME]).model_dump_json())
        log.info("presence replay sent", count=len(changes), reaches=len(reaches), hours=window.total_seconds() / 3600)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("presence replay failed", error=str(exc))


async def _sightings_loop(ws, sweeper=sweep, every: float = SWEEP_EVERY_S) -> None:
    """Sweep the home's network every minute and send who answered. A failed sweep is logged and
    the next one tries again."""
    while True:
        try:
            seen = await sweeper()
            await ws.send(Sightings(type="sightings", devices=seen).model_dump_json())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("sightings sweep failed", error=str(exc))
        await asyncio.sleep(every)


async def _replay_loop(ws, ha_client, *, timeout: float) -> None:
    while True:
        await asyncio.sleep(REPLAY_EVERY_S)
        await _send_replay(ws, ha_client, timeout, REPLAY_WINDOW)


async def _send_states(ws, ha_client, timeout: float) -> None:
    """Push a current-state snapshot so the cloud knows open/closed at connect time.

    Display-only on the backend (never runs the alarm engine). Best-effort: a failure
    is logged and skipped so it can never block the entity list or the heartbeat.
    """
    try:
        rows = await asyncio.wait_for(ha_client.fetch_states(), timeout)
        states = [StateSnapshotItem(**row) for row in rows]
        if states:
            await ws.send(StatesSnapshot(type="states", states=states).model_dump_json())
            log.info("states snapshot sent", count=len(states))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("states snapshot failed", error=str(exc))
    await _send_presence(ws, ha_client, timeout)


async def _handle_cmd(ws, cmd: Cmd, ha_client, idempotency: Idempotency, *, talk=None, recorder=None) -> None:
    if idempotency.seen(cmd.id):
        ack = Ack(type="ack", id=cmd.id, duplicate=True)
        await ws.send(ack.model_dump_json())
        return
    error: str | None = None
    result: dict[str, Any] | None = None
    try:
        if cmd.op == "call_service":
            await _dispatch_call_service(ha_client, cmd.args)
        elif cmd.op == "resync":
            entities = await ha_client.fetch_entities()
            entity_list = EntityList(type="entity_list", entities=entities, devices=getattr(ha_client, "devices", []))
            await ws.send(entity_list.model_dump_json())
            log.info("entity_list resent", count=len(entities))
            await _send_states(ws, ha_client, 10.0)
        elif cmd.op == "webrtc_offer":
            result = await _dispatch_webrtc_offer(ha_client, cmd.args)
        elif cmd.op == "webrtc_close":
            result = await _dispatch_webrtc_close(ha_client, cmd.args)
        elif cmd.op == "talk_offer":
            result = await _dispatch_talk_offer(ha_client, talk, cmd.args)
        elif cmd.op == "talk_close":
            result = await _dispatch_talk_close(talk, cmd.args)
        elif cmd.op in {"record_clip", "extend_clip", "upload_clip", "cancel_clip"}:
            result = await _dispatch_clip(recorder, ha_client, cmd.op, cmd.args)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # An exception without a message (a bare timeout) still has to tell the cloud something.
        error = str(exc) or type(exc).__name__
        log.warning("cmd failed", op=cmd.op, id=cmd.id, error=error)
    idempotency.record(cmd.id)
    ack = Ack(type="ack", id=cmd.id, error=error, result=result)
    await ws.send(ack.model_dump_json())


async def _dispatch_webrtc_offer(ha_client, args: dict[str, Any]) -> dict[str, Any]:
    entity_id = args.get("entity_id")
    sdp = args.get("sdp")
    if not isinstance(entity_id, str) or not entity_id.startswith("camera."):
        raise ValueError("webrtc_offer requires a camera entity_id")
    if not isinstance(sdp, str) or not sdp:
        raise ValueError("webrtc_offer requires sdp")
    return await ha_client.webrtc_offer(entity_id, sdp)


async def _dispatch_talk_offer(ha_client, talk, args: dict[str, Any]) -> dict[str, Any]:
    if talk is None:
        raise RuntimeError("talk is not available on this add-on")
    entity_id = args.get("entity_id")
    sdp = args.get("sdp")
    session_id = args.get("session_id")
    if not isinstance(entity_id, str) or not entity_id:
        raise ValueError("talk_offer requires a camera entity_id")
    if not isinstance(sdp, str) or not sdp:
        raise ValueError("talk_offer requires sdp")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("talk_offer requires session_id")
    return await talk.offer(entity_id, ha_client.unique_id_of(entity_id), sdp, session_id)


async def _dispatch_clip(recorder, ha_client, op: str, args: dict[str, Any]) -> dict[str, Any]:
    if recorder is None:
        raise RuntimeError("clips are not available: enter the Reolink hub login in the add-on options")
    if op == "record_clip":
        entity_id = args.get("entity_id")
        unique_id = ha_client.unique_id_of(entity_id) if isinstance(entity_id, str) else None
        return await recorder.record(args, unique_id)
    if op == "extend_clip":
        return await recorder.extend(args)
    if op == "upload_clip":
        return await recorder.upload(args)
    return await recorder.cancel(args)


async def _dispatch_talk_close(talk, args: dict[str, Any]) -> dict[str, Any]:
    if talk is None:
        raise RuntimeError("talk is not available on this add-on")
    session_id = args.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("talk_close requires session_id")
    return {"closed": await talk.close(session_id)}


async def _dispatch_webrtc_close(ha_client, args: dict[str, Any]) -> dict[str, Any]:
    session_id = args.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("webrtc_close requires session_id")
    return {"closed": await ha_client.webrtc_close(session_id)}


async def _dispatch_call_service(ha_client, args: dict[str, Any]) -> None:
    domain = args.get("domain")
    service = args.get("service")
    service_data = dict(args.get("service_data") or {})
    entity_id = args.get("entity_id")
    if entity_id and "entity_id" not in service_data:
        service_data["entity_id"] = entity_id
    if not domain or not service:
        raise ValueError("call_service requires domain and service")
    await ha_client.call_service(domain, service, service_data)
