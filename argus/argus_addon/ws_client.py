import asyncio
import json
import random
import ssl
from pathlib import Path
from typing import Any

import structlog
import websockets

from argus_addon import __version__
from argus_addon.envelope import Ack, Cmd, EntityList, EnvelopeAdapter, Hello, StateSnapshotItem, StatesSnapshot
from argus_addon.heartbeat import HEARTBEAT_INTERVAL, heartbeat
from argus_addon.idempotency import Idempotency

log = structlog.get_logger(__name__)


INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0

# Commands that block on Home Assistant for seconds and therefore run as background tasks.
SLOW_OPS = {"webrtc_offer", "webrtc_close"}

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
                        if env.op in SLOW_OPS:
                            # A WebRTC negotiation waits seconds for a camera to wake. It must never
                            # sit in front of a siren command, so it runs off the receive loop.
                            task = asyncio.create_task(_handle_cmd(ws, env, ha_client, idempotency))
                            background.add(task)
                            task.add_done_callback(background.discard)
                        else:
                            await _handle_cmd(ws, env, ha_client, idempotency)
                finally:
                    hb_task.cancel()
                    send_task.cancel()
                    entity_task.cancel()
                    for task in background:
                        task.cancel()
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
            entity_list = EntityList(type="entity_list", entities=entities)
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


async def _handle_cmd(ws, cmd: Cmd, ha_client, idempotency: Idempotency) -> None:
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
            entity_list = EntityList(type="entity_list", entities=entities)
            await ws.send(entity_list.model_dump_json())
            log.info("entity_list resent", count=len(entities))
            await _send_states(ws, ha_client, 10.0)
        elif cmd.op == "webrtc_offer":
            result = await _dispatch_webrtc_offer(ha_client, cmd.args)
        elif cmd.op == "webrtc_close":
            result = await _dispatch_webrtc_close(ha_client, cmd.args)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error = str(exc)
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
