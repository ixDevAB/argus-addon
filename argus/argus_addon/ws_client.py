import asyncio
import json
import random
import ssl
from pathlib import Path
from typing import Any

import structlog
import websockets

from argus_addon import __version__
from argus_addon.envelope import Ack, Cmd, EntityList, EnvelopeAdapter, Hello
from argus_addon.heartbeat import HEARTBEAT_INTERVAL, heartbeat
from argus_addon.idempotency import Idempotency

log = structlog.get_logger(__name__)


INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0


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
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            env = EnvelopeAdapter.validate_python(json.loads(raw))
                        except Exception as exc:
                            log.warning("invalid envelope", error=str(exc))
                            continue
                        if isinstance(env, Cmd):
                            await _handle_cmd(ws, env, ha_client, idempotency)
                finally:
                    hb_task.cancel()
                    send_task.cancel()
                    entity_task.cancel()
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


async def _handle_cmd(ws, cmd: Cmd, ha_client, idempotency: Idempotency) -> None:
    if idempotency.seen(cmd.id):
        ack = Ack(type="ack", id=cmd.id, duplicate=True)
        await ws.send(ack.model_dump_json())
        return
    error: str | None = None
    try:
        if cmd.op == "call_service":
            await _dispatch_call_service(ha_client, cmd.args)
        elif cmd.op == "resync":
            entities = await ha_client.fetch_entities()
            entity_list = EntityList(type="entity_list", entities=entities)
            await ws.send(entity_list.model_dump_json())
            log.info("entity_list resent", count=len(entities))
    except Exception as exc:
        error = str(exc)
    idempotency.record(cmd.id)
    ack = Ack(type="ack", id=cmd.id, error=error)
    await ws.send(ack.model_dump_json())


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
