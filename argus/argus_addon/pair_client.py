import asyncio
import json
import os
import random
import ssl
from datetime import datetime
from pathlib import Path

import structlog
import websockets

from argus_addon.envelope import PairCode, PairEnvelopeAdapter, PairToken
from argus_addon.heartbeat import HEARTBEAT_INTERVAL, heartbeat

log = structlog.get_logger(__name__)


INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0


class CodeHolder:
    """In-memory holder for the current pairing code.

    The code is held only to render it in the ingress Web UI; it is never
    persisted. On reconnect the backend issues a fresh code and the stale one
    is replaced.
    """

    def __init__(self):
        self._session_id: str | None = None
        self._code: str | None = None
        self._expires_at: datetime | None = None

    def set(self, session_id: str, code: str, expires_at: datetime) -> None:
        self._session_id = session_id
        self._code = code
        self._expires_at = expires_at

    def clear(self) -> None:
        self._session_id = None
        self._code = None
        self._expires_at = None

    @property
    def code(self) -> str | None:
        return self._code

    @property
    def expires_at(self) -> datetime | None:
        return self._expires_at

    @property
    def session_id(self) -> str | None:
        return self._session_id


def provisional_url(cloud_url: str) -> str:
    """Derive the token-less /ws/pair path from the authenticated cloud_url.

    cloud_url defaults to wss://ws.<domain>/ws/addon, so the provisional URL is
    the same origin with the final path segment swapped to /pair. No host
    literal is introduced.
    """
    return cloud_url.rsplit("/", 1)[0] + "/pair"


async def run_pairing(
    token_path: Path,
    cloud_url: str,
    code_holder: CodeHolder,
    *,
    max_backoff: float = MAX_BACKOFF,
    initial_backoff: float = INITIAL_BACKOFF,
    use_tls: bool = True,
    heartbeat_interval: float = HEARTBEAT_INTERVAL,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run the provisional pairing flow until a token is persisted.

    Connects the token-less /ws/pair path with full-jitter exponential backoff,
    displays the latest pair_code, and on pair_token persists the install token
    to token_path (0600) and returns so the caller can connect authenticated.
    Returns once a token has been written; raises if the /data write fails.
    """
    url = provisional_url(cloud_url)
    attempt = 0
    while True:
        if stop_event is not None and stop_event.is_set():
            return
        delay = min(initial_backoff * (2**attempt), max_backoff)
        if attempt:
            jitter = random.uniform(0, delay)
            log.info("pair reconnect", delay=jitter, attempt=attempt)
            await asyncio.sleep(jitter)
        try:
            ssl_ctx = ssl.create_default_context() if use_tls and url.startswith("wss://") else None
            async with websockets.connect(
                url,
                ssl=ssl_ctx,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                log.info("pair connected", attempt=attempt)
                hb_task = asyncio.create_task(heartbeat(ws, heartbeat_interval))
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        try:
                            env = PairEnvelopeAdapter.validate_python(json.loads(raw))
                        except Exception as exc:
                            log.warning("invalid pair envelope", error=str(exc))
                            continue
                        if isinstance(env, PairCode):
                            code_holder.set(env.session_id, env.code, env.expires_at)
                            log.info(
                                "pairing code",
                                code=env.code,
                                expires_at=env.expires_at.isoformat(),
                                session_id=env.session_id,
                            )
                        elif isinstance(env, PairToken):
                            _persist_token(token_path, env.install_token)
                            code_holder.clear()
                            log.info("pairing complete, token persisted")
                            return
                finally:
                    hb_task.cancel()
            attempt += 1
        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("pair ws closed", error=str(exc))
            code_holder.clear()
            attempt += 1
        except Exception as exc:
            log.error("pair_client unexpected", error=str(exc))
            code_holder.clear()
            attempt += 1


def _persist_token(token_path: Path, install_token: str) -> None:
    """Write the install token to /data, failing loud on any error.

    Never log or display the token. A persist failure must surface, not be
    swallowed into a token-less reconnect that would strand the home.
    """
    try:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        # open with 0o600 + fchmod before writing so the token is never briefly
        # readable through a default-permission window (no write-then-chmod gap)
        fd = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.fchmod(fd, 0o600)
            os.write(fd, install_token.encode())
        finally:
            os.close(fd)
    except OSError as exc:
        log.error("token persist failed", error=str(exc), path=str(token_path))
        raise
