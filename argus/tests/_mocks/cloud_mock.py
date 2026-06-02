import asyncio
import json
from typing import Any

import websockets
from websockets.asyncio.server import ServerConnection, serve


class CloudMock:
    def __init__(self):
        self.received: list[dict[str, Any]] = []
        self.connections: list[ServerConnection] = []
        self._server: Any | None = None
        self._port: int = 0
        self._lock = asyncio.Lock()
        self._next_event = asyncio.Event()

    async def start(self) -> str:
        self._server = await serve(self._handler, "127.0.0.1", 0)
        sock = next(iter(self._server.sockets))
        self._port = sock.getsockname()[1]
        return self.url

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self._port}"

    async def _handler(self, conn: ServerConnection):
        async with self._lock:
            self.connections.append(conn)
        try:
            async for raw in conn:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                env = json.loads(raw)
                self.received.append(env)
                self._next_event.set()
        except websockets.ConnectionClosed:
            pass

    async def wait_for(self, predicate, timeout: float = 2.0):
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            for env in self.received:
                if predicate(env):
                    return env
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"no envelope matched in {timeout}s; received={self.received}")
            self._next_event.clear()
            try:
                await asyncio.wait_for(self._next_event.wait(), remaining)
            except TimeoutError:
                raise TimeoutError(f"no envelope matched in {timeout}s; received={self.received}") from None

    async def send_to_client(self, env: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self.connections)
        for conn in targets:
            await conn.send(json.dumps(env))

    async def drop_connections(self) -> None:
        async with self._lock:
            targets = list(self.connections)
            self.connections.clear()
        for conn in targets:
            await conn.close()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
