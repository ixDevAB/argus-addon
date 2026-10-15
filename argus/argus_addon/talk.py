"""Push-to-talk through the add-on's own go2rtc.

Home Assistant's go2rtc opens a camera's RTSP without asking for the return channel, so audio
sent through HA's WebRTC API is accepted and silently dropped. For talk the add-on runs its own
go2rtc: the hub's RTSP source with `#backchannel=1`, registered only while a talk session is
live and removed when it ends, so the hub never carries an idle extra session. The hub login
comes from the add-on options and never leaves this box.
"""

import asyncio
import json
import re
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
import structlog

log = structlog.get_logger(__name__)

GO2RTC_API = "http://127.0.0.1:1984"
GO2RTC_WEBRTC_LISTEN = ":18556"
STREAM_PREFIX = "talk_"
REOLINK_RTSP_PORT = 554
REQUEST_TIMEOUT = 15.0

GO2RTC_CONFIG = """api:
  listen: "127.0.0.1:1984"
rtsp:
  listen: ""
webrtc:
  listen: "{webrtc_listen}"
log:
  format: text
"""


class TalkConfig:
    def __init__(self, host: str = "", username: str = "", password: str = "", use_https: bool = False):
        self.host = host.strip()
        self.username = username
        self.password = password
        self.use_https = use_https

    @property
    def configured(self) -> bool:
        return bool(self.host and self.username and self.password)

    @classmethod
    def from_options(cls, path: Path) -> "TalkConfig":
        if not path.exists():
            return cls()
        try:
            raw = json.loads(path.read_text() or "{}")
        except ValueError as exc:
            log.warning("options.json unreadable", error=str(exc))
            return cls()
        return cls(
            host=str(raw.get("reolink_host") or ""),
            username=str(raw.get("reolink_username") or ""),
            password=str(raw.get("reolink_password") or ""),
            use_https=bool(raw.get("reolink_use_https") or False),
        )


REOLINK_UID = re.compile(r"^[A-Z0-9]{12,20}$")
REOLINK_STREAMS = {"sub", "main", "ext"}


def camera_uid(unique_id: str | None) -> str | None:
    """The camera's own uid out of a Reolink camera entity's unique_id (`<hub>_<camera>_<stream>`)."""
    if not unique_id:
        return None
    parts = unique_id.split("_")
    if len(parts) < 3 or parts[-1] not in REOLINK_STREAMS or not REOLINK_UID.match(parts[-2]):
        return None
    return parts[-2]


def rtsp_talk_source(config: TalkConfig, channel: int) -> str:
    path = f"Preview_{channel + 1:02d}_sub"
    return (
        f"rtsp://{quote(config.username, safe='')}:{quote(config.password, safe='')}"
        f"@{config.host}:{REOLINK_RTSP_PORT}/{path}#backchannel=1"
    )


def stream_name(entity_id: str) -> str:
    return STREAM_PREFIX + entity_id.replace(".", "_")


class ReolinkHub:
    """Just enough of the Reolink HTTP API to find which hub channel a camera uid sits on."""

    def __init__(self, config: TalkConfig, base_url: str | None = None):
        self._config = config
        self._base = base_url or f"{'https' if config.use_https else 'http'}://{config.host}"
        self._token: str | None = None
        self._channels: dict[str, int] = {}

    async def _post(self, session: aiohttp.ClientSession, cmd: str, param: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base}/api.cgi?cmd={cmd}"
        if self._token and cmd != "Login":
            url += f"&token={self._token}"
        async with session.post(
            url,
            json=[{"cmd": cmd, "action": 0, "param": param}],
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ssl=False,
        ) as resp:
            body = await resp.json(content_type=None)
        if not isinstance(body, list) or not body:
            raise RuntimeError(f"reolink {cmd}: unexpected reply")
        reply = body[0]
        if reply.get("code") != 0:
            detail = (reply.get("error") or {}).get("detail") or reply.get("code")
            raise RuntimeError(f"reolink {cmd} failed: {detail}")
        return reply.get("value") or {}

    async def _login(self, session: aiohttp.ClientSession) -> None:
        value = await self._post(
            session,
            "Login",
            {"User": {"Version": "0", "userName": self._config.username, "password": self._config.password}},
        )
        token = (value.get("Token") or {}).get("name")
        if not token:
            raise RuntimeError("reolink login returned no token")
        self._token = token

    async def refresh_channels(self) -> dict[str, int]:
        async with aiohttp.ClientSession() as session:
            if self._token is None:
                await self._login(session)
            try:
                value = await self._post(session, "GetChannelstatus", {})
            except RuntimeError:
                # The token lease ran out; log in once more.
                self._token = None
                await self._login(session)
                value = await self._post(session, "GetChannelstatus", {})
        channels: dict[str, int] = {}
        for row in value.get("status") or []:
            uid = row.get("uid")
            channel = row.get("channel")
            if isinstance(uid, str) and isinstance(channel, int):
                channels[uid] = channel
        self._channels = channels
        log.info("reolink channels resolved", count=len(channels))
        return channels

    async def channel_for(self, uid: str) -> int:
        if uid not in self._channels:
            await self.refresh_channels()
        if uid not in self._channels:
            raise LookupError(f"camera {uid} is not a channel on hub {self._config.host}")
        return self._channels[uid]


class Go2rtc:
    """The add-on's own go2rtc process and the slice of its REST API talk needs."""

    def __init__(self, config_path: Path, api_url: str = GO2RTC_API, binary: str = "go2rtc"):
        self._config_path = config_path
        self._api = api_url
        self._binary = binary
        self._proc: asyncio.subprocess.Process | None = None
        self._log_task: asyncio.Task[None] | None = None

    async def start(self) -> bool:
        if shutil.which(self._binary) is None:
            log.error("go2rtc binary missing; talk disabled", binary=self._binary)
            return False
        self._config_path.write_text(GO2RTC_CONFIG.format(webrtc_listen=GO2RTC_WEBRTC_LISTEN))
        self._proc = await asyncio.create_subprocess_exec(
            self._binary,
            "-config",
            str(self._config_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        self._log_task = asyncio.create_task(self._pump_logs())
        for _ in range(50):
            if await self.alive():
                log.info("go2rtc started", pid=self._proc.pid)
                return True
            await asyncio.sleep(0.1)
        log.error("go2rtc did not answer on its API after start")
        return False

    async def _pump_logs(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        async for line in self._proc.stdout:
            text = line.decode(errors="replace").rstrip()
            if text:
                log.info("go2rtc", line=text)
        code = await self._proc.wait()
        log.warning("go2rtc exited", code=code)

    async def stop(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), 5)
            except TimeoutError:
                self._proc.kill()
        if self._log_task is not None:
            self._log_task.cancel()

    async def alive(self) -> bool:
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"{self._api}/api", timeout=aiohttp.ClientTimeout(total=2)) as resp,
            ):
                return resp.status == 200
        except (TimeoutError, aiohttp.ClientError, OSError):
            return False

    async def add_stream(self, name: str, src: str) -> None:
        async with (
            aiohttp.ClientSession() as session,
            session.put(
                f"{self._api}/api/streams",
                params={"name": name, "src": src},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp,
        ):
            if resp.status >= 300:
                raise RuntimeError(f"go2rtc add stream failed: {resp.status} {await resp.text()}")

    async def remove_stream(self, name: str) -> None:
        async with (
            aiohttp.ClientSession() as session,
            session.delete(
                f"{self._api}/api/streams",
                params={"src": name},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp,
        ):
            if resp.status >= 300 and resp.status != 404:
                raise RuntimeError(f"go2rtc remove stream failed: {resp.status}")

    async def offer(self, name: str, sdp: str) -> str:
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                f"{self._api}/api/webrtc",
                params={"src": name},
                data=sdp,
                headers={"Content-Type": "application/sdp"},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp,
        ):
            text = await resp.text()
            if resp.status >= 300:
                raise RuntimeError(f"go2rtc offer failed: {resp.status} {text[:200]}")
        if text.lstrip().startswith("{"):
            answer = json.loads(text).get("sdp")
            if not isinstance(answer, str):
                raise RuntimeError("go2rtc offer returned no sdp")
            return answer
        return text

    async def stream_medias(self, name: str) -> list[str]:
        async with (
            aiohttp.ClientSession() as session,
            session.get(
                f"{self._api}/api/streams",
                params={"src": name},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp,
        ):
            if resp.status >= 300:
                return []
            info = await resp.json(content_type=None)
        medias: list[str] = []
        for producer in (info or {}).get("producers") or []:
            medias.extend(m for m in producer.get("medias") or [] if isinstance(m, str))
        return medias


def has_backchannel(medias: list[str]) -> bool:
    return any(m.startswith("audio, sendonly") for m in medias)


class TalkService:
    def __init__(self, config: TalkConfig, go2rtc: Go2rtc, hub: ReolinkHub | None = None):
        self.config = config
        self._go2rtc = go2rtc
        self._hub = hub or ReolinkHub(config)
        self._sessions: dict[str, str] = {}

    @property
    def enabled(self) -> bool:
        return self.config.configured

    async def offer(self, entity_id: str, unique_id: str | None, sdp: str, session_id: str) -> dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("talk is not set up: enter the Reolink hub login in the add-on options")
        uid = camera_uid(unique_id)
        if uid is None:
            raise RuntimeError(f"{entity_id} is not a Reolink hub camera")
        channel = await self._hub.channel_for(uid)
        name = stream_name(entity_id)
        # One talk per camera: a leftover stream from an unclosed session is replaced.
        await self._go2rtc.remove_stream(name)
        await self._go2rtc.add_stream(name, rtsp_talk_source(self.config, channel))
        try:
            answer = await self._go2rtc.offer(name, sdp)
        except Exception:
            await self._go2rtc.remove_stream(name)
            raise
        medias = await self._go2rtc.stream_medias(name)
        receives = has_backchannel(medias)
        self._sessions[session_id] = name
        log.info("talk session opened", entity_id=entity_id, channel=channel, camera_receives=receives)
        return {"session_id": session_id, "sdp": answer, "camera_receives": receives}

    async def close(self, session_id: str) -> bool:
        name = self._sessions.pop(session_id, None)
        if name is None:
            return False
        await self._go2rtc.remove_stream(name)
        log.info("talk session closed", session_id=session_id)
        return True

    async def close_all(self) -> None:
        for session_id in list(self._sessions):
            await self.close(session_id)
