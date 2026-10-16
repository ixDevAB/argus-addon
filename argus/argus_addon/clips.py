"""Record a clip of what a camera sees and put it straight into the cloud's bucket.

The cloud decides and signs; this side opens the hub's RTSP main stream through the add-on's
own go2rtc, writes the fragmented MP4 to a spool file, uploads the poster first and the clip
when it closes, and reports every step as a `clip` frame. A file that could not be uploaded
stays in the spool until the cloud sends a fresh URL. Nothing here talks to Home Assistant.
"""

from __future__ import annotations

import asyncio
import json
import struct
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
import structlog

from argus_addon.envelope import Clip
from argus_addon.talk import REOLINK_RTSP_PORT, Go2rtc, ReolinkHub, TalkConfig, camera_uid

log = structlog.get_logger(__name__)

STREAM_PREFIX = "clip_"
MAX_CLIP_S = 60
POSTER_TIMEOUT_S = 20.0
FIRST_BYTE_TIMEOUT_S = 25.0
STALL_TIMEOUT_S = 15.0
UPLOAD_TIMEOUT_S = 120.0
DEFAULT_SPOOL_BYTES = 300 * 1024 * 1024


def rtsp_clip_source(config: TalkConfig, channel: int, stream: str) -> str:
    suffix = "main" if stream == "main" else "sub"
    return (
        f"rtsp://{quote(config.username, safe='')}:{quote(config.password, safe='')}"
        f"@{config.host}:{REOLINK_RTSP_PORT}/Preview_{channel + 1:02d}_{suffix}"
    )


def stream_name(entity_id: str) -> str:
    return STREAM_PREFIX + entity_id.replace(".", "_")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat().replace("+00:00", "Z")


def _boxes(buf: bytes, start: int, end: int, depth: int, out: list[tuple[int, str, int, int]]) -> None:
    i = start
    while i + 8 <= end:
        size, kind = struct.unpack(">I4s", buf[i : i + 8])
        header = 8
        if size == 1 and i + 16 <= end:
            size = struct.unpack(">Q", buf[i + 8 : i + 16])[0]
            header = 16
        elif size == 0:
            size = end - i
        if size < header:
            return
        name = kind.decode("latin-1")
        out.append((depth, name, i + header, min(i + size, end)))
        if name in {"moov", "trak", "mdia", "minf", "stbl"}:
            _boxes(buf, i + header, min(i + size, end), depth + 1, out)
        elif name == "stsd":
            _boxes(buf, i + header + 8, min(i + size, end), depth + 1, out)
        i += size


def probe_mp4(head: bytes) -> dict[str, Any]:
    """Codec and size from an MP4's first bytes: the moov, which go2rtc writes before any frame."""
    found: list[tuple[int, str, int, int]] = []
    _boxes(head, 0, len(head), 0, found)
    info: dict[str, Any] = {"has_moov": any(k == "moov" for _, k, _, _ in found)}
    stsd_depth = next((d for d, k, _, _ in found if k == "stsd"), None)
    if stsd_depth is None:
        return info
    for depth, kind, body_start, _ in found:
        if depth != stsd_depth + 1:
            continue
        if kind in {"avc1", "avc3"}:
            info["codec"] = "h264"
        elif kind in {"hvc1", "hev1"}:
            info["codec"] = "h265"
        else:
            continue
        # VisualSampleEntry: 6 reserved + data_reference_index, then 16 pre-defined/reserved bytes,
        # then width and height as 16-bit integers.
        if body_start + 28 <= len(head):
            width, height = struct.unpack(">HH", head[body_start + 24 : body_start + 28])
            if width and height:
                info["width"], info["height"] = width, height
        break
    return info


@dataclass
class Recording:
    clip_id: str
    entity_id: str
    name: str
    path: Path
    poster_path: Path
    args: dict[str, Any]
    started: float = field(default_factory=time.monotonic)
    deadline: float = 0.0
    task: asyncio.Task[None] | None = None
    cancelled: bool = False


class Spool:
    """Files recorded but not yet in the bucket, with an index that survives a restart."""

    def __init__(self, directory: Path, max_bytes: int = DEFAULT_SPOOL_BYTES):
        self.directory = directory
        self.max_bytes = max_bytes
        self.directory.mkdir(parents=True, exist_ok=True)
        self._index_path = directory / "index.json"
        self.entries: dict[str, dict[str, Any]] = {}
        if self._index_path.exists():
            try:
                self.entries = json.loads(self._index_path.read_text() or "{}")
            except ValueError:
                self.entries = {}
        self.entries = {k: v for k, v in self.entries.items() if Path(v.get("path", "")).exists()}
        self._save()

    def _save(self) -> None:
        self._index_path.write_text(json.dumps(self.entries))

    def used_bytes(self) -> int:
        return sum(int(v.get("bytes", 0)) for v in self.entries.values())

    def path_for(self, clip_id: str, suffix: str) -> Path:
        return self.directory / f"{clip_id}{suffix}"

    def keep(self, clip_id: str, path: Path, object_key: str, meta: dict[str, Any]) -> None:
        size = path.stat().st_size
        # A recording that could never fit is dropped; otherwise the oldest go until it does.
        if size > self.max_bytes:
            path.unlink(missing_ok=True)
            raise RuntimeError("spool_full")
        while self.entries and self.used_bytes() + size > self.max_bytes:
            oldest = min(self.entries, key=lambda k: self.entries[k].get("kept_at", 0))
            self.drop(oldest)
        self.entries[clip_id] = {
            "path": str(path),
            "object_key": object_key,
            "bytes": size,
            "kept_at": time.time(),
            **meta,
        }
        self._save()

    def drop(self, clip_id: str) -> None:
        entry = self.entries.pop(clip_id, None)
        if entry:
            Path(entry["path"]).unlink(missing_ok=True)
        self._save()


class ClipRecorder:
    def __init__(
        self,
        config: TalkConfig,
        go2rtc: Go2rtc,
        send_queue: asyncio.Queue,
        spool: Spool,
        *,
        hub: ReolinkHub | None = None,
        go2rtc_api: str | None = None,
        max_concurrent: int = 2,
    ):
        self.config = config
        self._go2rtc = go2rtc
        self._api = go2rtc_api or getattr(go2rtc, "_api", "http://127.0.0.1:1984")
        self._hub = hub or ReolinkHub(config)
        self._queue = send_queue
        self.spool = spool
        self._max_concurrent = max_concurrent
        self.in_flight: dict[str, Recording] = {}
        self.finished: set[str] = set()

    @property
    def enabled(self) -> bool:
        return self.config.configured

    async def _emit(self, clip_id: str, status: str, **fields: Any) -> None:
        await self._queue.put(Clip(type="clip", clip_id=clip_id, status=status, **fields))

    async def record(self, args: dict[str, Any], unique_id: str | None) -> dict[str, Any]:
        clip_id = args.get("clip_id")
        entity_id = args.get("entity_id")
        if not isinstance(clip_id, str) or not isinstance(entity_id, str) or not entity_id.startswith("camera."):
            raise ValueError("record_clip requires clip_id and a camera entity_id")
        if clip_id in self.in_flight or clip_id in self.finished:
            return {"accepted": True, "duplicate": True}
        if not self.enabled:
            await self._emit(
                clip_id, "failed", reason="not_configured", detail="no Reolink hub login in the add-on options"
            )
            return {"accepted": False, "error": "not_configured"}
        if len(self.in_flight) >= self._max_concurrent:
            await self._emit(clip_id, "failed", reason="busy", detail=f"{len(self.in_flight)} recordings in flight")
            return {"accepted": False, "error": "busy"}
        uid = camera_uid(unique_id)
        if uid is None:
            await self._emit(
                clip_id, "failed", reason="stream_failed", detail=f"{entity_id} is not a Reolink hub camera"
            )
            return {"accepted": False, "error": "not_a_hub_camera"}
        duration = min(MAX_CLIP_S, max(1, int(args.get("duration_s") or 20)))
        rec = Recording(
            clip_id=clip_id,
            entity_id=entity_id,
            name=stream_name(entity_id),
            path=self.spool.path_for(clip_id, ".mp4"),
            poster_path=self.spool.path_for(clip_id, ".jpg"),
            args=args,
        )
        rec.deadline = rec.started + duration
        self.in_flight[clip_id] = rec
        rec.task = asyncio.create_task(self._run(rec, uid))
        return {"accepted": True}

    async def extend(self, args: dict[str, Any]) -> dict[str, Any]:
        clip_id = args.get("clip_id")
        rec = self.in_flight.get(clip_id) if isinstance(clip_id, str) else None
        if rec is None:
            return {"extended": False}
        by = max(1, int(args.get("by_s") or 15))
        cap = rec.started + MAX_CLIP_S
        rec.deadline = min(cap, rec.deadline + by)
        return {"extended": True, "ends_in_s": round(rec.deadline - time.monotonic(), 1)}

    async def cancel(self, args: dict[str, Any]) -> dict[str, Any]:
        clip_id = args.get("clip_id")
        rec = self.in_flight.get(clip_id) if isinstance(clip_id, str) else None
        if rec is None:
            return {"cancelled": False}
        rec.cancelled = True
        rec.deadline = 0.0
        return {"cancelled": True}

    async def upload(self, args: dict[str, Any]) -> dict[str, Any]:
        clip_id = args.get("clip_id")
        entry = self.spool.entries.get(clip_id) if isinstance(clip_id, str) else None
        if entry is None or not isinstance(clip_id, str):
            return {"uploaded": False, "error": "not_in_spool"}
        put = args.get("put") or {}
        if not isinstance(put.get("url"), str):
            raise ValueError("upload_clip requires put.url")
        path = Path(entry["path"])
        try:
            await self._put_file(put, path, "video/mp4")
        except Exception as exc:
            log.warning("spooled upload failed", clip_id=clip_id, error=str(exc))
            await self._emit(clip_id, "spooled", bytes=path.stat().st_size)
            return {"uploaded": False, "error": "upload_failed"}
        meta = {k: entry[k] for k in ("duration_ms", "codec", "width", "height") if entry.get(k) is not None}
        await self._emit(clip_id, "uploaded", object_key=entry["object_key"], bytes=path.stat().st_size, **meta)
        self.spool.drop(clip_id)
        self.finished.add(clip_id)
        return {"uploaded": True}

    async def resend_spooled(self) -> None:
        """After a reconnect: every spooled file asks for a fresh URL."""
        for clip_id, entry in list(self.spool.entries.items()):
            await self._emit(clip_id, "spooled", bytes=int(entry.get("bytes", 0)))

    async def close_all(self) -> None:
        tasks = []
        for rec in list(self.in_flight.values()):
            rec.cancelled = True
            rec.deadline = 0.0
            if rec.task:
                rec.task.cancel()
                tasks.append(rec.task)
        await asyncio.gather(*tasks, return_exceptions=True)
        # A task cancelled before it ever ran never reaches its own cleanup.
        for rec in list(self.in_flight.values()):
            rec.path.unlink(missing_ok=True)
            rec.poster_path.unlink(missing_ok=True)
        self.in_flight.clear()

    async def _put_file(self, put: dict[str, Any], path: Path, content_type: str) -> None:
        headers = {str(k): str(v) for k, v in (put.get("headers") or {}).items()}
        headers.setdefault("Content-Type", content_type)
        headers["Content-Length"] = str(path.stat().st_size)
        async with aiohttp.ClientSession() as session:
            with path.open("rb") as fh:
                async with session.put(
                    put["url"], data=fh, headers=headers, timeout=aiohttp.ClientTimeout(total=UPLOAD_TIMEOUT_S)
                ) as resp:
                    if resp.status >= 300:
                        raise RuntimeError(f"PUT returned {resp.status}: {(await resp.text())[:200]}")

    @staticmethod
    def _url_expired(args: dict[str, Any]) -> bool:
        raw = args.get("url_expires_at")
        if not isinstance(raw, str):
            return False
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")) <= datetime.now(UTC)
        except ValueError:
            return False

    async def _poster(self, rec: Recording, session: aiohttp.ClientSession) -> None:
        async with session.get(
            f"{self._api}/api/frame.jpeg",
            params={"src": rec.name},
            timeout=aiohttp.ClientTimeout(total=POSTER_TIMEOUT_S),
        ) as resp:
            if resp.status >= 300:
                raise RuntimeError(f"frame.jpeg returned {resp.status}")
            rec.poster_path.write_bytes(await resp.read())
        poster_put = rec.args.get("poster_put") or {}
        if isinstance(poster_put.get("url"), str):
            await self._put_file(poster_put, rec.poster_path, "image/jpeg")
            await self._emit(rec.clip_id, "poster_uploaded", poster_key=str(rec.args.get("poster_key") or ""))
        rec.poster_path.unlink(missing_ok=True)

    async def _capture(
        self, rec: Recording, session: aiohttp.ClientSession
    ) -> tuple[int, float | None, dict[str, Any]]:
        max_bytes = int(rec.args.get("max_bytes") or 60_000_000)
        written = 0
        first_at: float | None = None
        head = b""
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=STALL_TIMEOUT_S)
        async with session.get(f"{self._api}/api/stream.mp4", params={"src": rec.name}, timeout=timeout) as resp:
            if resp.status >= 300:
                raise RuntimeError(f"stream.mp4 returned {resp.status}")
            out = rec.path.open("wb")
            while True:
                remaining = rec.deadline - time.monotonic()
                if remaining <= 0 or rec.cancelled or written >= max_bytes:
                    break
                try:
                    chunk = await asyncio.wait_for(
                        resp.content.read(64 * 1024),
                        timeout=FIRST_BYTE_TIMEOUT_S if first_at is None else min(remaining, STALL_TIMEOUT_S),
                    )
                except TimeoutError:
                    if first_at is None:
                        raise RuntimeError("no frames before the first-byte timeout") from None
                    break
                if not chunk:
                    break
                if first_at is None:
                    first_at = time.time()
                    await self._emit(rec.clip_id, "recording", media_started_at=_iso(first_at))
                if len(head) < 65536:
                    head += chunk
                out.write(chunk)
                written += len(chunk)
            out.close()
        return written, first_at, probe_mp4(head)

    async def _run(self, rec: Recording, uid: str) -> None:
        source_added = False
        try:
            channel = await self._hub.channel_for(uid)
            await self._go2rtc.remove_stream(rec.name)
            await self._go2rtc.add_stream(
                rec.name, rtsp_clip_source(self.config, channel, str(rec.args.get("stream") or "main"))
            )
            source_added = True
            async with aiohttp.ClientSession() as session:
                try:
                    await self._poster(rec, session)
                except Exception as exc:
                    log.warning("poster failed", clip_id=rec.clip_id, error=str(exc))
                written, first_at, info = await self._capture(rec, session)
            if rec.cancelled:
                rec.path.unlink(missing_ok=True)
                await self._emit(rec.clip_id, "failed", reason="cancelled")
                return
            if first_at is None:
                raise RuntimeError("stream ended before the first frame")
            if written == 0 or not info.get("has_moov"):
                rec.path.unlink(missing_ok=True)
                await self._emit(rec.clip_id, "failed", reason="stream_failed", detail="no playable MP4 was produced")
                return
            meta = {
                "duration_ms": int((time.time() - first_at) * 1000),
                **{k: info[k] for k in ("codec", "width", "height") if k in info},
            }
            put = rec.args.get("put") or {}
            object_key = str(rec.args.get("object_key") or "")
            if self._url_expired(rec.args) or not isinstance(put.get("url"), str):
                self.spool.keep(rec.clip_id, rec.path, object_key, meta)
                await self._emit(rec.clip_id, "spooled", bytes=written)
                return
            try:
                await self._put_file(put, rec.path, "video/mp4")
            except Exception as exc:
                log.warning("clip upload failed, spooling", clip_id=rec.clip_id, error=str(exc))
                try:
                    self.spool.keep(rec.clip_id, rec.path, object_key, meta)
                except RuntimeError:
                    await self._emit(rec.clip_id, "failed", reason="spool_full")
                    return
                await self._emit(rec.clip_id, "spooled", bytes=written)
                return
            rec.path.unlink(missing_ok=True)
            await self._emit(rec.clip_id, "uploaded", object_key=object_key, bytes=written, **meta)
            self.finished.add(rec.clip_id)
            log.info("clip uploaded", clip_id=rec.clip_id, entity_id=rec.entity_id, bytes=written, **meta)
        except asyncio.CancelledError:
            rec.path.unlink(missing_ok=True)
            raise
        except Exception as exc:
            rec.path.unlink(missing_ok=True)
            reason = "camera_asleep" if "first-byte" in str(exc) else "stream_failed"
            log.warning("clip failed", clip_id=rec.clip_id, entity_id=rec.entity_id, error=str(exc))
            await self._emit(rec.clip_id, "failed", reason=reason, detail=str(exc)[:200])
        finally:
            rec.poster_path.unlink(missing_ok=True)
            self.in_flight.pop(rec.clip_id, None)
            if source_added:
                try:
                    await self._go2rtc.remove_stream(rec.name)
                except Exception as exc:
                    log.warning("stream removal failed", name=rec.name, error=str(exc))
