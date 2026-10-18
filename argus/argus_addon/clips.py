"""Record a clip of what a camera sees and put it straight into the cloud's bucket.

The cloud decides and signs; this side opens the hub's RTSP main stream through the add-on's
own go2rtc, cuts the fragmented MP4 into an init and keyframe-aligned segments as it arrives,
uploads the poster first, then the init and each segment as it closes, and reports every step
as a `clip` frame. A piece that could not be uploaded stays in the spool until the cloud sends
a fresh URL. Nothing here talks to Home Assistant.
"""

from __future__ import annotations

import asyncio
import json
import shutil
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
from argus_addon.fmp4 import FragmentReader, Init, Segment, Segmenter
from argus_addon.talk import REOLINK_RTSP_PORT, Go2rtc, ReolinkHub, TalkConfig, camera_uid

log = structlog.get_logger(__name__)

STREAM_PREFIX = "clip_"
MAX_CLIP_S = 60
POSTER_TIMEOUT_S = 20.0
FIRST_BYTE_TIMEOUT_S = 25.0
STALL_TIMEOUT_S = 15.0
UPLOAD_TIMEOUT_S = 120.0
DEFAULT_SPOOL_BYTES = 300 * 1024 * 1024
DEFAULT_SEGMENT_S = 4
INIT_NAME = "init.mp4"
POSTER_NAME = "poster.jpg"
INIT_CONTENT_TYPE = "video/mp4"
SEGMENT_CONTENT_TYPE = "video/iso.segment"


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


def segment_name(index: int) -> str:
    return f"{index:03d}.m4s"


def piece_name(piece: int | str) -> str:
    return INIT_NAME if piece == "init" else segment_name(int(piece))


def _expiry(raw: Any) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(frozen=True)
class Signed:
    """Where one piece goes: the key to echo and the presigned PUT, never logged."""

    object_key: str
    put: dict[str, Any]
    expires_at: datetime | None

    @property
    def usable(self) -> bool:
        return isinstance(self.put.get("url"), str) and (self.expires_at is None or self.expires_at > datetime.now(UTC))


def _signed(raw: Any, expires_at: datetime | None) -> Signed | None:
    if not isinstance(raw, dict) or not isinstance(raw.get("object_key"), str):
        return None
    put = raw.get("put")
    return Signed(raw["object_key"], put if isinstance(put, dict) else {}, expires_at)


def _signed_segments(raw: Any, expires_at: datetime | None) -> dict[int, Signed]:
    out: dict[int, Signed] = {}
    for item in raw if isinstance(raw, list) else []:
        piece = _signed(item, expires_at)
        if piece is not None and isinstance(item.get("index"), int):
            out[item["index"]] = piece
    return out


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
    directory: Path
    args: dict[str, Any]
    init: Signed
    segments: dict[int, Signed]
    segment_s: float
    started: float = field(default_factory=time.monotonic)
    deadline: float = 0.0
    task: asyncio.Task[None] | None = None
    cancelled: bool = False
    info: dict[str, Any] = field(default_factory=dict)
    sizes: dict[str, int] = field(default_factory=dict)
    durations: dict[str, int] = field(default_factory=dict)
    uploads: asyncio.Queue[int | str | None] = field(default_factory=asyncio.Queue)
    stalled: str | None = None

    @property
    def poster_path(self) -> Path:
        return self.directory / POSTER_NAME

    def meta(self) -> dict[str, Any]:
        return {
            "segments": len(self.durations),
            "duration_ms": sum(self.durations.values()),
            "sizes": self.sizes,
            "durations": self.durations,
            **{k: self.info[k] for k in ("codec", "width", "height") if k in self.info},
        }


class Spool:
    """Pieces recorded but not yet in the bucket, one directory per clip, with an index that
    survives a restart. A clip is kept or evicted whole."""

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
        self.entries = {
            k: v
            for k, v in self.entries.items()
            if isinstance(v, dict) and isinstance(v.get("segments"), int) and self.clip_dir(k).is_dir()
        }
        # Anything else here is a recording a restart cut short, or a spool from before segments.
        for path in self.directory.iterdir():
            if path != self._index_path and path.name not in self.entries:
                shutil.rmtree(path) if path.is_dir() else path.unlink(missing_ok=True)
        self._save()

    def _save(self) -> None:
        self._index_path.write_text(json.dumps(self.entries))

    def clip_dir(self, clip_id: str) -> Path:
        return self.directory / clip_id

    def on_disk(self, clip_id: str) -> int:
        directory = self.clip_dir(clip_id)
        return sum(p.stat().st_size for p in directory.iterdir() if p.is_file()) if directory.is_dir() else 0

    def used_bytes(self) -> int:
        return sum(self.on_disk(k) for k in self.entries)

    def keep(self, clip_id: str, meta: dict[str, Any]) -> None:
        size = self.on_disk(clip_id)
        # A recording that could never fit is dropped; otherwise the oldest go until it does.
        if size > self.max_bytes:
            shutil.rmtree(self.clip_dir(clip_id), ignore_errors=True)
            raise RuntimeError("spool_full")
        while self.entries and self.used_bytes() + size > self.max_bytes:
            oldest = min(self.entries, key=lambda k: self.entries[k].get("kept_at", 0))
            self.drop(oldest)
        self.entries[clip_id] = {**meta, "kept_at": time.time()}
        self._save()

    def drop(self, clip_id: str) -> None:
        self.entries.pop(clip_id, None)
        shutil.rmtree(self.clip_dir(clip_id), ignore_errors=True)
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
        self._upload_locks: dict[str, asyncio.Lock] = {}

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
        expires_at = _expiry(args.get("url_expires_at"))
        init = _signed(args.get("init"), expires_at)
        segments = _signed_segments(args.get("segments"), expires_at)
        if init is None or not segments:
            raise ValueError("record_clip requires the init and segment uploads")
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
        directory = self.spool.clip_dir(clip_id)
        directory.mkdir(parents=True, exist_ok=True)
        rec = Recording(
            clip_id=clip_id,
            entity_id=entity_id,
            name=stream_name(entity_id),
            directory=directory,
            args=args,
            init=init,
            segments=segments,
            segment_s=max(1, int(args.get("segment_s") or DEFAULT_SEGMENT_S)),
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
        rec.segments.update(_signed_segments(args.get("segments"), _expiry(args.get("url_expires_at"))))
        by = max(1, int(args.get("by_s") or 15))
        cap = rec.started + MAX_CLIP_S
        rec.deadline = min(cap, rec.deadline + by)
        return {"extended": True, "ends_in_s": round(rec.deadline - time.monotonic(), 1), "segments": len(rec.segments)}

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
        if not isinstance(clip_id, str):
            raise ValueError("upload_clip requires clip_id")
        async with self._upload_locks.setdefault(clip_id, asyncio.Lock()):
            try:
                return await self._upload_spooled(clip_id, args)
            finally:
                self._upload_locks.pop(clip_id, None)

    async def _upload_spooled(self, clip_id: str, args: dict[str, Any]) -> dict[str, Any]:
        entry = self.spool.entries.get(clip_id)
        if entry is None:
            return {"uploaded": False, "error": "not_in_spool"}
        expires_at = _expiry(args.get("url_expires_at"))
        wanted: list[tuple[int | str, Signed]] = []
        init = _signed(args.get("init"), expires_at)
        if init is not None:
            wanted.append(("init", init))
        wanted += sorted(_signed_segments(args.get("segments"), expires_at).items())
        # A replayed upload_clip carries no URLs: the cloud never stores one. Ask for fresh ones.
        if any(not piece.usable for _, piece in wanted):
            await self._emit_spooled(clip_id, entry)
            return {"uploaded": False, "spooled": True}
        directory = self.spool.clip_dir(clip_id)
        for key, piece in wanted:
            path = directory / piece_name(key)
            if not path.exists():
                continue
            try:
                await self._put_piece(clip_id, key, piece, path, entry)
            except Exception as exc:
                log.warning("spooled upload failed", clip_id=clip_id, object_key=piece.object_key, error=str(exc))
                await self._emit_spooled(clip_id, entry)
                return {"uploaded": False, "error": "upload_failed"}
        # Whatever the cloud did not ask for again it has already verified.
        self.spool.drop(clip_id)
        await self._emit_closed(clip_id, entry)
        return {"uploaded": True}

    async def _emit_spooled(self, clip_id: str, entry: dict[str, Any]) -> None:
        await self._emit(clip_id, "spooled", segments=int(entry["segments"]), bytes=self.spool.on_disk(clip_id))

    async def _emit_closed(self, clip_id: str, meta: dict[str, Any]) -> None:
        await self._emit(
            clip_id,
            "uploaded",
            segments=int(meta["segments"]),
            bytes=sum(meta["sizes"].values()),
            duration_ms=int(meta["duration_ms"]),
        )
        self.finished.add(clip_id)

    async def resend_spooled(self) -> None:
        """After a reconnect: every spooled clip asks for fresh URLs."""
        for clip_id, entry in list(self.spool.entries.items()):
            await self._emit_spooled(clip_id, entry)

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
            shutil.rmtree(rec.directory, ignore_errors=True)
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

    async def _put_piece(self, clip_id: str, key: int | str, piece: Signed, path: Path, meta: dict[str, Any]) -> None:
        """PUT one piece, remove it from disk and say so, echoing the key exactly as signed."""
        size = path.stat().st_size
        if key == "init":
            await self._put_file(piece.put, path, INIT_CONTENT_TYPE)
            path.unlink(missing_ok=True)
            info = {k: meta[k] for k in ("codec", "width", "height") if meta.get(k) is not None}
            await self._emit(clip_id, "init_uploaded", object_key=piece.object_key, bytes=size, **info)
            return
        await self._put_file(piece.put, path, SEGMENT_CONTENT_TYPE)
        path.unlink(missing_ok=True)
        await self._emit(
            clip_id,
            "segment_uploaded",
            index=int(key),
            object_key=piece.object_key,
            bytes=size,
            duration_ms=int(meta["durations"][str(key)]),
        )

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

    def _stage(self, rec: Recording, key: int | str, data: bytes, duration_ms: int | None = None) -> None:
        """Write a closed piece to the clip's directory and queue it behind the ones before it."""
        (rec.directory / piece_name(key)).write_bytes(data)
        rec.sizes[str(key)] = len(data)
        if duration_ms is not None:
            rec.durations[str(key)] = duration_ms
        rec.uploads.put_nowait(key)

    async def _upload_loop(self, rec: Recording) -> None:
        """One PUT at a time in index order; after the first that cannot go, the rest stay on disk."""
        while (key := await rec.uploads.get()) is not None:
            if rec.stalled:
                continue
            piece = rec.init if key == "init" else rec.segments.get(int(key))
            if piece is None or not piece.usable:
                rec.stalled = "url_expired"
                continue
            try:
                await self._put_piece(rec.clip_id, key, piece, rec.directory / piece_name(key), rec.meta())
            except Exception as exc:
                log.warning(
                    "piece upload failed, spooling", clip_id=rec.clip_id, object_key=piece.object_key, error=str(exc)
                )
                rec.stalled = "upload_failed"

    async def _capture(self, rec: Recording, session: aiohttp.ClientSession) -> float | None:
        max_bytes = int(rec.args.get("max_bytes") or 60_000_000)
        written = 0
        first_at: float | None = None
        reader = FragmentReader()
        segmenter: Segmenter | None = None
        uploader = asyncio.create_task(self._upload_loop(rec))
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=STALL_TIMEOUT_S)
        try:
            async with session.get(f"{self._api}/api/stream.mp4", params={"src": rec.name}, timeout=timeout) as resp:
                if resp.status >= 300:
                    raise RuntimeError(f"stream.mp4 returned {resp.status}")
                full = False
                while not full:
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
                    written += len(chunk)
                    for item in reader.feed(chunk):
                        if isinstance(item, Init):
                            if item.video is None:
                                raise RuntimeError("the stream has no video track")
                            segmenter = Segmenter(item.video, rec.segment_s)
                            rec.info = probe_mp4(item.data)
                            self._stage(rec, "init", item.data)
                            continue
                        assert segmenter is not None
                        # A segment past the last signed index is not recorded: the clip closes here.
                        opens = segmenter.opens(item)
                        if opens is not None and opens not in rec.segments:
                            full = True
                            break
                        if closed := segmenter.push(item):
                            self._stage_segment(rec, closed)
            if segmenter is not None and (closed := segmenter.close()):
                self._stage_segment(rec, closed)
        except BaseException:
            uploader.cancel()
            await asyncio.gather(uploader, return_exceptions=True)
            raise
        rec.uploads.put_nowait(None)
        await uploader
        return first_at

    def _stage_segment(self, rec: Recording, segment: Segment) -> None:
        self._stage(rec, segment.index, segment.data, segment.duration_ms)

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
                rec.poster_path.unlink(missing_ok=True)
                first_at = await self._capture(rec, session)
            if rec.cancelled:
                shutil.rmtree(rec.directory, ignore_errors=True)
                await self._emit(rec.clip_id, "failed", reason="cancelled")
                return
            if first_at is None:
                raise RuntimeError("stream ended before the first frame")
            meta = rec.meta()
            if "init" not in rec.sizes or meta["segments"] == 0:
                shutil.rmtree(rec.directory, ignore_errors=True)
                await self._emit(rec.clip_id, "failed", reason="stream_failed", detail="no playable MP4 was produced")
                return
            if not any(rec.directory.iterdir()):
                shutil.rmtree(rec.directory, ignore_errors=True)
                await self._emit_closed(rec.clip_id, meta)
                log.info("clip uploaded", clip_id=rec.clip_id, entity_id=rec.entity_id, segments=meta["segments"])
                return
            try:
                self.spool.keep(rec.clip_id, meta)
            except RuntimeError:
                await self._emit(rec.clip_id, "failed", reason="spool_full")
                return
            log.info("clip spooled", clip_id=rec.clip_id, reason=rec.stalled, segments=meta["segments"])
            await self._emit_spooled(rec.clip_id, meta)
        except asyncio.CancelledError:
            shutil.rmtree(rec.directory, ignore_errors=True)
            raise
        except Exception as exc:
            shutil.rmtree(rec.directory, ignore_errors=True)
            reason = "camera_asleep" if "first-byte" in str(exc) else "stream_failed"
            log.warning("clip failed", clip_id=rec.clip_id, entity_id=rec.entity_id, error=str(exc))
            await self._emit(rec.clip_id, "failed", reason=reason, detail=str(exc)[:200])
        finally:
            self.in_flight.pop(rec.clip_id, None)
            if source_added:
                try:
                    await self._go2rtc.remove_stream(rec.name)
                except Exception as exc:
                    log.warning("stream removal failed", name=rec.name, error=str(exc))
