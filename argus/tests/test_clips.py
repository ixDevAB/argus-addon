import asyncio
import json
import time
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from argus_addon.clips import MAX_CLIP_S, ClipRecorder, Spool, probe_mp4, rtsp_clip_source, stream_name
from argus_addon.envelope import Clip
from argus_addon.talk import Go2rtc, TalkConfig
from tests.fmp4_fixture import frames, init_segment

CONFIG = TalkConfig(host="192.168.1.50", username="argus", password="p@ss/word")


def test_probe_reads_codec_and_size_from_the_moov():
    info = probe_mp4(init_segment() + b"".join(frames(1)))
    assert info == {"has_moov": True, "codec": "h264", "width": 2560, "height": 1440}
    assert probe_mp4(init_segment(b"hvc1", 3840, 2160))["codec"] == "h265"
    assert probe_mp4(b"".join(frames(1))) == {"has_moov": False}


def test_sources_and_names():
    assert rtsp_clip_source(CONFIG, 2, "main") == "rtsp://argus:p%40ss%2Fword@192.168.1.50:554/Preview_03_main"
    assert rtsp_clip_source(CONFIG, 0, "sub").endswith("/Preview_01_sub")
    assert stream_name("camera.entre_lag_klarhet") == "clip_camera_entre_lag_klarhet"


class Go2rtcMock:
    """frame.jpeg answers at once; stream.mp4 sends the init, then `video_frames` frames with their
    audio, then either ends or holds the connection open until the client goes away."""

    def __init__(self):
        self.streams: dict[str, str] = {}
        self.added: list[tuple[str, str]] = []
        self.fragment_interval = 0.001
        self.video_frames = 100
        self.hold_open = False
        self.fail_stream = False
        self.never_send = False

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/api", lambda r: web.json_response({"version": "mock"}))
        app.router.add_put("/api/streams", self.put_stream)
        app.router.add_delete("/api/streams", self.delete_stream)
        app.router.add_get("/api/frame.jpeg", self.frame)
        app.router.add_get("/api/stream.mp4", self.stream)
        return app

    async def put_stream(self, request: web.Request):
        self.streams[request.query["name"]] = request.query["src"]
        self.added.append((request.query["name"], request.query["src"]))
        return web.Response(text="ok")

    async def delete_stream(self, request: web.Request):
        self.streams.pop(request.query["src"], None)
        return web.Response(text="ok")

    async def frame(self, request: web.Request):
        if request.query["src"] not in self.streams:
            return web.Response(status=404)
        return web.Response(body=b"\xff\xd8poster\xff\xd9", content_type="image/jpeg")

    async def stream(self, request: web.Request):
        if self.fail_stream or request.query["src"] not in self.streams:
            return web.Response(status=500, text="stream failed")
        resp = web.StreamResponse(headers={"Content-Type": "video/mp4"})
        await resp.prepare(request)
        if self.never_send:
            await asyncio.sleep(60)
            return resp
        try:
            await resp.write(init_segment())
            for frag in frames(self.video_frames):
                await resp.write(frag)
                await asyncio.sleep(self.fragment_interval)
            if self.hold_open:
                await asyncio.sleep(60)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return resp


class BucketMock:
    def __init__(self):
        self.puts: list[tuple[str, str, int]] = []
        self.bodies: dict[str, bytes] = {}
        self.fail = False
        self.fail_after: int | None = None

    def app(self) -> web.Application:
        app = web.Application(client_max_size=256 * 1024 * 1024)
        app.router.add_put("/{key:.*}", self.put)
        return app

    async def put(self, request: web.Request):
        body = await request.read()
        if self.fail or (self.fail_after is not None and len(self.puts) >= self.fail_after):
            return web.Response(status=503, text="later")
        key = request.match_info["key"]
        self.puts.append((key, request.headers.get("Content-Type", ""), len(body)))
        self.bodies[key] = body
        return web.Response(status=200)


class HubMock:
    async def channel_for(self, uid: str) -> int:
        return 2


@pytest.fixture
async def go2rtc_mock():
    mock = Go2rtcMock()
    server = TestServer(mock.app())
    await server.start_server()
    mock.api = f"http://127.0.0.1:{server.port}"
    try:
        yield mock
    finally:
        await server.close()


@pytest.fixture
async def bucket_mock():
    mock = BucketMock()
    server = TestServer(mock.app())
    await server.start_server()
    mock.base = f"http://127.0.0.1:{server.port}"
    try:
        yield mock
    finally:
        await server.close()


def make_recorder(go2rtc_mock, tmp_path: Path, **kw) -> tuple[ClipRecorder, asyncio.Queue]:
    queue: asyncio.Queue = asyncio.Queue()
    go2rtc = Go2rtc(tmp_path / "go2rtc.yaml", api_url=go2rtc_mock.api)
    spool = Spool(tmp_path / "spool", max_bytes=kw.pop("spool_bytes", 10 * 1024 * 1024))
    rec = ClipRecorder(CONFIG, go2rtc, queue, spool, hub=HubMock(), go2rtc_api=go2rtc_mock.api, **kw)
    return rec, queue


def key(clip_id: str, name: str) -> str:
    return f"homes/h/clips/{clip_id}/{name}"


def init_piece(bucket_mock, clip_id: str, url: bool = True) -> dict:
    put = {"headers": {"Content-Type": "video/mp4"}}
    if url:
        put["url"] = f"{bucket_mock.base}/{key(clip_id, 'init.mp4')}"
    return {"object_key": key(clip_id, "init.mp4"), "put": put}


def segment_pieces(bucket_mock, clip_id: str, indices, url: bool = True) -> list[dict]:
    out = []
    for i in indices:
        put = {"headers": {"Content-Type": "video/iso.segment"}}
        if url:
            put["url"] = f"{bucket_mock.base}/{key(clip_id, f'{i:03d}.m4s')}"
        out.append({"index": i, "object_key": key(clip_id, f"{i:03d}.m4s"), "put": put})
    return out


def record_args(bucket_mock, clip_id="c1", duration_s=30, signed=4, **extra):
    return {
        "clip_id": clip_id,
        "entity_id": "camera.entre",
        "stream": "main",
        "duration_s": duration_s,
        "segment_s": 4,
        "max_bytes": 60_000_000,
        "init": init_piece(bucket_mock, clip_id),
        "segments": segment_pieces(bucket_mock, clip_id, range(signed)),
        "poster_put": {
            "url": f"{bucket_mock.base}/{key(clip_id, 'poster.jpg')}",
            "headers": {"Content-Type": "image/jpeg"},
        },
        "url_expires_at": "2099-01-01T00:00:00Z",
        "poster_key": key(clip_id, "poster.jpg"),
        **extra,
    }


async def drain(queue: asyncio.Queue, timeout: float = 5.0) -> list[Clip]:
    out: list[Clip] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            out.append(await asyncio.wait_for(queue.get(), timeout=0.2))
        except TimeoutError:
            if out and out[-1].status in {"uploaded", "failed", "spooled"}:
                break
    return out


UID = "9527000JQ5EC7MCL_9527000KN8I819HA_main"


async def test_records_the_poster_then_uploads_the_init_and_each_segment_as_it_closes(
    go2rtc_mock, bucket_mock, tmp_path
):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    assert await rec.record(record_args(bucket_mock), UID) == {"accepted": True}
    frames_ = await drain(queue)
    assert go2rtc_mock.added == [("clip_camera_entre", rtsp_clip_source(CONFIG, 2, "main"))]
    assert [f.status for f in frames_] == [
        "poster_uploaded",
        "recording",
        "init_uploaded",
        "segment_uploaded",
        "segment_uploaded",
        "uploaded",
    ]
    assert frames_[1].media_started_at and frames_[1].media_started_at.endswith("Z")
    init = frames_[2]
    assert init.object_key == key("c1", "init.mp4") and init.bytes == len(init_segment())
    assert init.codec == "h264" and init.width == 2560 and init.height == 1440
    segs = frames_[3:5]
    assert [(f.index, f.object_key, f.duration_ms) for f in segs] == [
        (0, key("c1", "000.m4s"), 5760),
        (1, key("c1", "001.m4s"), 2240),
    ]
    done = frames_[5]
    assert done.segments == 2 and done.duration_ms == 8000
    assert done.bytes == init.bytes + sum(f.bytes or 0 for f in segs)
    assert [(k, ct) for k, ct, _ in bucket_mock.puts] == [
        (key("c1", "poster.jpg"), "image/jpeg"),
        (key("c1", "init.mp4"), "video/mp4"),
        (key("c1", "000.m4s"), "video/iso.segment"),
        (key("c1", "001.m4s"), "video/iso.segment"),
    ]
    # The pieces joined are the stream go2rtc sent, byte for byte.
    joined = b"".join(bucket_mock.bodies[key("c1", n)] for n in ("init.mp4", "000.m4s", "001.m4s"))
    assert joined == init_segment() + b"".join(frames(100))
    # The stream is released, nothing is left on disk, a repeat is a duplicate.
    assert "clip_camera_entre" not in go2rtc_mock.streams
    assert [p.name for p in (tmp_path / "spool").iterdir()] == ["index.json"]
    assert await rec.record(record_args(bucket_mock), UID) == {"accepted": True, "duplicate": True}


async def test_a_clip_closes_before_a_segment_nobody_signed(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.hold_open = True
    await rec.record(record_args(bucket_mock, signed=1), UID)
    frames_ = await drain(queue)
    assert frames_[-1].status == "uploaded" and frames_[-1].segments == 1 and frames_[-1].duration_ms == 5760
    assert key("c1", "001.m4s") not in bucket_mock.bodies


async def test_extend_signs_more_segments_and_pushes_the_deadline_up_to_the_cap(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.fragment_interval = 0.005
    await rec.record(record_args(bucket_mock, duration_s=1, signed=1), UID)
    started = rec.in_flight["c1"].started
    out = await rec.extend(
        {
            "clip_id": "c1",
            "by_s": 10,
            "segments": segment_pieces(bucket_mock, "c1", [1, 2]),
            "url_expires_at": "2099-01-01T00:00:00Z",
        }
    )
    assert out["extended"] is True and out["segments"] == 3
    assert rec.in_flight["c1"].deadline == pytest.approx(started + 11, abs=0.01)
    frames_ = await drain(queue)
    assert frames_[-1].status == "uploaded" and frames_[-1].segments == 2
    assert key("c1", "001.m4s") in bucket_mock.bodies
    assert (await rec.extend({"clip_id": "nope"})) == {"extended": False}


async def test_extend_stops_at_the_cap_and_cancel_discards(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.hold_open = True
    go2rtc_mock.video_frames = 10
    await rec.record(record_args(bucket_mock, duration_s=1), UID)
    started = rec.in_flight["c1"].started
    assert (await rec.extend({"clip_id": "c1", "by_s": 500}))["ends_in_s"] <= MAX_CLIP_S
    assert rec.in_flight["c1"].deadline == pytest.approx(started + MAX_CLIP_S, abs=0.01)
    await rec.cancel({"clip_id": "c1"})
    frames_ = await drain(queue)
    assert frames_[-1].status == "failed" and frames_[-1].reason == "cancelled"
    assert not (tmp_path / "spool" / "c1").exists()


async def test_a_failed_upload_spools_the_rest_and_fresh_urls_finish_it(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    bucket_mock.fail_after = 2
    await rec.record(record_args(bucket_mock), UID)
    frames_ = await drain(queue)
    assert [f.status for f in frames_] == ["poster_uploaded", "recording", "init_uploaded", "spooled"]
    spooled = frames_[-1]
    assert spooled.segments == 2 and spooled.bytes == sum(
        p.stat().st_size for p in (tmp_path / "spool" / "c1").iterdir()
    )
    assert sorted(p.name for p in (tmp_path / "spool" / "c1").iterdir()) == ["000.m4s", "001.m4s"]

    # A restart reloads the index and a reconnect re-announces it.
    reloaded = Spool(tmp_path / "spool")
    assert set(reloaded.entries) == {"c1"}
    await rec.resend_spooled()
    assert (await queue.get()).segments == 2

    # A replayed upload_clip carries no URLs: the add-on asks for fresh ones.
    replay = {"clip_id": "c1", "segments": segment_pieces(bucket_mock, "c1", [0, 1], url=False)}
    assert await rec.upload(replay) == {"uploaded": False, "spooled": True}
    assert (await queue.get()).status == "spooled"

    bucket_mock.fail_after = None
    fresh = {
        "clip_id": "c1",
        "segments": segment_pieces(bucket_mock, "c1", [0, 1]),
        "url_expires_at": "2099-01-01T00:00:00Z",
    }
    assert await rec.upload(fresh) == {"uploaded": True}
    after = [await queue.get() for _ in range(3)]
    assert [(f.status, f.index, f.duration_ms) for f in after] == [
        ("segment_uploaded", 0, 5760),
        ("segment_uploaded", 1, 2240),
        ("uploaded", None, 8000),
    ]
    assert after[2].segments == 2 and after[2].bytes == len(init_segment()) + len(b"".join(frames(100)))
    assert "c1" not in rec.spool.entries and not (tmp_path / "spool" / "c1").exists()
    assert await rec.upload(fresh) == {"uploaded": False, "error": "not_in_spool"}


async def test_an_expired_url_spools_without_trying(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    await rec.record(record_args(bucket_mock, url_expires_at="2000-01-01T00:00:00Z"), UID)
    frames_ = await drain(queue)
    assert frames_[-1].status == "spooled" and frames_[-1].segments == 2
    assert [k for k, _, _ in bucket_mock.puts] == [key("c1", "poster.jpg")]
    assert sorted(p.name for p in (tmp_path / "spool" / "c1").iterdir()) == ["000.m4s", "001.m4s", "init.mp4"]


def spooled_clip(spool: Spool, clip_id: str, size: int) -> None:
    directory = spool.clip_dir(clip_id)
    directory.mkdir(parents=True)
    (directory / "000.m4s").write_bytes(b"x" * size)
    spool.keep(clip_id, {"segments": 1, "duration_ms": 4000, "sizes": {"0": size}, "durations": {"0": 4000}})


async def test_the_spool_evicts_oldest_first_and_refuses_what_never_fits(tmp_path):
    spool = Spool(tmp_path / "spool", max_bytes=10_000)
    for i in range(3):
        spooled_clip(spool, f"c{i}", 4_000)
    assert set(spool.entries) == {"c1", "c2"}
    assert not spool.clip_dir("c0").exists()
    with pytest.raises(RuntimeError, match="spool_full"):
        spooled_clip(spool, "big", 20_000)
    assert not spool.clip_dir("big").exists()
    assert json.loads((tmp_path / "spool" / "index.json").read_text()).keys() == {"c1", "c2"}


async def test_a_restart_clears_what_the_index_does_not_hold(tmp_path):
    spool = Spool(tmp_path / "spool")
    spooled_clip(spool, "kept", 100)
    (tmp_path / "spool" / "cut_short").mkdir()
    (tmp_path / "spool" / "cut_short" / "000.m4s").write_bytes(b"x")
    (tmp_path / "spool" / "old.mp4").write_bytes(b"x")
    reloaded = Spool(tmp_path / "spool")
    assert set(reloaded.entries) == {"kept"}
    assert sorted(p.name for p in (tmp_path / "spool").iterdir()) == ["index.json", "kept"]


async def test_failures_are_reported_with_a_reason_and_the_stream_is_released(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.fail_stream = True
    await rec.record(record_args(bucket_mock), UID)
    frames_ = await drain(queue)
    assert frames_[-1].status == "failed" and frames_[-1].reason == "stream_failed"
    assert "clip_camera_entre" not in go2rtc_mock.streams
    assert "c1" not in rec.in_flight and not (tmp_path / "spool" / "c1").exists()

    not_hub = await rec.record(record_args(bucket_mock, clip_id="c2"), None)
    assert not_hub["accepted"] is False
    assert (await queue.get()).reason == "stream_failed"

    with pytest.raises(ValueError, match="init and segment"):
        await rec.record({**record_args(bucket_mock, clip_id="c4"), "segments": []}, UID)

    rec.config = TalkConfig()
    off = await rec.record(record_args(bucket_mock, clip_id="c3"), UID)
    assert off == {"accepted": False, "error": "not_configured"}
    assert (await queue.get()).reason == "not_configured"


async def test_at_most_two_recordings_run_at_once(go2rtc_mock, bucket_mock, tmp_path):
    rec, _queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.hold_open = True
    await rec.record(record_args(bucket_mock, clip_id="a", duration_s=2), "x_9527000KN8I819HA_main")
    await rec.record(
        {**record_args(bucket_mock, clip_id="b", duration_s=2), "entity_id": "camera.two"}, "x_9527000KN8I819HB_main"
    )
    third = await rec.record(
        {**record_args(bucket_mock, clip_id="c"), "entity_id": "camera.three"}, "x_9527000KN8I819HC_main"
    )
    assert third == {"accepted": False, "error": "busy"}
    await rec.close_all()
    await asyncio.sleep(0.1)
    assert rec.in_flight == {}
    assert [p.name for p in (tmp_path / "spool").iterdir()] == ["index.json"]
