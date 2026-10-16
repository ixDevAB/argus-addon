import asyncio
import json
import struct
import time
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from argus_addon.clips import MAX_CLIP_S, ClipRecorder, Spool, probe_mp4, rtsp_clip_source, stream_name
from argus_addon.envelope import Clip
from argus_addon.talk import Go2rtc, TalkConfig

CONFIG = TalkConfig(host="192.168.1.50", username="argus", password="p@ss/word")


def box(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


def fake_init_segment(codec: bytes = b"avc1", width: int = 2560, height: int = 1440) -> bytes:
    entry = box(codec, b"\x00" * 6 + b"\x00\x01" + b"\x00" * 16 + struct.pack(">HH", width, height) + b"\x00" * 50)
    stsd = box(b"stsd", b"\x00" * 4 + struct.pack(">I", 1) + entry)
    moov = box(b"moov", box(b"trak", box(b"mdia", box(b"minf", box(b"stbl", stsd)))))
    return box(b"ftyp", b"iso5") + moov


def fake_fragment(n: int) -> bytes:
    return box(b"moof", b"\x00" * 16) + box(b"mdat", bytes([n % 256]) * 4096)


def test_probe_reads_codec_and_size_from_the_moov():
    info = probe_mp4(fake_init_segment() + fake_fragment(1))
    assert info == {"has_moov": True, "codec": "h264", "width": 2560, "height": 1440}
    assert probe_mp4(fake_init_segment(b"hvc1", 3840, 2160))["codec"] == "h265"
    assert probe_mp4(fake_fragment(1)) == {"has_moov": False}


def test_sources_and_names():
    assert rtsp_clip_source(CONFIG, 2, "main") == "rtsp://argus:p%40ss%2Fword@192.168.1.50:554/Preview_03_main"
    assert rtsp_clip_source(CONFIG, 0, "sub").endswith("/Preview_01_sub")
    assert stream_name("camera.entre_lag_klarhet") == "clip_camera_entre_lag_klarhet"


class Go2rtcMock:
    """frame.jpeg answers at once; stream.mp4 streams fragments until the client goes away."""

    def __init__(self):
        self.streams: dict[str, str] = {}
        self.added: list[tuple[str, str]] = []
        self.fragment_interval = 0.02
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
        await resp.write(fake_init_segment())
        n = 0
        try:
            while True:
                await resp.write(fake_fragment(n))
                n += 1
                await asyncio.sleep(self.fragment_interval)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return resp


class BucketMock:
    def __init__(self):
        self.puts: list[tuple[str, str, int]] = []
        self.fail = False

    def app(self) -> web.Application:
        app = web.Application(client_max_size=256 * 1024 * 1024)
        app.router.add_put("/{key:.*}", self.put)
        return app

    async def put(self, request: web.Request):
        body = await request.read()
        if self.fail:
            return web.Response(status=503, text="later")
        self.puts.append((request.match_info["key"], request.headers.get("Content-Type", ""), len(body)))
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


def record_args(bucket_mock, clip_id="c1", duration_s=1, **extra):
    return {
        "clip_id": clip_id,
        "entity_id": "camera.entre",
        "stream": "main",
        "duration_s": duration_s,
        "max_bytes": 60_000_000,
        "put": {"url": f"{bucket_mock.base}/homes/h/clips/{clip_id}/000.mp4", "headers": {"Content-Type": "video/mp4"}},
        "poster_put": {
            "url": f"{bucket_mock.base}/homes/h/clips/{clip_id}/poster.jpg",
            "headers": {"Content-Type": "image/jpeg"},
        },
        "url_expires_at": "2099-01-01T00:00:00Z",
        "object_key": f"homes/h/clips/{clip_id}/000.mp4",
        "poster_key": f"homes/h/clips/{clip_id}/poster.jpg",
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


async def test_records_posters_first_then_uploads_the_clip_and_reports_every_step(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    result = await rec.record(record_args(bucket_mock), "9527000JQ5EC7MCL_9527000KN8I819HA_main")
    assert result == {"accepted": True}
    frames = await drain(queue)
    assert go2rtc_mock.added == [("clip_camera_entre", rtsp_clip_source(CONFIG, 2, "main"))]
    assert [f.status for f in frames] == ["poster_uploaded", "recording", "uploaded"]
    assert frames[0].poster_key == "homes/h/clips/c1/poster.jpg"
    assert frames[1].media_started_at and frames[1].media_started_at.endswith("Z")
    done = frames[2]
    assert done.object_key == "homes/h/clips/c1/000.mp4"
    assert done.codec == "h264" and done.width == 2560 and done.height == 1440
    assert done.bytes and done.bytes > 4096 and 800 <= (done.duration_ms or 0) <= 3000
    assert [(k, ct) for k, ct, _ in bucket_mock.puts] == [
        ("homes/h/clips/c1/poster.jpg", "image/jpeg"),
        ("homes/h/clips/c1/000.mp4", "video/mp4"),
    ]
    assert bucket_mock.puts[1][2] == done.bytes
    # The stream is released, the spool is empty, a repeat is a duplicate.
    assert "clip_camera_entre" not in go2rtc_mock.streams
    assert not list((tmp_path / "spool").glob("*.mp4"))
    assert await rec.record(record_args(bucket_mock), "x_9527000KN8I819HA_main") == {
        "accepted": True,
        "duplicate": True,
    }


async def test_extend_pushes_the_deadline_up_to_the_cap(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    await rec.record(record_args(bucket_mock, duration_s=1), "x_9527000KN8I819HA_main")
    started = rec.in_flight["c1"].started
    assert (await rec.extend({"clip_id": "c1", "by_s": 1}))["extended"] is True
    assert rec.in_flight["c1"].deadline == pytest.approx(started + 2, abs=0.01)
    assert (await rec.extend({"clip_id": "c1", "by_s": 500}))["ends_in_s"] <= MAX_CLIP_S
    assert rec.in_flight["c1"].deadline == pytest.approx(started + MAX_CLIP_S, abs=0.01)
    assert (await rec.extend({"clip_id": "nope"})) == {"extended": False}
    await rec.cancel({"clip_id": "c1"})
    frames = await drain(queue)
    assert frames[-1].status == "failed" and frames[-1].reason == "cancelled"


async def test_a_failed_upload_spools_the_file_and_a_fresh_url_finishes_it(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    bucket_mock.fail = True
    await rec.record(record_args(bucket_mock), "x_9527000KN8I819HA_main")
    frames = await drain(queue)
    assert frames[-1].status == "spooled" and frames[-1].bytes
    assert "c1" in rec.spool.entries and Path(rec.spool.entries["c1"]["path"]).exists()

    # A restart reloads the index and a reconnect re-announces it.
    reloaded = Spool(tmp_path / "spool")
    assert set(reloaded.entries) == {"c1"}
    await rec.resend_spooled()
    assert (await queue.get()).status == "spooled"

    bucket_mock.fail = False
    out = await rec.upload(
        {
            "clip_id": "c1",
            "put": {"url": f"{bucket_mock.base}/homes/h/clips/c1/000.mp4", "headers": {"Content-Type": "video/mp4"}},
        }
    )
    assert out == {"uploaded": True}
    done = await queue.get()
    assert done.status == "uploaded" and done.object_key == "homes/h/clips/c1/000.mp4" and done.codec == "h264"
    assert "c1" not in rec.spool.entries
    assert await rec.upload({"clip_id": "c1", "put": {"url": "x"}}) == {"uploaded": False, "error": "not_in_spool"}


async def test_an_expired_url_spools_without_trying(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    await rec.record(record_args(bucket_mock, url_expires_at="2000-01-01T00:00:00Z"), "x_9527000KN8I819HA_main")
    frames = await drain(queue)
    assert frames[-1].status == "spooled"
    assert [k for k, _, _ in bucket_mock.puts] == ["homes/h/clips/c1/poster.jpg"]


async def test_the_spool_evicts_oldest_first_and_refuses_what_never_fits(tmp_path):
    spool = Spool(tmp_path / "spool", max_bytes=10_000)
    for i, size in enumerate((4_000, 4_000, 4_000)):
        p = spool.path_for(f"c{i}", ".mp4")
        p.write_bytes(b"x" * size)
        spool.keep(f"c{i}", p, f"k{i}", {})
    assert set(spool.entries) == {"c1", "c2"}
    assert not spool.path_for("c0", ".mp4").exists()
    big = spool.path_for("big", ".mp4")
    big.write_bytes(b"x" * 20_000)
    with pytest.raises(RuntimeError, match="spool_full"):
        spool.keep("big", big, "kb", {})
    assert not big.exists()
    assert json.loads((tmp_path / "spool" / "index.json").read_text()).keys() == {"c1", "c2"}


async def test_failures_are_reported_with_a_reason_and_the_stream_is_released(go2rtc_mock, bucket_mock, tmp_path):
    rec, queue = make_recorder(go2rtc_mock, tmp_path)
    go2rtc_mock.fail_stream = True
    await rec.record(record_args(bucket_mock), "x_9527000KN8I819HA_main")
    frames = await drain(queue)
    assert frames[-1].status == "failed" and frames[-1].reason == "stream_failed"
    assert "clip_camera_entre" not in go2rtc_mock.streams
    assert "c1" not in rec.in_flight

    not_hub = await rec.record(record_args(bucket_mock, clip_id="c2"), None)
    assert not_hub["accepted"] is False
    assert (await queue.get()).reason == "stream_failed"

    rec.config = TalkConfig()
    off = await rec.record(record_args(bucket_mock, clip_id="c3"), "x_9527000KN8I819HA_main")
    assert off == {"accepted": False, "error": "not_configured"}
    assert (await queue.get()).reason == "not_configured"


async def test_at_most_two_recordings_run_at_once(go2rtc_mock, bucket_mock, tmp_path):
    rec, _queue = make_recorder(go2rtc_mock, tmp_path)
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
