import json
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from argus_addon.talk import (
    Go2rtc,
    ReolinkHub,
    TalkConfig,
    TalkService,
    camera_uid,
    has_backchannel,
    rtsp_talk_source,
    stream_name,
)

CONFIG = TalkConfig(host="192.168.1.50", username="argus", password="p@ss/word")


def test_camera_uid_comes_from_the_entity_unique_id():
    assert camera_uid("9527000JQ5EC7MCL_9527000KN8I819HA_sub") == "9527000KN8I819HA"
    assert camera_uid("9527000JQ5EC7MCL_9527000KN8I819HA_main") == "9527000KN8I819HA"
    assert camera_uid("binary_sensor_only") is None
    assert camera_uid("abc_def_sub") is None
    assert camera_uid(None) is None


def test_rtsp_source_targets_the_channel_sub_stream_with_backchannel_and_escaped_login():
    assert rtsp_talk_source(CONFIG, 0) == "rtsp://argus:p%40ss%2Fword@192.168.1.50:554/Preview_01_sub#backchannel=1"
    assert rtsp_talk_source(CONFIG, 5).endswith("/Preview_06_sub#backchannel=1")
    assert stream_name("camera.living_room_lag_klarhet") == "talk_camera_living_room_lag_klarhet"


def test_has_backchannel_reads_the_producer_medias():
    assert has_backchannel(
        ["video, recvonly, H264", "audio, recvonly, MPEG4-GENERIC/16000", "audio, sendonly, PCMU/8000"]
    )
    assert not has_backchannel(["video, recvonly, H264", "audio, recvonly, MPEG4-GENERIC/16000"])


def test_config_from_options(tmp_path: Path):
    path = tmp_path / "options.json"
    assert TalkConfig.from_options(path).configured is False
    path.write_text(json.dumps({"reolink_host": " 10.0.0.9 ", "reolink_username": "u", "reolink_password": "p"}))
    cfg = TalkConfig.from_options(path)
    assert cfg.configured and cfg.host == "10.0.0.9" and cfg.use_https is False
    path.write_text(json.dumps({"reolink_host": "10.0.0.9", "reolink_username": "u", "reolink_password": ""}))
    assert TalkConfig.from_options(path).configured is False


class HubMock:
    """The two Reolink API calls talk needs, with a token that expires on demand."""

    def __init__(self):
        self.logins = 0
        self.expired = False
        self.channels = [
            {"channel": 0, "name": "Vardagsrummet", "online": 1, "uid": "9527000KN8I819HA"},
            {"channel": 3, "name": "Framsidan", "online": 1, "uid": "9527000KN8NAMAWX"},
        ]

    async def handler(self, request: web.Request) -> web.Response:
        cmd = request.query.get("cmd")
        body = await request.json()
        assert body[0]["cmd"] == cmd
        if cmd == "Login":
            self.logins += 1
            user = body[0]["param"]["User"]
            if user["password"] != "p@ss/word":
                return web.json_response([{"cmd": "Login", "code": 1, "error": {"detail": "login failed"}}])
            return web.json_response(
                [{"cmd": "Login", "code": 0, "value": {"Token": {"leaseTime": 3600, "name": "tok1"}}}]
            )
        if cmd == "GetChannelstatus":
            if self.expired or request.query.get("token") != "tok1":
                self.expired = False
                return web.json_response([{"cmd": cmd, "code": 1, "error": {"detail": "please login first"}}])
            return web.json_response([{"cmd": cmd, "code": 0, "value": {"count": 2, "status": self.channels}}])
        return web.json_response([{"cmd": cmd, "code": 1}])


class Go2rtcMock:
    def __init__(self):
        self.streams: dict[str, str] = {}
        self.offers: list[tuple[str, str]] = []
        self.medias = ["video, recvonly, H264", "audio, recvonly, MPEG4-GENERIC/16000", "audio, sendonly, PCMU/8000"]
        self.offer_fails = False

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/api", lambda r: web.json_response({"version": "mock"}))
        app.router.add_put("/api/streams", self.put_stream)
        app.router.add_delete("/api/streams", self.delete_stream)
        app.router.add_get("/api/streams", self.get_stream)
        app.router.add_post("/api/webrtc", self.webrtc)
        return app

    async def put_stream(self, request: web.Request) -> web.Response:
        self.streams[request.query["name"]] = request.query["src"]
        return web.json_response({})

    async def delete_stream(self, request: web.Request) -> web.Response:
        if request.query["src"] not in self.streams:
            return web.Response(status=404)
        self.streams.pop(request.query["src"])
        return web.Response(status=200)

    async def get_stream(self, request: web.Request) -> web.Response:
        name = request.query.get("src")
        if name not in self.streams:
            return web.Response(status=404)
        return web.json_response({"producers": [{"url": self.streams[name], "medias": self.medias}]})

    async def webrtc(self, request: web.Request) -> web.Response:
        name = request.query["src"]
        if name not in self.streams:
            return web.Response(status=404, text="stream not found")
        if self.offer_fails:
            return web.Response(status=500, text="codecs not matched")
        self.offers.append((name, await request.text()))
        return web.Response(text="v=0\r\na=answer\r\n", content_type="application/sdp")


@pytest.fixture
async def hub_mock():
    mock = HubMock()
    app = web.Application()
    app.router.add_post("/api.cgi", mock.handler)
    server = TestServer(app)
    await server.start_server()
    mock.base = f"http://127.0.0.1:{server.port}"
    try:
        yield mock
    finally:
        await server.close()


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


async def test_hub_resolves_channels_and_relogs_in_when_the_token_lapses(hub_mock):
    hub = ReolinkHub(CONFIG, base_url=hub_mock.base)
    assert await hub.channel_for("9527000KN8NAMAWX") == 3
    assert hub_mock.logins == 1
    # Cached: no further calls for a known uid.
    assert await hub.channel_for("9527000KN8I819HA") == 0
    assert hub_mock.logins == 1
    # Unknown uid forces a refresh; an expired token is handled by logging in again.
    hub_mock.expired = True
    with pytest.raises(LookupError):
        await hub.channel_for("nope")
    assert hub_mock.logins == 2


async def test_hub_wrong_password_is_a_clear_error(hub_mock):
    hub = ReolinkHub(TalkConfig(host="x", username="argus", password="wrong"), base_url=hub_mock.base)
    with pytest.raises(RuntimeError, match="login failed"):
        await hub.channel_for("9527000KN8I819HA")


async def test_talk_offer_opens_a_backchannel_stream_and_close_removes_it(hub_mock, go2rtc_mock, tmp_path: Path):
    go2rtc = Go2rtc(tmp_path / "go2rtc.yaml", api_url=go2rtc_mock.api)
    talk = TalkService(CONFIG, go2rtc, hub=ReolinkHub(CONFIG, base_url=hub_mock.base))
    result = await talk.offer(
        "camera.framsidan_lag_klarhet", "9527000JQ5EC7MCL_9527000KN8NAMAWX_sub", "v=0\r\noffer", "S1"
    )
    assert result == {"session_id": "S1", "sdp": "v=0\r\na=answer\r\n", "camera_receives": True}
    name = "talk_camera_framsidan_lag_klarhet"
    assert go2rtc_mock.streams[name] == "rtsp://argus:p%40ss%2Fword@192.168.1.50:554/Preview_04_sub#backchannel=1"
    assert go2rtc_mock.offers == [(name, "v=0\r\noffer")]

    assert await talk.close("S1") is True
    assert name not in go2rtc_mock.streams
    assert await talk.close("S1") is False


async def test_talk_offer_reports_when_the_camera_has_no_return_channel(hub_mock, go2rtc_mock, tmp_path: Path):
    go2rtc_mock.medias = ["video, recvonly, H264", "audio, recvonly, MPEG4-GENERIC/16000"]
    go2rtc = Go2rtc(tmp_path / "go2rtc.yaml", api_url=go2rtc_mock.api)
    talk = TalkService(CONFIG, go2rtc, hub=ReolinkHub(CONFIG, base_url=hub_mock.base))
    result = await talk.offer("camera.framsidan_lag_klarhet", "9527000JQ5EC7MCL_9527000KN8NAMAWX_sub", "v=0", "S2")
    assert result["camera_receives"] is False


async def test_talk_offer_failure_leaves_no_stream_behind(hub_mock, go2rtc_mock, tmp_path: Path):
    go2rtc_mock.offer_fails = True
    go2rtc = Go2rtc(tmp_path / "go2rtc.yaml", api_url=go2rtc_mock.api)
    talk = TalkService(CONFIG, go2rtc, hub=ReolinkHub(CONFIG, base_url=hub_mock.base))
    with pytest.raises(RuntimeError, match="codecs not matched"):
        await talk.offer("camera.framsidan_lag_klarhet", "9527000JQ5EC7MCL_9527000KN8NAMAWX_sub", "v=0", "S3")
    assert go2rtc_mock.streams == {}


async def test_talk_refuses_without_hub_login_or_for_a_non_hub_camera(go2rtc_mock, tmp_path: Path):
    go2rtc = Go2rtc(tmp_path / "go2rtc.yaml", api_url=go2rtc_mock.api)
    off = TalkService(TalkConfig(), go2rtc)
    assert off.enabled is False
    with pytest.raises(RuntimeError, match="not set up"):
        await off.offer("camera.x", "a_b_sub", "v=0", "S4")
    on = TalkService(CONFIG, go2rtc)
    with pytest.raises(RuntimeError, match="not a Reolink hub camera"):
        await on.offer("camera.generic", "generic", "v=0", "S5")
