"""Cut go2rtc's fragmented MP4 into the pieces a clip is uploaded as.

`stream.mp4` is `ftyp` and `moov`, then one `moof`+`mdat` pair per sample. The init is the bytes
before the first `moof`; a segment is a run of whole pairs that starts at a video sync sample.
The init followed by the segments in order is the stream byte for byte, apart from anything
before the first video keyframe, which no player could decode.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

SAMPLE_NON_SYNC = 0x00010000


@dataclass(frozen=True)
class Track:
    track_id: int
    handler: str
    timescale: int
    default_duration: int = 0
    default_flags: int = 0


@dataclass(frozen=True)
class Init:
    data: bytes
    tracks: dict[int, Track]

    @property
    def video(self) -> Track | None:
        return next((t for t in self.tracks.values() if t.handler == "vide"), None)


@dataclass(frozen=True)
class Fragment:
    data: bytes
    track_id: int
    sync: bool
    start: int
    duration: int


@dataclass(frozen=True)
class Segment:
    index: int
    data: bytes
    duration_ms: int


def _children(buf: bytes, start: int, end: int):
    i = start
    while i + 8 <= end:
        size, kind = struct.unpack_from(">I4s", buf, i)
        header = 8
        if size == 1:
            if i + 16 > end:
                return
            size = struct.unpack_from(">Q", buf, i + 8)[0]
            header = 16
        elif size == 0:
            size = end - i
        if size < header or i + size > end:
            return
        yield kind.decode("latin-1"), i + header, i + size
        i += size


def _child(buf: bytes, start: int, end: int, name: str) -> tuple[int, int] | None:
    return next(((s, e) for k, s, e in _children(buf, start, end) if k == name), None)


def _full(buf: bytes, start: int) -> tuple[int, int]:
    word = struct.unpack_from(">I", buf, start)[0]
    return word >> 24, word & 0xFFFFFF


def parse_tracks(moov: bytes) -> dict[int, Track]:
    """Track ids, handler types and timescales from a moov, with the trex defaults."""
    defaults: dict[int, tuple[int, int]] = {}
    mvex = _child(moov, 0, len(moov), "mvex")
    if mvex:
        for kind, s, _ in _children(moov, *mvex):
            if kind == "trex":
                track_id, _, duration, _, flags = struct.unpack_from(">IIIII", moov, s + 4)
                defaults[track_id] = (duration, flags)
    tracks: dict[int, Track] = {}
    for kind, s, e in _children(moov, 0, len(moov)):
        if kind != "trak":
            continue
        tkhd, mdia = _child(moov, s, e, "tkhd"), _child(moov, s, e, "mdia")
        if not tkhd or not mdia:
            continue
        version, _ = _full(moov, tkhd[0])
        track_id = struct.unpack_from(">I", moov, tkhd[0] + (20 if version == 1 else 12))[0]
        mdhd, hdlr = _child(moov, *mdia, "mdhd"), _child(moov, *mdia, "hdlr")
        if not mdhd or not hdlr:
            continue
        version, _ = _full(moov, mdhd[0])
        timescale = struct.unpack_from(">I", moov, mdhd[0] + (20 if version == 1 else 12))[0]
        handler = moov[hdlr[0] + 8 : hdlr[0] + 12].decode("latin-1")
        duration, flags = defaults.get(track_id, (0, 0))
        tracks[track_id] = Track(track_id, handler, timescale, duration, flags)
    return tracks


@dataclass
class _Traf:
    track_id: int
    first_flags: int | None
    start: int | None
    duration: int


def _parse_traf(buf: bytes, start: int, end: int, tracks: dict[int, Track]) -> _Traf | None:
    tfhd = _child(buf, start, end, "tfhd")
    if not tfhd:
        return None
    _, flags = _full(buf, tfhd[0])
    track_id = struct.unpack_from(">I", buf, tfhd[0] + 4)[0]
    track = tracks.get(track_id, Track(track_id, "", 0))
    default_duration, default_flags = track.default_duration, track.default_flags
    at = tfhd[0] + 8
    for bit, width in ((0x01, 8), (0x02, 4)):
        if flags & bit:
            at += width
    if flags & 0x08:
        default_duration = struct.unpack_from(">I", buf, at)[0]
        at += 4
    if flags & 0x10:
        at += 4
    if flags & 0x20:
        default_flags = struct.unpack_from(">I", buf, at)[0]
    decode_start: int | None = None
    tfdt = _child(buf, start, end, "tfdt")
    if tfdt:
        version, _ = _full(buf, tfdt[0])
        decode_start = struct.unpack_from(">Q" if version == 1 else ">I", buf, tfdt[0] + 4)[0]
    first_flags: int | None = None
    duration = 0
    for kind, s, _ in _children(buf, start, end):
        if kind != "trun":
            continue
        _, tflags = _full(buf, s)
        count = struct.unpack_from(">I", buf, s + 4)[0]
        at = s + 8 + (4 if tflags & 0x01 else 0)
        run_first: int | None = None
        if tflags & 0x04:
            run_first = struct.unpack_from(">I", buf, at)[0]
            at += 4
        for n in range(count):
            sample_duration, sample_flags = default_duration, default_flags
            if tflags & 0x100:
                sample_duration = struct.unpack_from(">I", buf, at)[0]
                at += 4
            if tflags & 0x200:
                at += 4
            if tflags & 0x400:
                sample_flags = struct.unpack_from(">I", buf, at)[0]
                at += 4
            if tflags & 0x800:
                at += 4
            if n == 0 and run_first is not None:
                sample_flags = run_first
            if first_flags is None:
                first_flags = sample_flags
            duration += sample_duration
    return _Traf(track_id, first_flags, decode_start, duration)


@dataclass
class FragmentReader:
    """Fed the stream as it arrives, hands back the init once and then each whole fragment."""

    _buf: bytearray = field(default_factory=bytearray)
    _head: bytearray = field(default_factory=bytearray)
    _moof: bytes | None = None
    _clock: dict[int, int] = field(default_factory=dict)
    init: Init | None = None

    def feed(self, chunk: bytes) -> list[Init | Fragment]:
        self._buf += chunk
        out: list[Init | Fragment] = []
        while (box := self._next_box()) is not None:
            kind, data = box
            if self.init is None and kind != "moof":
                self._head += data
                continue
            if self.init is None:
                self.init = self._make_init()
                out.append(self.init)
            if kind == "moof":
                self._moof = data
            elif kind == "mdat" and self._moof is not None:
                out.append(self._fragment(self._moof, data))
                self._moof = None
        return out

    def _next_box(self) -> tuple[str, bytes] | None:
        if len(self._buf) < 8:
            return None
        size, kind = struct.unpack_from(">I4s", self._buf, 0)
        if size == 1:
            if len(self._buf) < 16:
                return None
            size = struct.unpack_from(">Q", self._buf, 8)[0]
        if size < 8:
            raise ValueError(f"box {kind!r} has an impossible size {size}")
        if len(self._buf) < size:
            return None
        data = bytes(self._buf[:size])
        del self._buf[:size]
        return kind.decode("latin-1"), data

    def _make_init(self) -> Init:
        head = bytes(self._head)
        moov = _child(head, 0, len(head), "moov")
        if moov is None:
            raise ValueError("the stream has no moov before its first fragment")
        return Init(head, parse_tracks(head[moov[0] : moov[1]]))

    def _fragment(self, moof: bytes, mdat: bytes) -> Fragment:
        assert self.init is not None
        trafs = [
            t
            for kind, s, e in _children(moof, 8, len(moof))
            if kind == "traf" and (t := _parse_traf(moof, s, e, self.init.tracks)) is not None
        ]
        if not trafs:
            raise ValueError("a moof without a track fragment")
        video = self.init.video
        traf = next((t for t in trafs if video and t.track_id == video.track_id), trafs[0])
        start = traf.start if traf.start is not None else self._clock.get(traf.track_id, 0)
        self._clock[traf.track_id] = start + traf.duration
        sync = traf.first_flags is not None and not traf.first_flags & SAMPLE_NON_SYNC
        return Fragment(moof + mdat, traf.track_id, sync, start, traf.duration)


@dataclass
class Segmenter:
    """Groups fragments into segments that each start on a video keyframe at least segment_s apart."""

    video: Track
    segment_s: float
    _parts: list[bytes] = field(default_factory=list)
    _start: int | None = None
    _end: int = 0
    _next: int = 0

    @property
    def next_index(self) -> int:
        return self._next

    def opens(self, frag: Fragment) -> int | None:
        """The index of the segment this fragment would start, or None when it continues one."""
        if not self.would_cut(frag):
            return None
        return self._next + (1 if self._start is not None and self._parts else 0)

    def would_cut(self, frag: Fragment) -> bool:
        """True when this fragment starts a new segment, so the one before it closes."""
        if frag.track_id != self.video.track_id or not frag.sync:
            return False
        return self._start is None or frag.start - self._start >= self.segment_s * self.video.timescale

    def push(self, frag: Fragment) -> Segment | None:
        closed: Segment | None = None
        if self.would_cut(frag):
            closed = self.close()
            self._start = frag.start
            self._end = frag.start
        if self._start is None:
            return None
        self._parts.append(frag.data)
        if frag.track_id == self.video.track_id:
            self._end = max(self._end, frag.start + frag.duration)
        return closed

    def close(self) -> Segment | None:
        if self._start is None or not self._parts:
            return None
        segment = Segment(self._next, b"".join(self._parts), (self._end - self._start) * 1000 // self.video.timescale)
        self._next += 1
        self._parts = []
        self._start = None
        return segment
