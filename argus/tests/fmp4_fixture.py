"""A stream shaped like go2rtc 1.9.14's stream.mp4: ftyp and moov, then one moof+mdat per sample,
H.264 at 90 kHz on track 1 with a keyframe every 24 frames at 12.5 fps, AAC at 16 kHz on track 2."""

import struct

VIDEO_TICKS = 7200
AUDIO_TICKS = 1280
GOP = 24
NON_SYNC = 0x01010000
SYNC = 0x02000000


def box(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


def full(kind: bytes, version: int, flags: int, payload: bytes) -> bytes:
    return box(kind, struct.pack(">I", version << 24 | flags) + payload)


def trak(track_id: int, handler: bytes, timescale: int, entry: bytes) -> bytes:
    tkhd = full(b"tkhd", 0, 3, struct.pack(">III", 0, 0, track_id) + b"\x00" * 68)
    mdhd = full(b"mdhd", 0, 0, struct.pack(">IIII", 0, 0, timescale, 0) + b"\x00" * 4)
    hdlr = full(b"hdlr", 0, 0, b"\x00" * 4 + handler + b"\x00" * 12 + b"x\x00")
    stsd = full(b"stsd", 0, 0, struct.pack(">I", 1) + entry)
    return box(b"trak", tkhd + box(b"mdia", mdhd + hdlr + box(b"minf", box(b"stbl", stsd))))


def init_segment(codec: bytes = b"avc1", width: int = 2560, height: int = 1440) -> bytes:
    video = box(codec, b"\x00" * 6 + b"\x00\x01" + b"\x00" * 16 + struct.pack(">HH", width, height) + b"\x00" * 50)
    audio = box(b"mp4a", b"\x00" * 28)
    trex = b"".join(full(b"trex", 0, 0, struct.pack(">IIIII", t, 1, 0, 0, 0)) for t in (1, 2))
    moov = box(
        b"moov",
        full(b"mvhd", 0, 0, b"\x00" * 96)
        + trak(1, b"vide", 90000, video)
        + trak(2, b"soun", 16000, audio)
        + box(b"mvex", trex),
    )
    return box(b"ftyp", b"iso5\x00\x00\x02\x00iso5iso6mp41") + moov


def fragment(seq: int, track_id: int, decode_time: int, duration: int, flags: int, size: int) -> bytes:
    tfhd = full(b"tfhd", 0, 0x020000, struct.pack(">I", track_id))
    tfdt = full(b"tfdt", 1, 0, struct.pack(">Q", decode_time))
    trun = full(b"trun", 0, 0x000305 | 0x04, struct.pack(">IiIII", 1, 0, flags, duration, size))
    moof = box(b"moof", full(b"mfhd", 0, 0, struct.pack(">I", seq)) + box(b"traf", tfhd + tfdt + trun))
    return moof + box(b"mdat", bytes([seq % 256]) * size)


def video_frame(n: int, seq: int, size: int = 2048) -> bytes:
    return fragment(seq, 1, n * VIDEO_TICKS, VIDEO_TICKS, SYNC if n % GOP == 0 else NON_SYNC, size)


def audio_frame(n: int, seq: int) -> bytes:
    return fragment(seq, 2, n * AUDIO_TICKS, AUDIO_TICKS, SYNC, 64)


def frames(count: int, first: int = 0) -> list[bytes]:
    """Video frames first..first+count, each followed by its audio frame, as go2rtc interleaves them."""
    out = []
    for n in range(first, first + count):
        out.append(video_frame(n, 2 * n))
        out.append(audio_frame(n, 2 * n + 1))
    return out
