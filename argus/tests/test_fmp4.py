from argus_addon.fmp4 import Fragment, FragmentReader, Init, Segmenter, parse_tracks
from tests.fmp4_fixture import frames, init_segment, video_frame


def read(stream: bytes, chunk: int) -> list:
    reader = FragmentReader()
    out = []
    for i in range(0, len(stream), chunk):
        out += reader.feed(stream[i : i + chunk])
    return out


def test_the_reader_hands_back_the_init_and_whole_fragments_however_the_bytes_arrive():
    stream = init_segment() + b"".join(frames(30))
    for chunk in (7, 1000, 65536):
        items = read(stream, chunk)
        assert isinstance(items[0], Init) and items[0].data == init_segment()
        frags = items[1:]
        assert len(frags) == 60 and all(isinstance(f, Fragment) for f in frags)
        assert b"".join(f.data for f in frags) == b"".join(frames(30))
    video = [f for f in read(stream, 4096)[1:] if f.track_id == 1]
    assert [f.start for f in video[:3]] == [0, 7200, 14400]
    assert [i for i, f in enumerate(video) if f.sync] == [0, 24]


def test_tracks_come_from_the_moov():
    moov = init_segment()[init_segment().index(b"moov") - 4 :]
    tracks = parse_tracks(moov[8:])
    assert {t.track_id: (t.handler, t.timescale) for t in tracks.values()} == {1: ("vide", 90000), 2: ("soun", 16000)}


def segment(count: int, segment_s: float = 4, first: int = 0):
    items = read(init_segment() + b"".join(frames(count, first)), 4096)
    seg = Segmenter(items[0].video, segment_s)
    out = [s for f in items[1:] if (s := seg.push(f))]
    if last := seg.close():
        out.append(last)
    return out


def test_segments_start_on_the_first_keyframe_past_segment_s_and_join_back_to_the_stream():
    # Keyframes every 1.92 s: the first cut at or after 4 s is the keyframe at 5.76 s.
    segments = segment(100)
    assert [(s.index, s.duration_ms) for s in segments] == [(0, 5760), (1, 2240)]
    assert b"".join(s.data for s in segments) == b"".join(frames(100))
    assert segments[1].data.startswith(video_frame(72, 144))


def test_fragments_before_the_first_keyframe_are_dropped():
    segments = segment(40, first=10)
    assert segments[0].data.startswith(video_frame(24, 48))
    assert sum(s.duration_ms for s in segments) == (50 - 24) * 80


def test_opens_names_the_index_a_cut_would_start():
    items = read(init_segment() + b"".join(frames(80)), 4096)
    seg = Segmenter(items[0].video, 4)
    opened = []
    for f in items[1:]:
        if (i := seg.opens(f)) is not None:
            opened.append(i)
        seg.push(f)
    assert opened == [0, 1]
