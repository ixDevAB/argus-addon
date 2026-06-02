from argus_addon.idempotency import Idempotency


def test_in_memory_record_and_seen():
    store = Idempotency(":memory:")
    assert store.seen("cmd-1") is False
    store.record("cmd-1")
    assert store.seen("cmd-1") is True
    store.record("cmd-1")
    assert store.seen("cmd-1") is True


def test_distinct_ids():
    store = Idempotency(":memory:")
    store.record("a")
    store.record("b")
    assert store.seen("a") is True
    assert store.seen("b") is True
    assert store.seen("c") is False


def test_survives_reopen(tmp_path):
    db_path = tmp_path / "idempotency.db"
    first = Idempotency(db_path)
    first.record("cmd-1")
    assert first.seen("cmd-1") is True
    first.close()

    second = Idempotency(db_path)
    assert second.seen("cmd-1") is True
    assert second.seen("cmd-2") is False
    second.close()
