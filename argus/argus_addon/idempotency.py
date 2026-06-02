import sqlite3
from pathlib import Path


class Idempotency:
    def __init__(self, path: Path | str = ":memory:"):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute(
            "create table if not exists processed (id text primary key, seen_at integer not null default (strftime('%s','now')))"
        )
        self.conn.commit()

    def seen(self, cmd_id: str) -> bool:
        row = self.conn.execute("select 1 from processed where id = ?", (cmd_id,)).fetchone()
        return row is not None

    def record(self, cmd_id: str) -> None:
        self.conn.execute("insert or ignore into processed (id) values (?)", (cmd_id,))
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
