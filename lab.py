"""Atomically persist business state and replay local projections from an ordered outbox."""

import sqlite3
from collections.abc import Callable
from contextlib import contextmanager


class Outbox:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS order_total (id TEXT PRIMARY KEY, cents INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS event (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL, cents INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoint (consumer TEXT PRIMARY KEY, seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS projection (
                consumer TEXT NOT NULL, order_id TEXT NOT NULL, cents INTEGER NOT NULL,
                PRIMARY KEY(consumer, order_id)
            );
        """)

    @contextmanager
    def _write(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def set_total(self, order_id: str, cents: int) -> None:
        if not order_id or type(cents) is not int or cents < 0:
            raise ValueError("expected order ID and nonnegative integer cents")
        with self._write():
            self.db.execute(
                """INSERT INTO order_total VALUES (?, ?)
                ON CONFLICT(id) DO UPDATE SET cents = excluded.cents""",
                (order_id, cents),
            )
            self.db.execute(
                "INSERT INTO event(order_id, cents) VALUES (?, ?)", (order_id, cents)
            )

    def advance(
        self,
        consumer: str,
        limit: int = 100,
        *,
        before_checkpoint: Callable[[], None] | None = None,
    ) -> int:
        if not consumer or not 1 <= limit <= 1000:
            raise ValueError("expected consumer ID and a limit from 1 to 1000")
        with self._write():
            row = self.db.execute(
                "SELECT seq FROM checkpoint WHERE consumer = ?", (consumer,)
            ).fetchone()
            events = self.db.execute(
                """SELECT seq, order_id, cents FROM event
                WHERE seq > ? ORDER BY seq LIMIT ?""",
                (row[0] if row else 0, limit),
            ).fetchall()
            if not events:
                return 0
            self.db.executemany(
                """INSERT INTO projection VALUES (?, ?, ?)
                ON CONFLICT(consumer, order_id) DO UPDATE SET cents = excluded.cents""",
                ((consumer, order_id, cents) for _, order_id, cents in events),
            )
            if before_checkpoint:
                before_checkpoint()
            self.db.execute(
                """INSERT INTO checkpoint VALUES (?, ?)
                ON CONFLICT(consumer) DO UPDATE SET seq = excluded.seq""",
                (consumer, events[-1][0]),
            )
            return len(events)

    def view(self, consumer: str) -> dict[str, int]:
        return dict(
            self.db.execute(
                "SELECT order_id, cents FROM projection WHERE consumer = ?", (consumer,)
            )
        )

    def close(self) -> None:
        self.db.close()


if __name__ == "__main__":
    box = Outbox(":memory:")
    try:
        box.set_total("order-1", 1200)
        box.set_total("order-1", 1700)
        for _ in range(2):
            print(box.advance("billing", limit=1), box.view("billing"))
    finally:
        box.close()
