import sqlite3

import pytest

from lab import Outbox


@pytest.fixture
def box(tmp_path):
    instance = Outbox(str(tmp_path / "demo.db"))
    yield instance
    instance.close()


def test_business_write_rolls_back_if_event_insert_fails(box):
    box.set_total("a", 5)
    box.db.execute("""CREATE TRIGGER disallow_events BEFORE INSERT ON event
                    BEGIN SELECT RAISE(ABORT, 'simulated failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        box.set_total("a", 10)
    assert box.db.execute("SELECT cents FROM order_total").fetchone() == (5,)
    assert box.advance("billing") == 1 and box.view("billing") == {"a": 5}


def test_failed_projection_rolls_back_and_replays(box):
    box.set_total("a", 10)

    def fail():
        raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="interruption"):
        box.advance("billing", before_checkpoint=fail)
    assert box.view("billing") == {}
    assert box.advance("billing") == 1 and box.view("billing") == {"a": 10}
    assert box.advance("billing") == 0


def test_consumers_checkpoint_independently_and_recover_after_reconnect(box):
    box.set_total("a", 10)
    box.set_total("a", 15)
    assert box.advance("billing", limit=1) == 1
    assert box.view("billing") == {"a": 10}
    path = box.db.execute("PRAGMA database_list").fetchone()[2]
    other = Outbox(path)
    try:
        assert other.advance("billing") == 1
        assert other.advance("audit") == 2
        assert other.view("billing") == other.view("audit") == {"a": 15}
    finally:
        other.close()


@pytest.mark.parametrize("order_id,cents", [("", 1), ("a", -1), ("a", True)])
def test_reject_invalid_amounts(box, order_id, cents):
    with pytest.raises(ValueError):
        box.set_total(order_id, cents)
    assert box.advance("billing") == 0
