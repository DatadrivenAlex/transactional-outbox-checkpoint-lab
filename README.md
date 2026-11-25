# Transactional Outbox Checkpoint Lab

A SQLite reference implementation for keeping an order update and its event in the same transaction, then replaying events into a local per-consumer projection. It demonstrates two distinct atomic boundaries: order plus event, and projection plus checkpoint.

```sh
python lab.py
```

The demo applies two versions of one order in sequence. `BEGIN IMMEDIATE` serializes writers and overlapping consumers while each bounded batch updates its projection and checkpoint together. The tests force rollback on both paths and resume against the persisted database from a different connection. This is not an external message broker: there is no event retention, external delivery, distributed ordering, or exactly-once guarantee across services. The failure callback is only for injecting a local test fault; do not use it to perform external effects in the transaction.

## Checks

Use Python 3.11 or newer. To reproduce the CI checks in a virtual environment:

```sh
python -m pip install pytest==8.4.2 ruff==0.14.14
ruff check .
ruff format --check .
python -m pytest -q
```
