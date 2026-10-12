# Changelog

## 2026-10-12 — Reliability fixes (review follow-up)

All changes are minimal, architecture-preserving fixes for the issues
found in the 2026-10-12 code review. No rebuild, no new features.

### Fixed (P0)

- `RabbitMQManager.connect()` now returns the manager instance
  (`app/rabbitmq.py`). `python -m app.worker_service` and
  `python -m app.outbox_publisher` previously crashed with
  `AttributeError` on startup.
- Bounded retries + working dead-letter routing (`app/retry.py`,
  `app/rabbitmq.py`, `app/worker_common.py`):
  new `quickbite.retry` fanout exchange + `orders.retry.queue`
  (per-message TTL, dead-letters back to `quickbite.orders`);
  new `dlx.quickbite.orders` topic exchange;
  `x-dead-letter-exchange` on every worker queue;
  `orders.dead-letter.queue` bound with `#`.
  Max 3 attempts, 5s → 10s → 20s backoff. Exhaustion marks the order
  `FAILED` (new `OrderStatus.FAILED`, never overwrites terminal states).
- Routing keys aligned with the assignment specification
  (`app/events.py`, all workers, docs):
  `order.created`, `order.paid`, `order.payment.failed`,
  `order.confirmed`, `order.driver.assigned`.
- Acknowledgement semantics corrected in all four workers
  (`app/worker_common.py::settle_message`): explicit manual
  settlement — ack only after the DB transaction commits (or after the
  broker confirms the retry copy); poison messages raise
  `NonRetryableError` and are rejected straight to the DLQ. The
  `message.process()` + manual ack/nack double-settlement pattern is
  gone.
- Tests for broker reliability behavior:
  `tests/test_reliability.py` (mocked: routing contract, retry policy,
  topology args, ack semantics, atomic idempotency, `connect()`
  contract) and `tests/test_integration_live.py` (marked `live`,
  requires real PostgreSQL + RabbitMQ).

### Fixed (P1)

- Explicit publisher confirms
  (`connect_robust(..., publisher_confirms=True)`); outbox rows are
  marked `published='Y'` only after broker confirmation, with a
  publish timeout (`app/rabbitmq.py`, `app/outbox_publisher.py`).
- Atomic idempotency: `claim_event()` does a single
  `INSERT ... ON CONFLICT DO NOTHING` on
  `processed_events(event_id, worker_type)` inside the worker's
  transaction — safe across redeliveries and concurrent instances
  (`app/idempotency.py`, all workers).
- `GET /health` now reports `database` and `rabbitmq` separately plus
  overall `ok`/`degraded`; scope documented
  (`app/routes.py`, `app/schemas.py`).
- Docs: routing keys corrected in `ARCHITECTURE.md`,
  `TECHNICAL_REPORT.md` (stale keys fixed + dated addendum),
  `README.md`; new `docs/REQUIREMENTS_CHECKLIST.md` and
  `docs/TESTING.md`; `demo.sh` extended with status polling, DLQ
  inspection, poison → DLQ, duplicate delivery, notification downtime,
  and `--scale worker=3` demos.
- `docker-compose.yml` now defines `api`, `publisher`, and `worker`
  services (enables `--scale worker=3`); `Dockerfile` CMD fixed.
- `python -m app.worker_service --workers payment,notification`
  supports running worker subsets (notification-downtime demo).
- Each worker runs on its own AMQP channel with `prefetch_count=1`
  (`app/worker_service.py`); deprecated `@app.on_event` replaced with
  lifespan (`app/main.py`).

### Remaining limitations

- Notification worker logs to console; notifications are not persisted
  to a `notifications` table.
- Duplicate `Idempotency-Key` on `POST /orders` returns 201 (should be 200).
- Workers use sync SQLAlchemy sessions inside async callbacks.
- `alembic` pinned but unused; schema via `create_all`. Existing
  databases created before 2026-10-12 can pick up `OrderStatus.FAILED`
  without data loss via
  `docs/migrations/001_add_failed_to_orderstatus.sql`
  (`psql "$DATABASE_URL" -f docs/migrations/001_add_failed_to_orderstatus.sql`).
- Live-broker tests were not executed in the fix environment (no
  RabbitMQ available); run them with
  `QUICKBITE_RUN_LIVE=1 pytest -m live` wherever Compose provides
  the broker.
