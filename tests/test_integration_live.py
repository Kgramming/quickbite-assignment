"""Live integration tests against REAL PostgreSQL and RabbitMQ.

Marked ``live``: they are SKIPPED unless the environment provides both
services. Run them with::

    docker compose up -d postgres rabbitmq
    QUICKBITE_RUN_LIVE=1 python -m pytest -m live

Connection defaults match docker-compose.yml (host ports); override with
QUICKBITE_TEST_DATABASE_URL and QUICKBITE_TEST_RABBITMQ_URL.

All broker I/O shares a single module-scoped asyncio event loop
(``live_loop``): aio-pika connections are bound to the loop that
created them, so mixing ``asyncio.run()`` calls would hang.

These tests exercise the real broker: topology declaration, the outbox
publisher loop, real worker callbacks with manual acks, the retry loop,
and dead-letter routing. They are the honest counterpart to
tests/test_reliability.py (mocked) and tests/test_integration.py
(SQLite + direct function calls).
"""

import asyncio
import json
import os
import uuid

RUN_LIVE = os.environ.get("QUICKBITE_RUN_LIVE") == "1"
DATABASE_URL = os.environ.get(
    "QUICKBITE_TEST_DATABASE_URL",
    "postgresql+psycopg://quickbite_user:quickbite_password@127.0.0.1:5434/quickbite_db",
)
RABBITMQ_URL = os.environ.get(
    "QUICKBITE_TEST_RABBITMQ_URL", "amqp://quickbite:quickbite_password@127.0.0.1:5672/"
)

import aio_pika
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.events import QUEUE_CONFIG
from app.models import Order, OrderStatus, OutboxEvent
from app.rabbitmq import (
    DLQ_QUEUE,
    MAIN_EXCHANGE,
    RETRY_EXCHANGE,
    RabbitMQManager,
)
from app.retry import RETRY_COUNT_HEADER
from app.worker_common import settle_message
from app.workers.payment import process_payment_event

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not RUN_LIVE, reason="live services not requested (set QUICKBITE_RUN_LIVE=1)"
    ),
]


@pytest.fixture(scope="module")
def live_loop():
    """Single event loop shared by all broker I/O in this module."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def live_db_session_factory():
    """Session factory against the REAL PostgreSQL (isolated tables)."""
    engine = create_engine(DATABASE_URL, future=True)
    # Connectivity check with a clear error if Postgres is down.
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    # Drop pooled connections first: a failed test's traceback can keep a
    # session (and its open transaction) alive, which would block TRUNCATE.
    engine.dispose()
    # Clean up only rows created by these tests (by table truncation).
    with engine.begin() as conn:
        for table in ("processed_events", "outbox_events", "orders"):
            conn.execute(text(f"TRUNCATE {table} CASCADE"))
    engine.dispose()


@pytest.fixture(scope="module")
def live_rabbitmq(live_loop):
    """Connected RabbitMQManager against the REAL broker."""
    from app.rabbitmq import DLQ_QUEUE, RETRY_QUEUE

    async def setup():
        manager = await RabbitMQManager(RABBITMQ_URL).connect()
        await manager.declare_application_topology(QUEUE_CONFIG)
        # Start from a clean slate: purge leftovers from earlier runs.
        for queue_name in list(QUEUE_CONFIG) + [DLQ_QUEUE, RETRY_QUEUE]:
            queue = await manager.channel.get_queue(queue_name)
            await queue.purge()
        return manager

    manager = live_loop.run_until_complete(setup())
    yield manager
    live_loop.run_until_complete(manager.close())


@pytest.fixture(scope="module", autouse=True)
def _point_app_at_test_db(live_db_session_factory):
    """Point the app's SessionLocal users at the test database.

    Needed because ``app.database`` binds its engine at import time
    (long before this module runs), so the workers' and publisher's
    ``SessionLocal`` would otherwise target the default DATABASE_URL.
    """
    import app.outbox_publisher as outbox_publisher_module
    import app.worker_common as worker_common_module
    import app.workers.payment as payment_module

    outbox_publisher_module.SessionLocal = live_db_session_factory
    worker_common_module.SessionLocal = live_db_session_factory
    payment_module.SessionLocal = live_db_session_factory


def _make_order(session, total_amount="12.99", status=OrderStatus.PENDING):
    order_id = str(uuid.uuid4())
    order = Order(
        id=order_id,
        customer_name="Live Test",
        customer_email="live@example.com",
        customer_phone="5550000000",
        delivery_address="1 Live St",
        items=json.dumps([{"name": "Pizza", "quantity": 1, "price": total_amount}]),
        status=status,
        total_amount=total_amount,
        payment_status="pending",
    )
    session.add(order)
    session.commit()
    return order_id


class TestLiveTopology:
    def test_exchange_and_queues_exist(self, live_rabbitmq, live_loop):
        async def check():
            channel = live_rabbitmq.channel
            # Passive declares raise if the entity does not exist.
            await channel.declare_exchange(
                MAIN_EXCHANGE, aio_pika.ExchangeType.TOPIC,
                durable=True, passive=True,
            )
            await channel.declare_exchange(
                RETRY_EXCHANGE, aio_pika.ExchangeType.FANOUT,
                durable=True, passive=True,
            )
            for queue_name in list(QUEUE_CONFIG) + [DLQ_QUEUE]:
                await channel.declare_queue(queue_name, durable=True, passive=True)

        live_loop.run_until_complete(check())


class TestLiveOutboxAndPayment:
    def test_outbox_event_reaches_payment_queue(self, live_db_session_factory,
                                               live_rabbitmq, live_loop):
        """End-to-end slice: outbox row -> broker -> payment worker callback."""
        from app.outbox_publisher import OutboxPublisher

        # NOTE: _point_app_at_test_db (autouse) routes the app's
        # SessionLocal users at the test database.
        session = live_db_session_factory()
        order_id = _make_order(session)
        event_id = str(uuid.uuid4())
        session.add(
            OutboxEvent(
                id=event_id,
                event_type="order.created",
                order_id=order_id,
                payload=json.dumps({"event_id": event_id,
                                    "event_type": "order.created",
                                    "order_id": order_id}),
            )
        )
        session.commit()

        # Relay the outbox row to the real broker.
        live_loop.run_until_complete(
            OutboxPublisher(live_rabbitmq).publish_events())

        # The publisher used its own session: expire cached state first.
        session.expire_all()
        row = session.query(OutboxEvent).filter(
            OutboxEvent.id == event_id).first()
        assert row.published == "Y"  # only after broker confirmation

        # Pull the message from the real payment.queue and run the worker.
        async def consume_one():
            channel = await live_rabbitmq.new_channel()
            queue = await channel.get_queue("payment.queue")
            message = await queue.get(timeout=10)
            assert message is not None
            retry_exchange = await live_rabbitmq.get_retry_exchange(channel)
            result = await settle_message(
                message, retry_exchange, process_payment_event, "payment")
            await channel.close()
            return result

        assert live_loop.run_until_complete(consume_one()) == "acked"

        # The worker used its own session: expire cached state first.
        session.expire_all()
        order = session.query(Order).filter(Order.id == order_id).first()
        assert order.status == OrderStatus.PAID
        # Downstream event was written to the outbox for the next hop.
        downstream = session.query(OutboxEvent).filter(
            OutboxEvent.order_id == order_id,
            OutboxEvent.event_type == "order.paid",
        ).first()
        assert downstream is not None
        session.close()


class TestLiveDeadLetter:
    def test_poison_message_reaches_dlq(self, live_rabbitmq, live_loop):
        """An unparseable message is rejected (no requeue) and must appear
        in orders.dead-letter.queue, proving the DLX wiring."""

        async def scenario():
            channel = await live_rabbitmq.new_channel()
            retry_exchange = await live_rabbitmq.get_retry_exchange(channel)
            queue = await channel.get_queue("payment.queue")
            dlq = await channel.get_queue(DLQ_QUEUE)
            # Purge the DLQ first for a deterministic assertion.
            await dlq.purge()

            await live_rabbitmq.exchange.publish(
                aio_pika.Message(
                    body=b"this-is-not-json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"order_id": "poison-1"},
                ),
                routing_key="order.created",
            )

            message = await queue.get(timeout=10)
            assert message is not None
            result = await settle_message(
                message, retry_exchange, process_payment_event, "payment")
            assert result == "dlq"

            dead = await dlq.get(timeout=10)
            assert dead is not None
            assert dead.body == b"this-is-not-json"
            await dead.ack()
            await channel.close()

        live_loop.run_until_complete(scenario())

    def test_retry_then_dlq_marks_order_failed(self, live_db_session_factory,
                                              live_rabbitmq, live_loop):
        """A transient failure with x-retry-count exhausted is rejected to
        the DLQ and the order is marked FAILED."""
        # NOTE: DATABASE_URL is pointed at the test DB at module import,
        # so the app's SessionLocal (used by mark_order_failed) hits it.

        async def scenario():
            session = live_db_session_factory()
            order_id = _make_order(session)
            session.close()

            channel = await live_rabbitmq.new_channel()
            retry_exchange = await live_rabbitmq.get_retry_exchange(channel)
            queue = await channel.get_queue("payment.queue")

            body = json.dumps({"event_id": str(uuid.uuid4()),
                               "order_id": order_id}).encode()
            await live_rabbitmq.exchange.publish(
                aio_pika.Message(
                    body=body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"order_id": order_id,
                             RETRY_COUNT_HEADER: 99},  # already exhausted
                ),
                routing_key="order.created",
            )

            async def always_transient(_body: bytes) -> bool:
                return False

            message = await queue.get(timeout=10)
            assert message is not None
            result = await settle_message(
                message, retry_exchange, always_transient, "payment")
            assert result == "dlq"

            dlq = await channel.get_queue(DLQ_QUEUE)
            dead = await dlq.get(timeout=10)
            assert dead is not None
            await dead.ack()
            await channel.close()

            session = live_db_session_factory()
            order = session.query(Order).filter(Order.id == order_id).first()
            assert order.status == OrderStatus.FAILED
            session.close()

        live_loop.run_until_complete(scenario())
