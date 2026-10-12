"""Reliability unit tests (mocked broker; no live RabbitMQ/Postgres needed).

These tests verify the reliability *mechanisms* — routing-key contract,
bounded-retry policy, topology declaration arguments, acknowledgement
semantics, and atomic idempotency — using fakes. They do NOT verify
against a live broker; see tests/test_integration_live.py for the
live-service tests (marked ``live``, skipped without a broker).
"""

import json
from datetime import timedelta

import aio_pika
import pytest

from app import rabbitmq as rabbitmq_module
from app.database import Base
from app.events import EVENT_ROUTING_KEYS, OUTBOX_ROUTING_KEYS, QUEUE_CONFIG, EventType
from app.idempotency import claim_event
from app.models import Order, OrderStatus
from app.rabbitmq import (
    DLQ_QUEUE,
    DLX_EXCHANGE,
    MAIN_EXCHANGE,
    RETRY_EXCHANGE,
    RETRY_QUEUE,
    RabbitMQManager,
)
from app.retry import (
    MAX_RETRIES,
    RETRY_COUNT_HEADER,
    NonRetryableError,
    get_retry_count,
    next_retry_delay_seconds,
    republish_for_retry,
)
from app.worker_common import mark_order_failed, settle_message


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeExchange:
    def __init__(self, name):
        self.name = name
        self.published = []  # list of (message, routing_key, kwargs)

    async def publish(self, message, routing_key="", **kwargs):
        self.published.append((message, routing_key, kwargs))


class FakeQueue:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs
        self.bindings = []  # list of (exchange_name, routing_key)

    async def bind(self, exchange, routing_key=""):
        name = exchange.name if hasattr(exchange, "name") else exchange
        self.bindings.append((name, routing_key))


class FakeChannel:
    def __init__(self):
        self.declared_exchanges = {}  # name -> (type, durable)
        self.declared_queues = {}  # name -> FakeQueue
        self.qos = None

    async def set_qos(self, prefetch_count=1):
        self.qos = prefetch_count

    async def declare_exchange(self, name, type, durable=True):
        self.declared_exchanges[name] = (type, durable)
        return FakeExchange(name)

    async def declare_queue(self, name, durable=True, arguments=None):
        queue = FakeQueue(name, durable=durable, arguments=arguments or {})
        self.declared_queues[name] = queue
        return queue


class FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.closed = False

    async def channel(self, publisher_confirms=False):
        self.publisher_confirms = publisher_confirms
        return self._channel

    async def close(self):
        self.closed = True


class FakeMessage:
    """Minimal stand-in for aio_pika.IncomingMessage."""

    def __init__(self, body: bytes, headers=None, routing_key="order.created"):
        self.body = body
        self.headers = headers or {}
        self.routing_key = routing_key
        self.message_id = "msg-1"
        self.acked = 0
        self.nacked = 0
        self.rejected = []  # list of requeue flags

    async def ack(self):
        self.acked += 1

    async def nack(self, requeue=True):
        self.nacked += 1

    async def reject(self, requeue=False):
        self.rejected.append(requeue)


# ---------------------------------------------------------------------------
# Routing-key contract (assignment specification)
# ---------------------------------------------------------------------------

class TestRoutingKeyContract:
    def test_event_routing_keys_match_spec(self):
        assert EVENT_ROUTING_KEYS[EventType.ORDER_CREATED] == "order.created"
        assert EVENT_ROUTING_KEYS[EventType.PAYMENT_SUCCESS] == "order.paid"
        assert EVENT_ROUTING_KEYS[EventType.PAYMENT_FAILED] == "order.payment.failed"
        assert EVENT_ROUTING_KEYS[EventType.RESTAURANT_CONFIRMED] == "order.confirmed"
        assert EVENT_ROUTING_KEYS[EventType.DELIVERY_ASSIGNED] == "order.driver.assigned"

    def test_queue_bindings_match_spec(self):
        assert QUEUE_CONFIG["payment.queue"] == ["order.created"]
        assert QUEUE_CONFIG["restaurant.queue"] == ["order.paid"]
        assert QUEUE_CONFIG["delivery.queue"] == ["order.confirmed"]
        assert QUEUE_CONFIG["notification.queue"] == ["order.#"]

    def test_outbox_routing_keys_are_allow_listed(self):
        for key in ("order.created", "order.paid", "order.payment.failed",
                    "order.confirmed", "order.driver.assigned"):
            assert OUTBOX_ROUTING_KEYS[key] == key
        assert OUTBOX_ROUTING_KEYS.get("order.bogus") is None


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

class TestRetryPolicy:
    def test_max_retries_bounded(self):
        assert MAX_RETRIES == 3

    def test_retry_count_defaults_to_zero(self):
        assert get_retry_count(FakeMessage(b"{}")) == 0

    def test_retry_count_reads_header(self):
        msg = FakeMessage(b"{}", headers={RETRY_COUNT_HEADER: 2})
        assert get_retry_count(msg) == 2

    def test_retry_count_tolerates_malformed_header(self):
        msg = FakeMessage(b"{}", headers={RETRY_COUNT_HEADER: "bogus"})
        assert get_retry_count(msg) == 0

    def test_backoff_increases(self):
        delays = [next_retry_delay_seconds(i) for i in range(3)]
        assert delays == sorted(delays)
        assert len(set(delays)) == 3  # strictly increasing

    async def test_republish_for_retry_sets_headers_and_ttl(self):
        exchange = FakeExchange(RETRY_EXCHANGE)
        msg = FakeMessage(b'{"a": 1}', headers={"order_id": "o1"},
                          routing_key="order.created")
        new_count = await republish_for_retry(exchange, msg, "order.created")

        assert new_count == 1
        assert len(exchange.published) == 1
        published, routing_key, _ = exchange.published[0]
        assert routing_key == "order.created"  # original key preserved
        assert published.headers[RETRY_COUNT_HEADER] == 1
        assert published.headers["order_id"] == "o1"
        assert published.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert published.expiration is not None
        assert published.body == b'{"a": 1}'

    async def test_republish_for_retry_increments_existing_count(self):
        exchange = FakeExchange(RETRY_EXCHANGE)
        msg = FakeMessage(b"{}", headers={RETRY_COUNT_HEADER: 1},
                          routing_key="order.paid")
        new_count = await republish_for_retry(exchange, msg, "order.paid")
        assert new_count == 2
        published, _, _ = exchange.published[0]
        assert published.headers[RETRY_COUNT_HEADER] == 2


# ---------------------------------------------------------------------------
# Topology declaration
# ---------------------------------------------------------------------------

class TestTopologyDeclaration:
    async def _manager_with_fake_channel(self):
        manager = RabbitMQManager("amqp://test")
        manager.channel = FakeChannel()
        manager.exchange = FakeExchange(MAIN_EXCHANGE)
        return manager

    async def test_worker_queues_declare_dead_letter_exchange(self):
        manager = await self._manager_with_fake_channel()
        await manager.declare_application_topology(QUEUE_CONFIG)

        for queue_name in ("payment.queue", "restaurant.queue",
                           "delivery.queue", "notification.queue"):
            queue = manager.channel.declared_queues[queue_name]
            assert queue.kwargs["durable"] is True
            assert queue.kwargs["arguments"] == {
                "x-dead-letter-exchange": DLX_EXCHANGE
            }, f"{queue_name} missing DLX argument"

    async def test_retry_queue_dead_letters_back_to_main_exchange(self):
        manager = await self._manager_with_fake_channel()
        await manager.declare_application_topology(QUEUE_CONFIG)

        queue = manager.channel.declared_queues[RETRY_QUEUE]
        assert queue.kwargs["arguments"] == {
            "x-dead-letter-exchange": MAIN_EXCHANGE
        }

    async def test_dead_letter_queue_bound_to_dlx(self):
        manager = await self._manager_with_fake_channel()
        await manager.declare_application_topology(QUEUE_CONFIG)

        assert DLQ_QUEUE in manager.channel.declared_queues
        dlq = manager.channel.declared_queues[DLQ_QUEUE]
        assert (DLX_EXCHANGE, "#") in dlq.bindings

    async def test_exchanges_declared_durable(self):
        manager = await self._manager_with_fake_channel()
        await manager.declare_application_topology(QUEUE_CONFIG)

        assert manager.channel.declared_exchanges[RETRY_EXCHANGE][1] is True
        assert manager.channel.declared_exchanges[DLX_EXCHANGE][1] is True


# ---------------------------------------------------------------------------
# connect() contract (P0-1 regression)
# ---------------------------------------------------------------------------

class TestConnectContract:
    async def test_connect_returns_manager_and_enables_confirms(self, monkeypatch):
        captured = {}

        async def fake_connect_robust(url, **kwargs):
            captured["url"] = url
            captured["kwargs"] = kwargs
            return FakeConnection(FakeChannel())

        monkeypatch.setattr(rabbitmq_module.aio_pika, "connect_robust",
                            fake_connect_robust)

        manager = await RabbitMQManager("amqp://u:p@h:5672/").connect()

        assert manager is not None
        assert isinstance(manager, RabbitMQManager)
        assert manager.channel is not None
        assert manager.exchange is not None
        # P1-6: publisher confirms must be explicitly enabled
        assert captured["kwargs"].get("publisher_confirms") is True


# ---------------------------------------------------------------------------
# Acknowledgement semantics (P0-4)
# ---------------------------------------------------------------------------

def _sqlite_session():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


class TestSettleMessage:
    async def test_success_acks_exactly_once(self):
        message = FakeMessage(b'{"order_id": "o1"}')
        exchange = FakeExchange(RETRY_EXCHANGE)

        async def process_ok(body: bytes) -> bool:
            return True

        result = await settle_message(message, exchange, process_ok, "payment")

        assert result == "acked"
        assert message.acked == 1
        assert message.nacked == 0
        assert message.rejected == []
        assert exchange.published == []

    async def test_transient_failure_schedules_retry_then_acks_original(self):
        message = FakeMessage(b'{"order_id": "o1"}', routing_key="order.created")
        exchange = FakeExchange(RETRY_EXCHANGE)

        async def process_fail(body: bytes) -> bool:
            return False

        result = await settle_message(message, exchange, process_fail, "payment")

        assert result == "retried"
        assert len(exchange.published) == 1  # retry copy confirmed first
        assert message.acked == 1  # original acked only afterwards
        assert message.rejected == []

    async def test_exception_is_treated_as_transient(self):
        message = FakeMessage(b'{"order_id": "o1"}', routing_key="order.created")
        exchange = FakeExchange(RETRY_EXCHANGE)

        async def process_boom(body: bytes) -> bool:
            raise RuntimeError("db exploded")

        result = await settle_message(message, exchange, process_boom, "payment")

        assert result == "retried"
        assert len(exchange.published) == 1
        assert message.acked == 1

    async def test_non_retryable_goes_straight_to_dlq(self):
        message = FakeMessage(b"not-json", routing_key="order.created")
        exchange = FakeExchange(RETRY_EXCHANGE)

        async def process_poison(body: bytes) -> bool:
            raise NonRetryableError("unparseable")

        result = await settle_message(message, exchange, process_poison, "payment")

        assert result == "dlq"
        assert message.rejected == [False]  # reject(requeue=False) -> DLX -> DLQ
        assert message.acked == 0
        assert exchange.published == []  # no retry burned

    async def test_exhausted_retries_reject_to_dlq_and_mark_failed(self, monkeypatch):
        import app.worker_common as worker_common_module

        session = _sqlite_session()
        order = Order(
            id="order-dlq-1",
            customer_name="N",
            customer_email="n@n.com",
            customer_phone="5550000000",
            delivery_address="addr",
            items="[]",
            status=OrderStatus.PENDING,
            total_amount="10.00",
        )
        session.add(order)
        session.commit()

        # Route mark_order_failed at the test session.
        monkeypatch.setattr(
            worker_common_module, "SessionLocal", lambda: session
        )

        body = json.dumps({"order_id": "order-dlq-1",
                           "event_id": "evt-1"}).encode()
        message = FakeMessage(body, headers={RETRY_COUNT_HEADER: MAX_RETRIES},
                              routing_key="order.created")
        exchange = FakeExchange(RETRY_EXCHANGE)

        async def process_fail(body: bytes) -> bool:
            return False

        result = await settle_message(message, exchange, process_fail, "payment")

        assert result == "dlq"
        assert message.rejected == [False]
        assert exchange.published == []  # no further retry
        # mark_order_failed closed the session; re-query for a fresh state.
        order = session.query(Order).filter(Order.id == "order-dlq-1").first()
        assert order.status == OrderStatus.FAILED

    def test_mark_order_failed_never_overwrites_terminal(self, monkeypatch):
        import app.worker_common as worker_common_module

        session = _sqlite_session()
        order = Order(
            id="order-term-1",
            customer_name="N",
            customer_email="n@n.com",
            customer_phone="5550000000",
            delivery_address="addr",
            items="[]",
            status=OrderStatus.PAYMENT_FAILED,
            total_amount="10.00",
        )
        session.add(order)
        session.commit()
        monkeypatch.setattr(worker_common_module, "SessionLocal", lambda: session)

        mark_order_failed("order-term-1")
        # mark_order_failed closed the session; re-query for a fresh state.
        order = session.query(Order).filter(Order.id == "order-term-1").first()
        assert order.status == OrderStatus.PAYMENT_FAILED


# ---------------------------------------------------------------------------
# Atomic idempotency (P1-7)
# ---------------------------------------------------------------------------

class TestAtomicIdempotency:
    def test_first_claim_wins_second_loses(self):
        session = _sqlite_session()
        assert claim_event(session, "evt-a", "payment") is True
        assert claim_event(session, "evt-a", "payment") is False
        # Different worker type is an independent claim.
        assert claim_event(session, "evt-a", "restaurant") is True

    def test_claim_persists_after_commit(self):
        session = _sqlite_session()
        assert claim_event(session, "evt-b", "payment") is True
        session.commit()
        # Second claim in a new transaction still loses.
        assert claim_event(session, "evt-b", "payment") is False


# ---------------------------------------------------------------------------
# Consumer lifetime: run_worker must not return after registering (regression)
# ---------------------------------------------------------------------------

class TestConsumerLifetime:
    @pytest.mark.asyncio
    async def test_run_worker_stays_alive(self):
        """run_worker must keep consuming forever.

        Regression: an earlier version returned right after
        queue.consume(), so asyncio.gather() completed and the whole
        worker process exited seconds after startup.
        """
        import asyncio

        from app.worker_common import run_worker

        consumed = []

        class FakeQueue:
            async def consume(self, callback):
                consumed.append(callback)
                return "ctag"

        class FakeExchange:
            pass

        class FakeChannel:
            async def declare_exchange(self, name, type, durable=True):
                return FakeExchange()

            async def get_queue(self, name):
                return FakeQueue()

        async def process_fn(body: bytes) -> bool:
            return True

        task = asyncio.create_task(
            run_worker(FakeChannel(), "payment.queue", process_fn, "payment"))
        await asyncio.sleep(0.2)
        # The consumer callback was registered...
        assert len(consumed) == 1
        # ...and the worker task is still alive (not returned).
        assert not task.done(), "run_worker returned; workers would exit!"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
