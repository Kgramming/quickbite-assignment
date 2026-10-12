"""Shared reliable-consumer helpers for all workers.

Settlement contract (explicit manual acks, no ``message.process()``):

  - ``process_fn`` returns True   -> all DB work committed -> ``ack()``
  - ``process_fn`` returns False  -> transient failure -> bounded retry,
    or reject-to-DLQ when retries are exhausted
  - ``process_fn`` raises NonRetryableError -> poison -> reject-to-DLQ
  - any other exception           -> treated as transient failure

The ack is always the last step and is only issued after the worker's
database transaction has committed (and, on the retry path, after the
broker confirms the retry copy). A crash before ack therefore causes a
redelivery that the idempotent ``process_fn`` handles safely.
"""

import asyncio
import json
import logging
from typing import Awaitable, Callable

import aio_pika

from app.database import SessionLocal
from app.models import Order, OrderStatus
from app.rabbitmq import RETRY_EXCHANGE
from app.retry import (
    MAX_RETRIES,
    NonRetryableError,
    get_retry_count,
    republish_for_retry,
)

logger = logging.getLogger(__name__)

# Order statuses after which no further automatic transition is attempted.
TERMINAL_STATUSES = {
    OrderStatus.PAYMENT_FAILED,
    OrderStatus.DRIVER_ASSIGNED,
    OrderStatus.CANCELLED,
    OrderStatus.FAILED,
}


def mark_order_failed(order_id: str) -> None:
    """Mark an order FAILED after its message exhausted retries.

    Only transitions non-terminal orders; never overwrites a terminal
    state that a later event may have set concurrently.
    """
    session = SessionLocal()
    try:
        order = session.query(Order).filter(Order.id == order_id).first()
        if order is None:
            logger.warning(f"Cannot mark FAILED: order {order_id} not found")
            return
        if order.status in TERMINAL_STATUSES:
            logger.info(
                f"Order {order_id} already terminal ({order.status}); not overwriting"
            )
            return
        order.status = OrderStatus.FAILED
        session.commit()
        logger.warning(f"Order {order_id} marked FAILED after retry exhaustion")
    except Exception:
        session.rollback()
        logger.exception(f"Failed to mark order {order_id} as FAILED")
    finally:
        session.close()


def _extract_order_id(body: bytes) -> str | None:
    """Best-effort order_id extraction for failure bookkeeping."""
    try:
        data = json.loads(body.decode("utf-8"))
        return data.get("order_id")
    except Exception:
        return None


async def settle_message(
    message: aio_pika.IncomingMessage,
    retry_exchange: aio_pika.Exchange,
    process_fn: Callable[[bytes], Awaitable[bool]],
    worker_name: str,
) -> str:
    """Process one message and settle it according to the contract above.

    Returns one of: "acked", "retried", "dlq".
    """
    headers = message.headers or {}
    order_id = headers.get("order_id", "unknown")
    logger.info(f"{worker_name} worker received message for order {order_id}")

    try:
        outcome = await process_fn(message.body)
    except NonRetryableError as e:
        logger.warning(f"{worker_name}: non-retryable message: {e} -> DLQ")
        await message.reject(requeue=False)
        return "dlq"
    except Exception:
        logger.exception(f"{worker_name}: transient error processing message")
        outcome = False

    if outcome is True:
        # DB transaction committed inside process_fn; safe to ack.
        await message.ack()
        logger.info(f"{worker_name} worker acknowledged message for order {order_id}")
        return "acked"

    # Transient failure -> bounded retry, else DLQ.
    count = get_retry_count(message)
    if count < MAX_RETRIES:
        # Publish retry copy with confirms; ack original only afterwards.
        await republish_for_retry(retry_exchange, message, message.routing_key)
        await message.ack()
        logger.warning(
            f"{worker_name} worker scheduled retry for order {order_id} "
            f"(attempt {count + 1}/{MAX_RETRIES})"
        )
        return "retried"

    real_order_id = _extract_order_id(message.body)
    if real_order_id:
        mark_order_failed(real_order_id)
    await message.reject(requeue=False)  # DLX -> orders.dead-letter.queue
    logger.warning(
        f"{worker_name} worker exhausted retries for order {order_id} -> DLQ"
    )
    return "dlq"


async def run_worker(
    channel: aio_pika.Channel,
    queue_name: str,
    process_fn: Callable[[bytes], Awaitable[bool]],
    worker_name: str,
) -> None:
    """Start a consumer on ``queue_name`` with reliable settlement.

    Declaring the retry exchange here is idempotent; the retry queue and
    its dead-letter routing are declared once at startup by
    ``declare_application_topology``.
    """
    retry_exchange = await channel.declare_exchange(
        name=RETRY_EXCHANGE,
        type=aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    queue = await channel.get_queue(queue_name)
    logger.info(f"{worker_name} worker started, listening on {queue_name}")

    async def callback(message: aio_pika.IncomingMessage):
        await settle_message(message, retry_exchange, process_fn, worker_name)

    await queue.consume(callback)
    # Keep the consumer task alive for the lifetime of the service.
    # Without this the coroutine returns after registering the consumer,
    # asyncio.gather() completes, and the whole worker process exits.
    await asyncio.Future()
