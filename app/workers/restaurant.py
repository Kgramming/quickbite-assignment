"""Restaurant worker: confirms orders with restaurant system.

Consumes ``order.paid`` from ``restaurant.queue`` and emits
``order.confirmed`` via the outbox table.

Reliability: atomic ``claim_event`` idempotency, ack only after the
transaction commits, bounded retries on transient failure,
``NonRetryableError`` for poison messages.
"""

import asyncio
import json
import logging
import uuid
from typing import Optional

import aio_pika
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.idempotency import claim_event
from app.models import Order, OrderStatus, OutboxEvent
from app.retry import NonRetryableError
from app.worker_common import run_worker

logger = logging.getLogger(__name__)

WORKER_TYPE = "restaurant"


async def process_restaurant_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a restaurant event from the queue.

    This is called when an order.paid event arrives in restaurant.queue.
    Confirms the order with the restaurant and emits order.confirmed.

    Args:
        body: Message body (JSON)
        session: Optional existing DB session (used by tests)

    Returns:
        True if fully processed and committed (safe to ack),
        False on transient failure (caller should retry with bounds).

    Raises:
        NonRetryableError: message can never succeed (poison -> DLQ).
    """
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception as e:
        raise NonRetryableError(f"unparseable restaurant event body: {e}")

    order_id = data.get("order_id")
    event_id = data.get("event_id")
    if not order_id or not event_id:
        raise NonRetryableError("missing order_id or event_id in restaurant event")

    own_session = session is None
    if own_session:
        session = SessionLocal()
    try:
        # Atomic idempotency claim.
        if not claim_event(session, event_id, WORKER_TYPE):
            logger.info(f"Event {event_id} already claimed by restaurant worker (duplicate)")
            session.rollback()
            return True

        # Fetch order
        order = session.query(Order).filter(Order.id == order_id).first()
        if not order:
            logger.error(f"Order {order_id} not found; claiming without state change")
            session.commit()
            return True

        # State guard: only PAID orders can be confirmed.
        if order.status != OrderStatus.PAID:
            logger.warning(
                f"Order {order_id} is not PAID (current: {order.status}). "
                "Cannot confirm order."
            )
            session.commit()
            return True

        # Confirm with restaurant
        await _confirm_with_restaurant(order)

        # Update order to CONFIRMED
        order.status = OrderStatus.CONFIRMED
        session.add(order)

        # Create order.confirmed outbox event
        event_data = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order.confirmed",
            "order_id": order.id,
            "restaurant_id": "REST-001",
            "estimated_time": 30,  # minutes
        }
        session.add(
            OutboxEvent(
                id=event_data["event_id"],
                event_type="order.confirmed",
                order_id=order.id,
                payload=json.dumps(event_data),
            )
        )

        # Single commit: claim + transition + outbox row are atomic.
        session.commit()

        logger.info(f"Restaurant confirmed order {order_id}. Status: PAID -> CONFIRMED")
        return True

    except Exception:
        session.rollback()
        logger.exception("Error processing restaurant event")
        return False  # Transient: caller applies bounded retry
    finally:
        if own_session:
            session.close()


async def _confirm_with_restaurant(order: Order):
    """Mock restaurant confirmation."""
    logger.info(f"Confirming order {order.id} with restaurant")
    await asyncio.sleep(0.1)  # Simulate API call
    logger.info(f"Restaurant confirmed order {order.id}")


async def run_restaurant_worker(channel: aio_pika.Channel):
    """
    Run the restaurant worker consumer.

    Consumes from restaurant.queue with explicit, safe acknowledgement.
    """
    await run_worker(channel, "restaurant.queue", process_restaurant_event, WORKER_TYPE)
