"""Delivery worker: assigns drivers to confirmed orders.

Consumes ``order.confirmed`` from ``delivery.queue`` and emits
``order.driver.assigned`` via the outbox table.

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

WORKER_TYPE = "delivery"


async def process_delivery_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a delivery event from the queue.

    This is called when an order.confirmed event arrives in delivery.queue.
    Assigns a driver and emits order.driver.assigned.

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
        raise NonRetryableError(f"unparseable delivery event body: {e}")

    order_id = data.get("order_id")
    event_id = data.get("event_id")
    if not order_id or not event_id:
        raise NonRetryableError("missing order_id or event_id in delivery event")

    own_session = session is None
    if own_session:
        session = SessionLocal()
    try:
        # Atomic idempotency claim.
        if not claim_event(session, event_id, WORKER_TYPE):
            logger.info(f"Event {event_id} already claimed by delivery worker (duplicate)")
            session.rollback()
            return True

        # Fetch order
        order = session.query(Order).filter(Order.id == order_id).first()
        if not order:
            logger.error(f"Order {order_id} not found; claiming without state change")
            session.commit()
            return True

        # State guard: only CONFIRMED orders can get a driver.
        if order.status != OrderStatus.CONFIRMED:
            logger.warning(
                f"Order {order_id} is not CONFIRMED (current: {order.status}). "
                "Cannot assign delivery."
            )
            session.commit()
            return True

        # Assign driver
        driver_id = await _assign_driver(order)

        # Update order to DRIVER_ASSIGNED
        order.status = OrderStatus.DRIVER_ASSIGNED
        session.add(order)

        # Create order.driver.assigned outbox event
        event_data = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order.driver.assigned",
            "order_id": order.id,
            "driver_id": driver_id,
            "driver_phone": "+1-555-0100",
            "vehicle": "Tesla Model 3",
        }
        session.add(
            OutboxEvent(
                id=event_data["event_id"],
                event_type="order.driver.assigned",
                order_id=order.id,
                payload=json.dumps(event_data),
            )
        )

        # Single commit: claim + transition + outbox row are atomic.
        session.commit()

        logger.info(f"Driver assigned to order {order_id}. Status: CONFIRMED -> DRIVER_ASSIGNED")
        return True

    except Exception:
        session.rollback()
        logger.exception("Error processing delivery event")
        return False  # Transient: caller applies bounded retry
    finally:
        if own_session:
            session.close()


async def _assign_driver(order: Order) -> str:
    """Mock driver assignment."""
    logger.info(f"Assigning driver for order {order.id}")
    await asyncio.sleep(0.1)  # Simulate API call
    driver_id = f"DRV-{order.id[:8].upper()}"
    logger.info(f"Driver {driver_id} assigned to order {order.id}")
    return driver_id


async def run_delivery_worker(channel: aio_pika.Channel):
    """
    Run the delivery worker consumer.

    Consumes from delivery.queue with explicit, safe acknowledgement.
    """
    await run_worker(channel, "delivery.queue", process_delivery_event, WORKER_TYPE)
