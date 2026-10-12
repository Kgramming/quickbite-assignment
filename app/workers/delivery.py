"""Delivery worker: assigns drivers to confirmed orders."""

import asyncio
import json
import logging
import uuid
from typing import Optional

import aio_pika

from app.database import SessionLocal
from app.idempotency import ProcessedEventStatus, has_processed_event, record_processed_event
from app.models import Order, OrderStatus, OutboxEvent
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


async def process_delivery_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a delivery event from the queue.

    This is called when an order.restaurant_confirmed event arrives in delivery.queue.
    Assigns a driver and emits delivery.assigned.

    Args:
        body: Message body (JSON)

    Returns:
        True if processing succeeded, False otherwise
    """
    try:
        # Parse event
        data = json.loads(body.decode("utf-8"))
        order_id = data.get("order_id")
        event_id = data.get("event_id")

        if not order_id or not event_id:
            logger.error("Missing order_id or event_id in delivery event")
            return False

        if session is None:
            session = SessionLocal()
            close_session = True
        else:
            close_session = False

        try:
            # Idempotency check
            if has_processed_event(session, event_id, "delivery"):
                logger.info(f"Event {event_id} already processed by delivery worker (idempotent)")
                return True

            # Fetch order
            order = session.query(Order).filter(Order.id == order_id).first()
            if not order:
                logger.error(f"Order {order_id} not found")
                record_processed_event(
                    session,
                    event_id,
                    "delivery",
                    ProcessedEventStatus.FAILED,
                    "Order not found",
                )
                return True

            # Check order state (must be CONFIRMED)
            if order.status != OrderStatus.CONFIRMED:
                logger.warning(
                    f"Order {order_id} is not CONFIRMED (current: {order.status}). "
                    "Cannot assign delivery."
                )
                record_processed_event(
                    session,
                    event_id,
                    "delivery",
                    ProcessedEventStatus.SUCCESS,
                    f"Order not in CONFIRMED state: {order.status}",
                )
                return True

            # Assign driver
            driver_id = await _assign_driver(order)

            # Update order to DRIVER_ASSIGNED
            order.status = OrderStatus.DRIVER_ASSIGNED
            session.add(order)
            session.flush()

            # Create delivery.assigned outbox event
            event_data = {
                "order_id": order.id,
                "driver_id": driver_id,
                "driver_phone": "+1-555-0100",
                "vehicle": "Tesla Model 3",
            }

            outbox_event = OutboxEvent(
                id=str(uuid.uuid4()),
                event_type="order.delivery_assigned",
                order_id=order.id,
                payload=json.dumps(event_data),
            )
            session.add(outbox_event)

            # Record success
            record_processed_event(
                session,
                event_id,
                "delivery",
                ProcessedEventStatus.SUCCESS,
            )

            session.commit()

            logger.info(
                f"✓ Driver assigned to order {order_id}. "
                f"Status: CONFIRMED → DRIVER_ASSIGNED"
            )
            return True

        finally:
            if close_session:
                session.close()

    except Exception as e:
        logger.error(f"Error processing delivery event: {e}", exc_info=True)
        return False


async def _assign_driver(order: Order) -> str:
    """Mock driver assignment."""
    logger.info(f"Assigning driver for order {order.id}")
    await asyncio.sleep(0.1)  # Simulate API call
    driver_id = f"DRV-{order.id[:8].upper()}"
    logger.info(f"Driver {driver_id} assigned to order {order.id}")
    return driver_id


async def run_delivery_worker(channel: aio_pika.Channel):
    """Run the delivery worker consumer."""
    queue = await channel.get_queue("delivery.queue")

    logger.info("Delivery worker started, listening on delivery.queue")

    async def callback(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                order_id = message.headers.get("order_id", "unknown") if message.headers else "unknown"
                logger.info(f"Delivery worker received message for order {order_id}")

                success = await process_delivery_event(message.body)

                if success:
                    await message.ack()
                    logger.info(f"Delivery worker acknowledged message for order {order_id}")
                else:
                    await message.nack(requeue=True)
                    logger.warning(f"Delivery worker nacked message for order {order_id}")

            except Exception as e:
                logger.error(f"Error in delivery worker callback: {e}", exc_info=True)
                await message.nack(requeue=True)

    await queue.consume(callback)
