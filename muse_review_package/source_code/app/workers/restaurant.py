"""Restaurant worker: confirms orders with restaurant system."""

import asyncio
import json
import logging
import uuid

import aio_pika

from app.database import SessionLocal
from app.idempotency import ProcessedEventStatus, has_processed_event, record_processed_event
from app.models import Order, OrderStatus, OutboxEvent
from sqlalchemy.orm import Session
from typing import Optional

logger = logging.getLogger(__name__)


async def process_restaurant_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a restaurant event from the queue.

    This is called when an order.payment.success event arrives in restaurant.queue.
    Confirms the order with the restaurant and emits restaurant.confirmed.

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
            logger.error("Missing order_id or event_id in restaurant event")
            return False

        if session is None:
            session = SessionLocal()
            close_session = True
        else:
            close_session = False

        try:
            # Idempotency check
            if has_processed_event(session, event_id, "restaurant"):
                logger.info(f"Event {event_id} already processed by restaurant worker (idempotent)")
                return True

            # Fetch order
            order = session.query(Order).filter(Order.id == order_id).first()
            if not order:
                logger.error(f"Order {order_id} not found")
                record_processed_event(
                    session,
                    event_id,
                    "restaurant",
                    ProcessedEventStatus.FAILED,
                    "Order not found",
                )
                return True

            # Check order state (must be PAID)
            if order.status != OrderStatus.PAID:
                logger.warning(
                    f"Order {order_id} is not PAID (current: {order.status}). "
                    "Cannot confirm order."
                )
                record_processed_event(
                    session,
                    event_id,
                    "restaurant",
                    ProcessedEventStatus.SUCCESS,
                    f"Order not in PAID state: {order.status}",
                )
                return True  # Idempotent: already processed or out of sequence

            # Confirm with restaurant
            await _confirm_with_restaurant(order)

            # Update order to CONFIRMED
            order.status = OrderStatus.CONFIRMED
            session.add(order)
            session.flush()

            # Create restaurant.confirmed outbox event
            event_data = {
                "order_id": order.id,
                "restaurant_id": "REST-001",
                "estimated_time": 30,  # minutes
            }

            outbox_event = OutboxEvent(
                id=str(uuid.uuid4()),
                event_type="order.restaurant_confirmed",
                order_id=order.id,
                payload=json.dumps(event_data),
            )
            session.add(outbox_event)

            # Record success
            record_processed_event(
                session,
                event_id,
                "restaurant",
                ProcessedEventStatus.SUCCESS,
            )

            session.commit()

            logger.info(
                f"✓ Restaurant confirmed order {order_id}. "
                f"Status: PAID → CONFIRMED"
            )
            return True

        finally:
            if close_session:
                session.close()

    except Exception as e:
        logger.error(f"Error processing restaurant event: {e}", exc_info=True)
        return False


async def _confirm_with_restaurant(order: Order):
    """Mock restaurant confirmation."""
    logger.info(f"Confirming order {order.id} with restaurant")
    await asyncio.sleep(0.1)  # Simulate API call
    logger.info(f"Restaurant confirmed order {order.id}")


async def run_restaurant_worker(channel: aio_pika.Channel):
    """Run the restaurant worker consumer."""
    queue = await channel.get_queue("restaurant.queue")

    logger.info("Restaurant worker started, listening on restaurant.queue")

    async def callback(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                order_id = message.headers.get("order_id", "unknown") if message.headers else "unknown"
                logger.info(f"Restaurant worker received message for order {order_id}")

                success = await process_restaurant_event(message.body)

                if success:
                    await message.ack()
                    logger.info(f"Restaurant worker acknowledged message for order {order_id}")
                else:
                    await message.nack(requeue=True)
                    logger.warning(f"Restaurant worker nacked message for order {order_id}")

            except Exception as e:
                logger.error(f"Error in restaurant worker callback: {e}", exc_info=True)
                await message.nack(requeue=True)

    await queue.consume(callback)
