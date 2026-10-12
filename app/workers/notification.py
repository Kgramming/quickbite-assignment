"""Notification worker: sends notifications to customers."""

import asyncio
import json
import logging
from typing import Optional

import aio_pika

from app.database import SessionLocal
from app.idempotency import ProcessedEventStatus, has_processed_event, record_processed_event
from app.models import Order
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


async def process_notification_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a notification event from the queue.

    This worker listens to all order.* events and sends appropriate notifications.
    It does not update order state, only sends notifications.

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
        event_type = data.get("event_type")

        if not order_id or not event_id:
            logger.error("Missing order_id or event_id in notification event")
            return False

        if session is None:
            session = SessionLocal()
            close_session = True
        else:
            close_session = False

        try:
            # Idempotency check: has this specific event been processed?
            if has_processed_event(session, event_id, "notification"):
                logger.info(f"Notification already sent for event {event_id} (idempotent)")
                return True

            # Fetch order
            order = session.query(Order).filter(Order.id == order_id).first()
            if not order:
                logger.warning(f"Order {order_id} not found for notification")
                record_processed_event(
                    session,
                    event_id,
                    "notification",
                    ProcessedEventStatus.SUCCESS,
                    "Order not found",
                )
                return True

            # Send notification based on event type
            await _send_notification(order, event_type)

            # Record that notification was sent
            record_processed_event(
                session,
                event_id,
                "notification",
                ProcessedEventStatus.SUCCESS,
            )

            session.commit()

            logger.info(f"✓ Notification sent for order {order_id} (event: {event_type})")
            return True

        finally:
            if close_session:
                session.close()

    except Exception as e:
        logger.error(f"Error processing notification event: {e}", exc_info=True)
        return False


async def _send_notification(order: Order, event_type: str):
    """Mock notification sending."""
    logger.info(
        f"Sending notification to {order.customer_email}: "
        f"Order {order.id} - {event_type} (status: {order.status})"
    )
    await asyncio.sleep(0.05)  # Simulate sending delay
    logger.info(f"Notification sent to {order.customer_email}")


async def run_notification_worker(channel: aio_pika.Channel):
    """Run the notification worker consumer."""
    queue = await channel.get_queue("notification.queue")

    logger.info("Notification worker started, listening on notification.queue (all order events)")

    async def callback(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                order_id = message.headers.get("order_id", "unknown") if message.headers else "unknown"
                event_type = message.headers.get("event_type", "unknown") if message.headers else "unknown"
                logger.info(f"Notification worker received {event_type} event for order {order_id}")

                success = await process_notification_event(message.body)

                if success:
                    await message.ack()
                    logger.info(f"Notification worker acknowledged message for order {order_id}")
                else:
                    await message.nack(requeue=True)
                    logger.warning(f"Notification worker nacked message for order {order_id}")

            except Exception as e:
                logger.error(f"Error in notification worker callback: {e}", exc_info=True)
                await message.nack(requeue=True)

    await queue.consume(callback)
