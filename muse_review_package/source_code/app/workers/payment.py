"""Payment worker: processes payments and transitions orders."""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Optional

import aio_pika

from app.config import settings
from app.database import SessionLocal
from app.events import (
    EVENT_ROUTING_KEYS,
    EventType,
    OrderCreatedEvent,
    PaymentFailedEvent,
    PaymentSuccessEvent,
)
from app.idempotency import ProcessedEventStatus, has_processed_event, record_processed_event
from app.models import Order, OrderStatus, OutboxEvent
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


async def process_payment_event(body: bytes, session: Optional[Session] = None) -> bool:
    """
    Process a payment event from the queue.

    This is called when an order.created event arrives in payment.queue.
    Charges the payment and emits either payment.success or payment.failed.

    Args:
        body: Message body (JSON)

    Returns:
        True if processing succeeded (should acknowledge), False otherwise
    """
    try:
        # Parse event
        data = json.loads(body.decode("utf-8"))
        order_id = data.get("order_id")
        event_id = data.get("event_id")

        if not order_id or not event_id:
            logger.error("Missing order_id or event_id in payment event")
            return False

        if session is None:
            session = SessionLocal()
            close_session = True
        else:
            close_session = False

        try:
            # Idempotency check: has this event been processed by payment worker?
            if has_processed_event(session, event_id, "payment"):
                logger.info(f"Event {event_id} already processed by payment worker (idempotent)")
                return True  # Acknowledge duplicate

            # Fetch order
            order = session.query(Order).filter(Order.id == order_id).first()
            if not order:
                logger.error(f"Order {order_id} not found")
                record_processed_event(
                    session,
                    event_id,
                    "payment",
                    ProcessedEventStatus.FAILED,
                    "Order not found",
                )
                return True  # Acknowledge, but don't retry

            # Check order state (idempotency: ensure still PENDING)
            if order.status != OrderStatus.PENDING:
                logger.warning(
                    f"Order {order_id} is not in PENDING state (current: {order.status}). "
                    "Payment already processed or order in unexpected state."
                )
                record_processed_event(
                    session,
                    event_id,
                    "payment",
                    ProcessedEventStatus.SUCCESS,
                    f"Order already in state {order.status}",
                )
                return True  # Acknowledge (idempotent behavior)

            # Simulate payment processing
            payment_success = await _charge_payment(order)

            if payment_success:
                # Update order to PAID
                order.status = OrderStatus.PAID
                order.payment_status = "success"
                session.add(order)
                session.flush()

                # Create payment.success outbox event
                event_data = {
                    "order_id": order.id,
                    "amount": order.total_amount,
                    "transaction_id": str(uuid.uuid4()),
                }

                outbox_event = OutboxEvent(
                    id=str(uuid.uuid4()),
                    event_type="order.payment_success",
                    order_id=order.id,
                    payload=json.dumps(event_data),
                )
                session.add(outbox_event)

                # Record success
                record_processed_event(
                    session,
                    event_id,
                    "payment",
                    ProcessedEventStatus.SUCCESS,
                )

                session.commit()

                logger.info(
                    f"✓ Payment succeeded for order {order_id}. "
                    f"Status: PENDING → PAID"
                )
                return True

            else:
                # Update order to PAYMENT_FAILED
                order.status = OrderStatus.PAYMENT_FAILED
                order.payment_status = "failed"
                session.add(order)
                session.flush()

                # Create payment.failed outbox event
                event_data = {
                    "order_id": order.id,
                    "reason": "Insufficient funds",
                }

                outbox_event = OutboxEvent(
                    id=str(uuid.uuid4()),
                    event_type="order.payment_failed",
                    order_id=order.id,
                    payload=json.dumps(event_data),
                )
                session.add(outbox_event)

                # Record as success (payment failure is a valid outcome)
                record_processed_event(
                    session,
                    event_id,
                    "payment",
                    ProcessedEventStatus.SUCCESS,
                )

                session.commit()

                logger.info(
                    f"✗ Payment failed for order {order_id}. "
                    f"Status: PENDING → PAYMENT_FAILED"
                )
                return True

        finally:
            if close_session:
                session.close()

    except Exception as e:
        logger.error(f"Error processing payment event: {e}", exc_info=True)
        return False  # Do not acknowledge, let it retry


async def _charge_payment(order: Order) -> bool:
    """
    Mock payment processing.

    Returns True if payment succeeds, False if it fails.
    """
    # Simple mock: fail if total is exactly 999.99 (for testing)
    total = float(order.total_amount)

    if total == 999.99:
        logger.info(f"Mock payment processor: REJECTED (amount: {order.total_amount})")
        return False

    logger.info(f"Mock payment processor: APPROVED (amount: {order.total_amount})")
    await asyncio.sleep(0.1)  # Simulate processing delay
    return True


async def run_payment_worker(channel: aio_pika.Channel):
    """
    Run the payment worker consumer.

    Consumes from payment.queue and processes order.created events.
    """
    queue = await channel.get_queue("payment.queue")

    logger.info("Payment worker started, listening on payment.queue")

    async def callback(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                # Get order_id from headers for logging
                order_id = message.headers.get("order_id", "unknown") if message.headers else "unknown"
                logger.info(f"Payment worker received message for order {order_id}")

                success = await process_payment_event(message.body)

                if success:
                    await message.ack()
                    logger.info(f"Payment worker acknowledged message for order {order_id}")
                else:
                    # Negative acknowledgement - message will be requeued
                    await message.nack(requeue=True)
                    logger.warning(f"Payment worker nacked message for order {order_id} (will retry)")

            except Exception as e:
                logger.error(f"Error in payment worker callback: {e}", exc_info=True)
                await message.nack(requeue=True)

    await queue.consume(callback)
