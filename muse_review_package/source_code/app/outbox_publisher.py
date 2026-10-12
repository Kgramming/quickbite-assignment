"""Outbox publisher: polls database and publishes events to RabbitMQ."""

import asyncio
import json
import logging
from datetime import datetime

import aio_pika

from app.config import settings
from app.database import SessionLocal
from app.events import EVENT_ROUTING_KEYS, EventType
from app.models import OutboxEvent
from app.rabbitmq import RabbitMQManager

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """Poll outbox table and publish events to RabbitMQ."""

    def __init__(self, rabbitmq_manager: RabbitMQManager, poll_interval: float = 2.0):
        """
        Initialize the outbox publisher.

        Args:
            rabbitmq_manager: RabbitMQ manager instance
            poll_interval: How often to poll for unpublished events (seconds)
        """
        self.rabbitmq = rabbitmq_manager
        self.poll_interval = poll_interval
        self.running = False

    async def publish_events(self):
        """Publish all unpublished outbox events to RabbitMQ."""
        session = SessionLocal()
        try:
            # Query all unpublished events
            unpublished = session.query(OutboxEvent).filter(
                OutboxEvent.published == "N"
            ).all()

            if not unpublished:
                return

            logger.info(f"Publishing {len(unpublished)} unpublished event(s)")

            for outbox_event in unpublished:
                try:
                    # Determine routing key based on event type
                    event_type = outbox_event.event_type
                    routing_key = EVENT_ROUTING_KEYS.get(event_type, f"order.{event_type}")

                    # Create AMQP message
                    message = aio_pika.Message(
                        body=outbox_event.payload.encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=outbox_event.id,
                        headers={
                            "order_id": outbox_event.order_id,
                            "event_type": outbox_event.event_type,
                            "created_at": str(outbox_event.created_at),
                        },
                    )

                    # Publish with confirmation
                    await self.rabbitmq.exchange.publish(
                        message,
                        routing_key=routing_key,
                    )

                    logger.info(
                        f"Published event {outbox_event.id} "
                        f"(type: {event_type}, routing_key: {routing_key})"
                    )

                    # Mark as published AFTER confirmation
                    outbox_event.published = "Y"
                    outbox_event.published_at = datetime.utcnow()
                    session.add(outbox_event)
                    session.commit()

                except Exception as e:
                    logger.error(f"Failed to publish event {outbox_event.id}: {e}")
                    session.rollback()
                    # Leave unpublished=N so it will be retried

        finally:
            session.close()

    async def run(self):
        """
        Run the publisher loop.

        Polls the outbox table at regular intervals and publishes unpublished events.
        """
        self.running = True
        logger.info("Outbox publisher started")

        try:
            while self.running:
                try:
                    await self.publish_events()
                except Exception as e:
                    logger.error(f"Error in publish loop: {e}")

                # Wait before next poll
                await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.info("Outbox publisher stopping (cancelled)")
            self.running = False
        except Exception as e:
            logger.error(f"Fatal error in outbox publisher: {e}")
            self.running = False

    async def stop(self):
        """Stop the publisher."""
        self.running = False
        logger.info("Outbox publisher stopped")


async def run_outbox_publisher(poll_interval: float = 2.0):
    """
    Run the outbox publisher as a standalone service.

    Args:
        poll_interval: How often to poll for unpublished events (seconds)
    """
    rabbitmq_url = (
        f"amqp://{settings.rabbitmq_user}:{settings.rabbitmq_password}"
        f"@{settings.rabbitmq_host}:{settings.rabbitmq_port}/{settings.rabbitmq_vhost}"
    )

    logger.info(f"Connecting to RabbitMQ at {settings.rabbitmq_host}:{settings.rabbitmq_port}")
    rabbitmq = await RabbitMQManager(rabbitmq_url).connect()

    # Declare topology
    await rabbitmq.declare_queue("payment.queue", [EVENT_ROUTING_KEYS[EventType.ORDER_CREATED]])
    await rabbitmq.declare_queue("restaurant.queue", [EVENT_ROUTING_KEYS[EventType.PAYMENT_SUCCESS]])
    await rabbitmq.declare_queue("delivery.queue", [EVENT_ROUTING_KEYS[EventType.RESTAURANT_CONFIRMED]])
    await rabbitmq.declare_queue("notification.queue", ["order.#"])
    await rabbitmq.declare_dead_letter_queue()

    publisher = OutboxPublisher(rabbitmq, poll_interval=poll_interval)

    try:
        await publisher.run()
    finally:
        await rabbitmq.close()


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_outbox_publisher(poll_interval=2.0))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
