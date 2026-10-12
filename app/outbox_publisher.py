"""Outbox publisher: polls database and publishes events to RabbitMQ.

The publisher only marks an outbox row ``published='Y'`` after the broker
confirms receipt (publisher confirms are enabled on the connection), so
a row marked published is guaranteed to be in RabbitMQ. Rows left at
``'N'`` are retried on the next poll.
"""

import asyncio
import json
import logging
from datetime import datetime

import aio_pika

from app.config import settings
from app.database import SessionLocal
from app.events import OUTBOX_ROUTING_KEYS, QUEUE_CONFIG
from app.models import OutboxEvent
from app.rabbitmq import RabbitMQManager

logger = logging.getLogger(__name__)

PUBLISH_TIMEOUT_SECONDS = 10


class OutboxPublisher:
    """Poll outbox table and publish events to RabbitMQ."""

    def __init__(self, rabbitmq_manager: RabbitMQManager, poll_interval: float = 2.0):
        """
        Initialize the outbox publisher.

        Args:
            rabbitmq_manager: Connected RabbitMQ manager (confirms enabled)
            poll_interval: How often to poll for unpublished events (seconds)
        """
        self.rabbitmq = rabbitmq_manager
        self.poll_interval = poll_interval
        self.running = False

    async def publish_events(self):
        """Publish all unpublished outbox events to RabbitMQ."""
        session = SessionLocal()
        try:
            # Query all unpublished events, oldest first for ordering.
            unpublished = (
                session.query(OutboxEvent)
                .filter(OutboxEvent.published == "N")
                .order_by(OutboxEvent.created_at)
                .all()
            )

            if not unpublished:
                return

            logger.info(f"Publishing {len(unpublished)} unpublished event(s)")

            for outbox_event in unpublished:
                try:
                    # Resolve routing key from the explicit allow-list.
                    # Unknown event types are never published silently.
                    routing_key = OUTBOX_ROUTING_KEYS.get(outbox_event.event_type)
                    if routing_key is None:
                        logger.error(
                            f"Unknown event_type '{outbox_event.event_type}' "
                            f"for outbox event {outbox_event.id}; leaving unpublished"
                        )
                        continue

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

                    # Publish with broker confirmation: with publisher
                    # confirms enabled, this raises if the broker nacks
                    # (or on timeout), so the row below is only marked
                    # published after the broker has accepted the message.
                    await self.rabbitmq.exchange.publish(
                        message,
                        routing_key=routing_key,
                        timeout=PUBLISH_TIMEOUT_SECONDS,
                    )

                    logger.info(
                        f"Published event {outbox_event.id} "
                        f"(type: {outbox_event.event_type}, routing_key: {routing_key})"
                    )

                    # Mark as published ONLY after broker confirmation.
                    outbox_event.published = "Y"
                    outbox_event.published_at = datetime.utcnow()
                    session.add(outbox_event)
                    session.commit()

                except Exception as e:
                    logger.error(f"Failed to publish event {outbox_event.id}: {e}")
                    session.rollback()
                    # Leave published='N' so it will be retried on next poll.

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
    # connect() returns the manager (and raises if the broker is unreachable).
    rabbitmq = await RabbitMQManager(rabbitmq_url).connect()

    # Declare retry + DLQ topology and all worker queues/bindings.
    # Single source of truth shared with the worker service.
    await rabbitmq.declare_application_topology(QUEUE_CONFIG)

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
