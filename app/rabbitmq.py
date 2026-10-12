"""RabbitMQ topology and connection management.

Topology (all durable):
  - quickbite.orders (topic)      : business events
      payment.queue      <- order.created
      restaurant.queue   <- order.paid
      delivery.queue     <- order.confirmed
      notification.queue <- order.#
  - quickbite.retry (fanout)      : delayed retries
      orders.retry.queue (TTL per message, DLX -> quickbite.orders)
  - dlx.quickbite.orders (topic)  : dead-letter exchange
      orders.dead-letter.queue <- #

Every worker queue declares x-dead-letter-exchange so rejected messages
can never be silently dropped: they land in orders.dead-letter.queue.
"""

import logging
from typing import Optional

import aio_pika
from aio_pika import Connection, Exchange, Queue

logger = logging.getLogger(__name__)

MAIN_EXCHANGE = "quickbite.orders"
RETRY_EXCHANGE = "quickbite.retry"
DLX_EXCHANGE = "dlx.quickbite.orders"
RETRY_QUEUE = "orders.retry.queue"
DLQ_QUEUE = "orders.dead-letter.queue"


class RabbitMQManager:
    """Manages RabbitMQ connection, topology, and declarations."""

    def __init__(self, url: str):
        """Initialize with RabbitMQ URL."""
        self.url = url
        self.connection: Optional[Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[Exchange] = None
        self.retry_exchange: Optional[Exchange] = None

    async def connect(self) -> "RabbitMQManager":
        """Establish connection and declare core topology.

        Publisher confirms are enabled so every publish is acknowledged
        by the broker (or raises); callers must only treat a message as
        sent after ``publish`` returns without error.

        Returns:
            self, so callers can write
            ``manager = await RabbitMQManager(url).connect()``.
        """
        self.connection = await aio_pika.connect_robust(
            self.url, publisher_confirms=True
        )
        self.channel = await self.connection.channel(publisher_confirms=True)

        # Set QoS (prefetch count) for fair distribution
        await self.channel.set_qos(prefetch_count=1)

        # Declare durable topic exchange for business events
        self.exchange = await self.channel.declare_exchange(
            name=MAIN_EXCHANGE,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        # Declare durable fanout exchange for delayed retries
        self.retry_exchange = await self.channel.declare_exchange(
            name=RETRY_EXCHANGE,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        )

        logger.info("RabbitMQ connected and core topology declared")
        return self

    async def new_channel(self, prefetch_count: int = 1) -> aio_pika.Channel:
        """Open an independent channel with its own prefetch budget.

        Each worker gets its own channel so a channel-level error in one
        consumer cannot take down the others, and prefetch applies
        per worker rather than being shared.
        """
        if not self.connection:
            raise RuntimeError("Not connected to RabbitMQ")
        channel = await self.connection.channel(publisher_confirms=True)
        await channel.set_qos(prefetch_count=prefetch_count)
        return channel

    async def declare_queue(
        self,
        queue_name: str,
        routing_keys: list[str],
        channel: Optional[aio_pika.Channel] = None,
    ) -> Queue:
        """
        Declare a durable queue with bindings to the topic exchange.

        The queue declares ``x-dead-letter-exchange`` so that rejected
        (non-requeued) messages are routed to the dead-letter exchange
        instead of being dropped.

        Args:
            queue_name: Name of the queue
            routing_keys: List of routing key patterns to bind
            channel: Channel to declare on (defaults to manager channel)

        Returns:
            The declared Queue object
        """
        ch = channel or self.channel
        if not ch:
            raise RuntimeError("Not connected to RabbitMQ")

        queue = await ch.declare_queue(
            name=queue_name,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX_EXCHANGE},
        )

        for routing_key in routing_keys:
            await queue.bind(self.exchange, routing_key=routing_key)
            logger.info(f"  Bound {queue_name} to {routing_key}")

        return queue

    async def declare_retry_topology(
        self, channel: Optional[aio_pika.Channel] = None
    ) -> Queue:
        """Declare the delayed-retry loop.

        ``orders.retry.queue`` holds messages with a per-message TTL.
        On expiry the broker dead-letters them back to ``quickbite.orders``
        with their *original* routing key, so they are redelivered to the
        worker queue they came from. No plugins required.
        """
        ch = channel or self.channel
        if not ch:
            raise RuntimeError("Not connected to RabbitMQ")

        queue = await ch.declare_queue(
            name=RETRY_QUEUE,
            durable=True,
            arguments={"x-dead-letter-exchange": MAIN_EXCHANGE},
        )
        retry_exchange = await ch.declare_exchange(
            name=RETRY_EXCHANGE,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        )
        await queue.bind(retry_exchange)
        logger.info("Retry topology declared")
        return queue

    async def declare_dead_letter_topology(
        self, channel: Optional[aio_pika.Channel] = None
    ) -> Queue:
        """Declare the dead-letter exchange and queue.

        Rejected (non-requeued) messages from any worker queue arrive
        here with their original routing key preserved.
        """
        ch = channel or self.channel
        if not ch:
            raise RuntimeError("Not connected to RabbitMQ")

        dlx = await ch.declare_exchange(
            name=DLX_EXCHANGE,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        dlq = await ch.declare_queue(
            name=DLQ_QUEUE,
            durable=True,
        )
        await dlq.bind(dlx, routing_key="#")
        logger.info("Dead-letter topology declared")
        return dlq

    async def declare_application_topology(
        self, queue_config: dict[str, list[str]]
    ) -> None:
        """Declare retry + DLQ topology and all worker queues/bindings.

        Single source of truth used by both the worker service and the
        outbox publisher, so topology cannot drift between entrypoints.
        """
        await self.declare_retry_topology()
        await self.declare_dead_letter_topology()
        for queue_name, routing_keys in queue_config.items():
            await self.declare_queue(queue_name, routing_keys)
        logger.info("Application topology declared")

    async def get_retry_exchange(
        self, channel: aio_pika.Channel
    ) -> Exchange:
        """Return the retry exchange bound to the given channel.

        Declaring is idempotent, so each worker channel gets its own
        correctly-bound exchange object.
        """
        return await channel.declare_exchange(
            name=RETRY_EXCHANGE,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        )

    async def close(self):
        """Close connection."""
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed")


async def create_rabbitmq_manager(rabbitmq_url: str) -> RabbitMQManager:
    """Factory to create and connect RabbitMQ manager."""
    manager = RabbitMQManager(rabbitmq_url)
    await manager.connect()
    return manager
