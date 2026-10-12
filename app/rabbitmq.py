"""RabbitMQ topology and connection management."""

import logging
from typing import Optional

import aio_pika
from aio_pika import Connection, Exchange, Queue

logger = logging.getLogger(__name__)


class RabbitMQManager:
    """Manages RabbitMQ connection, topology, and declarations."""

    def __init__(self, url: str):
        """Initialize with RabbitMQ URL."""
        self.url = url
        self.connection: Optional[Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[Exchange] = None

    async def connect(self):
        """Establish connection and declare topology."""
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()

        # Set QoS (prefetch count) for fair distribution
        await self.channel.set_qos(prefetch_count=1)

        # Declare durable topic exchange
        self.exchange = await self.channel.declare_exchange(
            name="quickbite.orders",
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        logger.info("✓ RabbitMQ connected and topology declared")

    async def declare_queue(self, queue_name: str, routing_keys: list[str]) -> Queue:
        """
        Declare a durable queue with bindings to the topic exchange.

        Args:
            queue_name: Name of the queue
            routing_keys: List of routing key patterns to bind

        Returns:
            The declared Queue object
        """
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")

        # Declare main queue as durable
        queue = await self.channel.declare_queue(
            name=queue_name,
            durable=True,
        )

        # Bind to exchange with each routing key
        for routing_key in routing_keys:
            await queue.bind(self.exchange, routing_key=routing_key)
            logger.info(f"  Bound {queue_name} to {routing_key}")

        return queue

    async def declare_dead_letter_queue(self) -> Queue:
        """Declare the dead-letter queue for failed messages."""
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")

        # Dead-letter queue is a standard durable queue
        dlq = await self.channel.declare_queue(
            name="orders.dead-letter.queue",
            durable=True,
        )

        logger.info("✓ Dead-letter queue declared")
        return dlq

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
