"""Unified worker service runner for all four consumers."""

import asyncio
import logging
import signal
from typing import List

import aio_pika

from app.config import settings
from app.database import init_db
from app.rabbitmq import RabbitMQManager
from app.workers.delivery import run_delivery_worker
from app.workers.notification import run_notification_worker
from app.workers.payment import run_payment_worker
from app.workers.restaurant import run_restaurant_worker

logger = logging.getLogger(__name__)


class WorkerService:
    """Manages all four worker consumers."""

    def __init__(self):
        """Initialize worker service."""
        self.rabbitmq: RabbitMQManager = None
        self.channel: aio_pika.Channel = None
        self.tasks: List[asyncio.Task] = []

    async def start(self):
        """Start all workers."""
        logger.info("Starting worker service...")

        # Initialize database
        init_db()
        logger.info("✓ Database initialized")

        # Connect to RabbitMQ
        rabbitmq_url = (
            f"amqp://{settings.rabbitmq_user}:{settings.rabbitmq_password}"
            f"@{settings.rabbitmq_host}:{settings.rabbitmq_port}/{settings.rabbitmq_vhost}"
        )
        logger.info(f"Connecting to RabbitMQ at {settings.rabbitmq_host}:{settings.rabbitmq_port}")

        self.rabbitmq = await RabbitMQManager(rabbitmq_url).connect()
        self.channel = self.rabbitmq.channel

        logger.info("✓ RabbitMQ connected")

        # Declare queues and bindings
        await self.rabbitmq.declare_queue("payment.queue", ["order.created"])
        await self.rabbitmq.declare_queue("restaurant.queue", ["order.payment.success"])
        await self.rabbitmq.declare_queue("delivery.queue", ["order.restaurant.confirmed"])
        await self.rabbitmq.declare_queue("notification.queue", ["order.#"])
        await self.rabbitmq.declare_dead_letter_queue()

        logger.info("✓ RabbitMQ topology declared")

        # Start all workers
        logger.info("Starting consumers...")
        self.tasks = [
            asyncio.create_task(run_payment_worker(self.channel)),
            asyncio.create_task(run_restaurant_worker(self.channel)),
            asyncio.create_task(run_delivery_worker(self.channel)),
            asyncio.create_task(run_notification_worker(self.channel)),
        ]

        logger.info("✓ All workers started")
        logger.info("")
        logger.info("=" * 60)
        logger.info("Worker service running. Press Ctrl+C to stop.")
        logger.info("=" * 60)
        logger.info("")

        # Wait for all tasks
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            logger.info("Worker tasks cancelled")

    async def stop(self):
        """Stop all workers."""
        logger.info("Stopping worker service...")

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Close RabbitMQ connection
        if self.rabbitmq:
            await self.rabbitmq.close()

        logger.info("Worker service stopped")


async def run_workers():
    """Run the worker service."""
    service = WorkerService()

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        asyncio.create_task(service.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        await service.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    asyncio.run(run_workers())
