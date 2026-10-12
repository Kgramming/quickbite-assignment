"""Unified worker service runner for all four consumers."""

import asyncio
import logging
import signal
from typing import List

import aio_pika

from app.config import settings
from app.database import init_db
from app.events import QUEUE_CONFIG
from app.rabbitmq import RabbitMQManager
from app.workers.delivery import run_delivery_worker
from app.workers.notification import run_notification_worker
from app.workers.payment import run_payment_worker
from app.workers.restaurant import run_restaurant_worker

logger = logging.getLogger(__name__)


WORKER_RUNNERS = {
    "payment": run_payment_worker,
    "restaurant": run_restaurant_worker,
    "delivery": run_delivery_worker,
    "notification": run_notification_worker,
}


class WorkerService:
    """Manages worker consumers."""

    def __init__(self, workers: List[str] = None):
        """Initialize worker service.

        Args:
            workers: Subset of {"payment", "restaurant", "delivery",
                "notification"} to run. Defaults to all four. Running a
                subset is useful for demos (e.g. notification downtime)
                and for scaling individual workers.
        """
        unknown = set(workers or []) - set(WORKER_RUNNERS)
        if unknown:
            raise ValueError(f"Unknown workers: {sorted(unknown)}")
        self.worker_names = workers or list(WORKER_RUNNERS)
        self.rabbitmq: RabbitMQManager = None
        self.channels: List[aio_pika.Channel] = []
        self.tasks: List[asyncio.Task] = []

    async def start(self):
        """Start all workers."""
        logger.info("Starting worker service...")

        # Initialize database
        init_db()
        logger.info("Database initialized")

        # Connect to RabbitMQ (connect() returns the manager; raises on failure)
        rabbitmq_url = (
            f"amqp://{settings.rabbitmq_user}:{settings.rabbitmq_password}"
            f"@{settings.rabbitmq_host}:{settings.rabbitmq_port}/{settings.rabbitmq_vhost}"
        )
        logger.info(f"Connecting to RabbitMQ at {settings.rabbitmq_host}:{settings.rabbitmq_port}")

        self.rabbitmq = await RabbitMQManager(rabbitmq_url).connect()

        logger.info("RabbitMQ connected")

        # Declare retry + DLQ topology and all worker queues/bindings.
        # Single source of truth shared with the outbox publisher.
        await self.rabbitmq.declare_application_topology(QUEUE_CONFIG)

        logger.info("RabbitMQ topology declared")

        # Start workers, each on its own channel so that a
        # channel-level error in one consumer cannot take down the others
        # and prefetch applies per worker rather than being shared.
        logger.info(f"Starting consumers: {', '.join(self.worker_names)}...")
        for name in self.worker_names:
            channel = await self.rabbitmq.new_channel(prefetch_count=1)
            self.channels.append(channel)
            self.tasks.append(asyncio.create_task(WORKER_RUNNERS[name](channel)))

        logger.info("Workers started")
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

        # Close channels and RabbitMQ connection
        for channel in self.channels:
            try:
                if not channel.is_closed:
                    await channel.close()
            except Exception:
                pass

        # Close RabbitMQ connection
        if self.rabbitmq:
            await self.rabbitmq.close()

        logger.info("Worker service stopped")


async def run_workers(workers: List[str] = None):
    """Run the worker service."""
    service = WorkerService(workers)

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
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="QuickBite worker service")
    parser.add_argument(
        "--workers",
        default=",".join(WORKER_RUNNERS),
        help="Comma-separated subset of workers to run "
        "(default: payment,restaurant,delivery,notification)",
    )
    args = parser.parse_args()
    selected = [w.strip() for w in args.workers.split(",") if w.strip()]

    asyncio.run(run_workers(selected))
