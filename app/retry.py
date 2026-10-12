"""Bounded retry policy for worker message processing.

Strategy (no broker plugins required):
  - Each message carries an ``x-retry-count`` header (default 0).
  - On transient failure with count < MAX_RETRIES, the worker publishes
    the message to the ``quickbite.retry`` fanout exchange with a
    per-message TTL (5s / 10s / 20s backoff) and acks the original.
    ``orders.retry.queue`` dead-letters expired messages back to
    ``quickbite.orders`` with their original routing key, so they are
    redelivered to the originating worker queue.
  - When retries are exhausted (or the failure is non-retryable), the
    worker rejects with requeue=False; the queue's
    ``x-dead-letter-exchange`` routes the message to
    ``orders.dead-letter.queue`` and the order is marked FAILED.
"""

import logging
from datetime import timedelta

import aio_pika

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAYS_SECONDS = [5.0, 10.0, 20.0]
RETRY_COUNT_HEADER = "x-retry-count"


class NonRetryableError(Exception):
    """Raised when a message can never succeed (poison message).

    The worker must reject it straight to the dead-letter queue instead
    of burning retries.
    """


def get_retry_count(message: aio_pika.IncomingMessage) -> int:
    """Read the retry attempt count from message headers (0 if absent)."""
    try:
        return int((message.headers or {}).get(RETRY_COUNT_HEADER, 0))
    except (TypeError, ValueError):
        return 0


def next_retry_delay_seconds(retry_count: int) -> float:
    """Backoff delay for the *next* attempt, given attempts already made."""
    idx = min(max(retry_count, 0), len(RETRY_DELAYS_SECONDS) - 1)
    return RETRY_DELAYS_SECONDS[idx]


async def republish_for_retry(
    retry_exchange: aio_pika.Exchange,
    message: aio_pika.IncomingMessage,
    routing_key: str,
) -> int:
    """Publish a delayed-retry copy of the message.

    The retry copy is published with publisher confirms: this coroutine
    only returns after the broker confirms receipt. Callers must ack the
    original message only after this returns.

    Returns:
        The new retry count carried by the retry copy.
    """
    count = get_retry_count(message)
    delay = next_retry_delay_seconds(count)
    headers = dict(message.headers or {})
    headers[RETRY_COUNT_HEADER] = count + 1

    retry_message = aio_pika.Message(
        body=message.body,
        headers=headers,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=message.message_id,
        expiration=timedelta(seconds=delay),
    )
    await retry_exchange.publish(retry_message, routing_key=routing_key)
    logger.info(
        f"Scheduled retry {count + 1}/{MAX_RETRIES} in {delay:.0f}s "
        f"(routing_key={routing_key})"
    )
    return count + 1
