"""Event schemas and business event definitions."""

import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Types of order events.

    Values are the AMQP routing keys used on the ``quickbite.orders``
    topic exchange, aligned with the assignment specification.
    """

    ORDER_CREATED = "order.created"
    PAYMENT_INITIATED = "order.payment.initiated"
    PAYMENT_SUCCESS = "order.paid"
    PAYMENT_FAILED = "order.payment.failed"
    RESTAURANT_CONFIRMED = "order.confirmed"
    DELIVERY_ASSIGNED = "order.driver.assigned"
    NOTIFICATION_SENT = "order.notification.sent"


class BusinessEvent(BaseModel):
    """Base class for all business events."""

    event_id: str = Field(..., description="Unique event ID (UUID)")
    event_type: EventType = Field(..., description="Type of event")
    order_id: str = Field(..., description="Order UUID")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event creation time")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event-specific data")

    def to_json(self) -> str:
        """Serialize to JSON."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> "BusinessEvent":
        """Deserialize from JSON."""
        return cls.model_validate_json(data)

    class Config:
        use_enum_values = True


class OrderCreatedEvent(BusinessEvent):
    """Event: Order created (from POST /orders)."""

    event_type: EventType = EventType.ORDER_CREATED
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains customer, items, and total_amount",
    )


class PaymentInitiatedEvent(BusinessEvent):
    """Event: Payment processing started."""

    event_type: EventType = EventType.PAYMENT_INITIATED


class PaymentSuccessEvent(BusinessEvent):
    """Event: Payment succeeded."""

    event_type: EventType = EventType.PAYMENT_SUCCESS
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains transaction_id, amount",
    )


class PaymentFailedEvent(BusinessEvent):
    """Event: Payment failed."""

    event_type: EventType = EventType.PAYMENT_FAILED
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains failure_reason",
    )


class RestaurantConfirmedEvent(BusinessEvent):
    """Event: Restaurant confirmed order."""

    event_type: EventType = EventType.RESTAURANT_CONFIRMED
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains restaurant_id, estimated_time",
    )


class DeliveryAssignedEvent(BusinessEvent):
    """Event: Driver assigned to delivery."""

    event_type: EventType = EventType.DELIVERY_ASSIGNED
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains driver_id, driver_phone, vehicle_info",
    )


class NotificationSentEvent(BusinessEvent):
    """Event: Notification sent to customer."""

    event_type: EventType = EventType.NOTIFICATION_SENT
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Contains channel (email/sms), recipient, status",
    )


# Routing key mapping for RabbitMQ topic exchange.
# Keys follow the assignment specification exactly:
#   order.created, order.paid, order.payment.failed,
#   order.confirmed, order.driver.assigned
EVENT_ROUTING_KEYS = {
    EventType.ORDER_CREATED: "order.created",
    EventType.PAYMENT_INITIATED: "order.payment.initiated",
    EventType.PAYMENT_SUCCESS: "order.paid",
    EventType.PAYMENT_FAILED: "order.payment.failed",
    EventType.RESTAURANT_CONFIRMED: "order.confirmed",
    EventType.DELIVERY_ASSIGNED: "order.driver.assigned",
    EventType.NOTIFICATION_SENT: "order.notification.sent",
}


# Queue configuration mapping: queue name -> routing keys bound on quickbite.orders
QUEUE_CONFIG = {
    "payment.queue": [
        "order.created",
    ],
    "restaurant.queue": [
        "order.paid",
    ],
    "delivery.queue": [
        "order.confirmed",
    ],
    "notification.queue": [
        "order.#",  # Listen to all order events
    ],
}


# Mapping from outbox_events.event_type (stored string) to AMQP routing key.
# Workers store the dotted spec form directly, so this is an explicit
# allow-list: unknown event types are never published silently.
OUTBOX_ROUTING_KEYS = {
    "order.created": "order.created",
    "order.paid": "order.paid",
    "order.payment.failed": "order.payment.failed",
    "order.confirmed": "order.confirmed",
    "order.driver.assigned": "order.driver.assigned",
}
