"""Tests for database models."""

import json
import uuid

from app.models import Order, OrderStatus, OutboxEvent


def test_order_model():
    """Test Order model creation with values."""
    order_id = str(uuid.uuid4())
    order = Order(
        id=order_id,
        customer_name="John Doe",
        customer_email="john@example.com",
        customer_phone="5551234567",
        delivery_address="123 Main St",
        items=json.dumps([{"name": "Pizza", "quantity": 1, "price": "12.99"}]),
        status=OrderStatus.PENDING,
        total_amount="12.99",
        payment_status="pending",
    )

    # Verify assigned values
    assert order.id == order_id
    assert order.customer_name == "John Doe"
    assert order.customer_email == "john@example.com"
    assert order.customer_phone == "5551234567"
    assert order.status == OrderStatus.PENDING
    assert order.payment_status == "pending"
    assert order.total_amount == "12.99"


def test_order_status_enum():
    """Test OrderStatus enumeration values."""
    assert OrderStatus.PENDING.value == "PENDING"
    assert OrderStatus.PAID.value == "PAID"
    assert OrderStatus.CONFIRMED.value == "CONFIRMED"
    assert OrderStatus.DRIVER_ASSIGNED.value == "DRIVER_ASSIGNED"
    assert OrderStatus.PAYMENT_FAILED.value == "PAYMENT_FAILED"
    assert OrderStatus.CANCELLED.value == "CANCELLED"


def test_outbox_event_model():
    """Test OutboxEvent model creation."""
    event_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    event = OutboxEvent(
        id=event_id,
        event_type="order.created",
        order_id=order_id,
        payload=json.dumps({"test": "data"}),
        published="N",
    )

    assert event.id == event_id
    assert event.event_type == "order.created"
    assert event.order_id == order_id
    assert event.published == "N"
    assert event.published_at is None
