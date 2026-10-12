"""Tests for Pydantic schemas."""

import pytest
from pydantic import ValidationError

from app.schemas import CreateOrderRequest, OrderItemSchema


def test_order_item_schema_valid():
    """Test valid order item creation."""
    item = OrderItemSchema(name="Pizza", quantity=2, price="12.99")
    assert item.name == "Pizza"
    assert item.quantity == 2
    assert item.price == "12.99"


def test_order_item_schema_invalid_price():
    """Test order item with invalid price format."""
    with pytest.raises(ValidationError) as exc_info:
        OrderItemSchema(name="Pizza", quantity=2, price="12.9")
    assert "regex" in str(exc_info.value).lower() or "price" in str(exc_info.value).lower()


def test_order_item_schema_zero_quantity():
    """Test order item with invalid quantity."""
    with pytest.raises(ValidationError) as exc_info:
        OrderItemSchema(name="Pizza", quantity=0, price="12.99")
    assert "greater than 0" in str(exc_info.value).lower()


def test_create_order_request_valid():
    """Test valid order creation request."""
    request = CreateOrderRequest(
        customer_name="John Doe",
        customer_email="john@example.com",
        customer_phone="5551234567",
        delivery_address="123 Main St",
        items=[
            OrderItemSchema(name="Pizza", quantity=1, price="12.99"),
            OrderItemSchema(name="Drink", quantity=2, price="2.99"),
        ],
        total_amount="18.97",
    )

    assert request.customer_name == "John Doe"
    assert request.customer_email == "john@example.com"
    assert len(request.items) == 2


def test_create_order_request_invalid_email():
    """Test order request with invalid email."""
    with pytest.raises(ValidationError) as exc_info:
        CreateOrderRequest(
            customer_name="John Doe",
            customer_email="not-an-email",
            customer_phone="5551234567",
            delivery_address="123 Main St",
            items=[OrderItemSchema(name="Pizza", quantity=1, price="12.99")],
            total_amount="12.99",
        )
    assert "email" in str(exc_info.value).lower()


def test_create_order_request_empty_items():
    """Test order request with no items."""
    with pytest.raises(ValidationError) as exc_info:
        CreateOrderRequest(
            customer_name="John Doe",
            customer_email="john@example.com",
            customer_phone="5551234567",
            delivery_address="123 Main St",
            items=[],
            total_amount="0.00",
        )
    assert "at least 1 item" in str(exc_info.value).lower()


def test_create_order_request_with_idempotency_key():
    """Test order request with idempotency key."""
    request = CreateOrderRequest(
        customer_name="John Doe",
        customer_email="john@example.com",
        customer_phone="5551234567",
        delivery_address="123 Main St",
        items=[OrderItemSchema(name="Pizza", quantity=1, price="12.99")],
        total_amount="12.99",
        idempotency_key="unique-key-123",
    )

    assert request.idempotency_key == "unique-key-123"
