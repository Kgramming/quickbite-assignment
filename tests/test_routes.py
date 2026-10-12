"""Tests for API routes."""

import pytest
from fastapi.testclient import TestClient


def test_health_check_broker_down(client: TestClient, monkeypatch):
    """Health is 'degraded' when RabbitMQ is unreachable (mocked).

    Mocked so the test is deterministic regardless of whether a broker
    happens to be running in the test environment.
    """
    import app.routes as routes_module

    async def fake_rabbitmq_check(timeout_seconds: float = 3.0) -> str:
        return "unhealthy: mocked connection refused"

    monkeypatch.setattr(routes_module, "_check_rabbitmq", fake_rabbitmq_check)
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "degraded"
    assert data["database"] == "healthy"
    assert data["rabbitmq"].startswith("unhealthy")
    assert data["version"] == "0.1.0"
    assert "timestamp" in data


def test_health_check_all_healthy(client: TestClient, monkeypatch):
    """Health is 'ok' when every dependency reports healthy (mocked)."""
    import app.routes as routes_module

    async def fake_rabbitmq_check(timeout_seconds: float = 3.0) -> str:
        return "healthy"

    monkeypatch.setattr(routes_module, "_check_rabbitmq", fake_rabbitmq_check)
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["rabbitmq"] == "healthy"


def test_create_order_success(client: TestClient):
    """Test successful order creation."""
    payload = {
        "customer_name": "John Doe",
        "customer_email": "john@example.com",
        "customer_phone": "5551234567",
        "delivery_address": "123 Main St",
        "items": [
            {"name": "Pizza Margherita", "quantity": 1, "price": "12.99"},
            {"name": "Coca-Cola", "quantity": 2, "price": "2.99"},
        ],
        "total_amount": "18.97",
    }

    response = client.post("/orders", json=payload)
    assert response.status_code == 201
    data = response.json()

    assert data["customer_name"] == "John Doe"
    assert data["customer_email"] == "john@example.com"
    assert data["status"] == "PENDING"
    assert data["payment_status"] == "pending"
    assert "id" in data
    assert "created_at" in data
    assert "updated_at" in data


def test_create_order_with_idempotency_key(client: TestClient):
    """Test order creation with idempotency key returns same order on retry."""
    payload = {
        "customer_name": "Jane Smith",
        "customer_email": "jane@example.com",
        "customer_phone": "5559876543",
        "delivery_address": "456 Oak Ave",
        "items": [{"name": "Burger", "quantity": 1, "price": "9.99"}],
        "total_amount": "9.99",
        "idempotency_key": "test-idempotency-key-001",
    }

    # First request
    response1 = client.post("/orders", json=payload)
    assert response1.status_code == 201
    order_id_1 = response1.json()["id"]

    # Second request with same idempotency key
    response2 = client.post("/orders", json=payload)
    assert response2.status_code == 201
    order_id_2 = response2.json()["id"]

    # Should return the same order
    assert order_id_1 == order_id_2


def test_get_order_success(client: TestClient):
    """Test successful order retrieval."""
    # Create an order first
    create_payload = {
        "customer_name": "Bob Johnson",
        "customer_email": "bob@example.com",
        "customer_phone": "5551111111",
        "delivery_address": "789 Pine Rd",
        "items": [{"name": "Pasta", "quantity": 2, "price": "8.99"}],
        "total_amount": "17.98",
    }

    create_response = client.post("/orders", json=create_payload)
    order_id = create_response.json()["id"]

    # Retrieve the order
    get_response = client.get(f"/orders/{order_id}")
    assert get_response.status_code == 200
    data = get_response.json()

    assert data["id"] == order_id
    assert data["customer_name"] == "Bob Johnson"
    assert data["customer_email"] == "bob@example.com"
    assert data["status"] == "PENDING"


def test_get_order_not_found(client: TestClient):
    """Test retrieval of non-existent order."""
    fake_uuid = "12345678-1234-5678-1234-567812345678"
    response = client.get(f"/orders/{fake_uuid}")
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_create_order_invalid_request(client: TestClient):
    """Test order creation with invalid request data."""
    payload = {
        "customer_name": "",  # Invalid: empty name
        "customer_email": "invalid-email",  # Invalid: not an email
        "customer_phone": "123",  # Invalid: too short
        "delivery_address": "123 St",
        "items": [],  # Invalid: no items
        "total_amount": "0.00",
    }

    response = client.post("/orders", json=payload)
    assert response.status_code == 422  # Validation error


def test_create_order_invalid_price_format(client: TestClient):
    """Test order creation with invalid price format."""
    payload = {
        "customer_name": "John Doe",
        "customer_email": "john@example.com",
        "customer_phone": "5551234567",
        "delivery_address": "123 Main St",
        "items": [{"name": "Pizza", "quantity": 1, "price": "12"}],  # Invalid: missing decimals
        "total_amount": "12.99",
    }

    response = client.post("/orders", json=payload)
    assert response.status_code == 422
