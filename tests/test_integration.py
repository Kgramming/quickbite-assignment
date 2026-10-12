"""Integration tests for Phase 2: workers, messaging, and order flow."""

import asyncio
import json
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, SessionLocal, get_session
from app.idempotency import ProcessedEvent, has_processed_event, record_processed_event, ProcessedEventStatus
from app.main import app
from app.models import Order, OrderStatus, OutboxEvent
from app.workers.delivery import process_delivery_event
from app.workers.notification import process_notification_event
from app.workers.payment import process_payment_event
from app.workers.restaurant import process_restaurant_event


@pytest.fixture
def integration_db():
    """Create a test database with all tables."""
    # Use actual SQLite for integration tests
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    Base.metadata.create_all(bind=engine)

    TestSessionLocal = sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )

    def get_session_override():
        session = TestSessionLocal()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = get_session_override

    session = TestSessionLocal()
    yield session
    session.close()
    
    # Cleanup
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture
def integration_client(integration_db):
    """Provide test client with integration database."""
    return TestClient(app)


class TestOrderFlow:
    """Test successful complete order flow."""

    def test_create_order(self, integration_client, integration_db):
        """Test creating an order via API."""
        payload = {
            "customer_name": "John Doe",
            "customer_email": "john@example.com",
            "customer_phone": "5551234567",
            "delivery_address": "123 Main St",
            "items": [{"name": "Pizza", "quantity": 1, "price": "12.99"}],
            "total_amount": "12.99",
        }

        response = integration_client.post("/orders", json=payload)
        assert response.status_code == 201

        data = response.json()
        assert data["status"] == "PENDING"
        assert data["payment_status"] == "pending"

        # Verify order in database
        order = integration_db.query(Order).filter(Order.id == data["id"]).first()
        assert order is not None
        assert order.status == OrderStatus.PENDING

        # Verify outbox event created
        outbox = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == data["id"]
        ).first()
        assert outbox is not None
        assert outbox.event_type == "order.created"
        assert outbox.published == "N"

    def test_payment_success_flow(self, integration_db):
        """Test successful payment processing."""
        # Create order
        order = Order(
            id="test-order-1",
            customer_name="Jane Smith",
            customer_email="jane@example.com",
            customer_phone="5559876543",
            delivery_address="456 Oak Ave",
            items=json.dumps([{"name": "Burger", "quantity": 1, "price": "9.99"}]),
            status=OrderStatus.PENDING,
            total_amount="9.99",
            payment_status="pending",
        )
        integration_db.add(order)
        integration_db.commit()

        # Create order.created event
        event_id = "evt-payment-1"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-1",
        }

        # Process payment event
        result = asyncio.run(process_payment_event(json.dumps(event_payload).encode(), integration_db))
        assert result is True

        # Verify order transitioned to PAID
        integration_db.refresh(order)
        assert order.status == OrderStatus.PAID
        assert order.payment_status == "success"

        # Verify outbox event created for payment.success
        outbox = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == "test-order-1",
            OutboxEvent.event_type == "order.paid",
        ).first()
        assert outbox is not None

        # Verify idempotency record created
        assert has_processed_event(integration_db, event_id, "payment")

    def test_payment_failure_flow(self, integration_db):
        """Test payment failure prevents downstream processing."""
        # Create order
        order = Order(
            id="test-order-fail",
            customer_name="Bob Johnson",
            customer_email="bob@example.com",
            customer_phone="5551111111",
            delivery_address="789 Pine Rd",
            items=json.dumps([{"name": "Expensive Item", "quantity": 1, "price": "999.99"}]),
            status=OrderStatus.PENDING,
            total_amount="999.99",
            payment_status="pending",
        )
        integration_db.add(order)
        integration_db.commit()

        # Create payment event for failing order
        event_id = "evt-payment-fail"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-fail",
        }

        # Process payment event (should fail)
        result = asyncio.run(process_payment_event(json.dumps(event_payload).encode(), integration_db))
        assert result is True

        # Verify order transitioned to PAYMENT_FAILED
        integration_db.refresh(order)
        assert order.status == OrderStatus.PAYMENT_FAILED
        assert order.payment_status == "failed"

        # Verify NO payment.success event was created
        success_events = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == "test-order-fail",
            OutboxEvent.event_type == "order.paid",
        ).all()
        assert len(success_events) == 0

    def test_restaurant_confirmation(self, integration_db):
        """Test restaurant confirmation."""
        # Create paid order
        order = Order(
            id="test-order-rest",
            customer_name="Alice Brown",
            customer_email="alice@example.com",
            customer_phone="5552222222",
            delivery_address="321 Elm St",
            items=json.dumps([{"name": "Salad", "quantity": 1, "price": "8.99"}]),
            status=OrderStatus.PAID,
            total_amount="8.99",
            payment_status="success",
        )
        integration_db.add(order)
        integration_db.commit()

        # Create payment.success event
        event_id = "evt-rest-1"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-rest",
        }

        # Process restaurant event
        result = asyncio.run(process_restaurant_event(json.dumps(event_payload).encode(), integration_db))
        assert result is True

        # Verify order transitioned to CONFIRMED
        integration_db.refresh(order)
        assert order.status == OrderStatus.CONFIRMED

        # Verify restaurant.confirmed event created
        outbox = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == "test-order-rest",
            OutboxEvent.event_type == "order.confirmed",
        ).first()
        assert outbox is not None

    def test_delivery_assignment(self, integration_db):
        """Test delivery assignment."""
        # Create confirmed order
        order = Order(
            id="test-order-deliv",
            customer_name="Charlie Davis",
            customer_email="charlie@example.com",
            customer_phone="5553333333",
            delivery_address="654 Maple Ave",
            items=json.dumps([{"name": "Sandwich", "quantity": 2, "price": "6.99"}]),
            status=OrderStatus.CONFIRMED,
            total_amount="13.98",
            payment_status="success",
        )
        integration_db.add(order)
        integration_db.commit()

        # Create restaurant.confirmed event
        event_id = "evt-deliv-1"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-deliv",
        }

        # Process delivery event
        result = asyncio.run(process_delivery_event(json.dumps(event_payload).encode(), integration_db))
        assert result is True

        # Verify order transitioned to DRIVER_ASSIGNED
        integration_db.refresh(order)
        assert order.status == OrderStatus.DRIVER_ASSIGNED

        # Verify delivery.assigned event created
        outbox = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == "test-order-deliv",
            OutboxEvent.event_type == "order.driver.assigned",
        ).first()
        assert outbox is not None


class TestIdempotency:
    """Test idempotency and duplicate handling."""

    def test_duplicate_payment_event_idempotent(self, integration_db):
        """Test that processing same payment event twice is idempotent."""
        # Create order
        order = Order(
            id="test-order-dup",
            customer_name="Diana Evans",
            customer_email="diana@example.com",
            customer_phone="5554444444",
            delivery_address="987 Cedar Ln",
            items=json.dumps([{"name": "Pasta", "quantity": 1, "price": "11.99"}]),
            status=OrderStatus.PENDING,
            total_amount="11.99",
            payment_status="pending",
        )
        integration_db.add(order)
        integration_db.commit()

        event_id = "evt-dup-1"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-dup",
        }

        # First processing
        result1 = asyncio.run(process_payment_event(json.dumps(event_payload).encode(), integration_db))
        assert result1 is True

        integration_db.refresh(order)
        assert order.status == OrderStatus.PAID

        # Second processing (duplicate) - should be idempotent
        result2 = asyncio.run(process_payment_event(json.dumps(event_payload).encode(), integration_db))
        assert result2 is True

        integration_db.refresh(order)
        # Order should still be PAID, not double-processed
        assert order.status == OrderStatus.PAID

        # Verify only one payment.success event
        success_events = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == "test-order-dup",
            OutboxEvent.event_type == "order.paid",
        ).all()
        assert len(success_events) == 1

    def test_out_of_order_event_rejection(self, integration_db):
        """Test that out-of-order events are handled safely."""
        # Create PENDING order
        order = Order(
            id="test-order-oooo",
            customer_name="Eve Frank",
            customer_email="eve@example.com",
            customer_phone="5555555555",
            delivery_address="159 Oak St",
            items=json.dumps([{"name": "Tacos", "quantity": 3, "price": "4.99"}]),
            status=OrderStatus.PENDING,
            total_amount="14.97",
            payment_status="pending",
        )
        integration_db.add(order)
        integration_db.commit()

        # Try to process restaurant event on PENDING order (should be safe)
        event_id = "evt-oooo-1"
        event_payload = {
            "event_id": event_id,
            "order_id": "test-order-oooo",
        }

        result = asyncio.run(process_restaurant_event(json.dumps(event_payload).encode(), integration_db))
        assert result is True  # Should succeed (acknowledge) but not modify order

        integration_db.refresh(order)
        # Order should still be PENDING
        assert order.status == OrderStatus.PENDING


class TestProcessedEventTracking:
    """Test processed event tracking and idempotency database."""

    def test_record_and_check_processed_event(self, integration_db):
        """Test recording and checking processed events."""
        event_id = "test-evt-123"
        worker_type = "payment"

        # Initially not processed
        assert not has_processed_event(integration_db, event_id, worker_type)

        # Record as processed
        record_processed_event(
            integration_db,
            event_id,
            worker_type,
            ProcessedEventStatus.SUCCESS,
            "Test result",
        )

        # Now should be marked as processed
        assert has_processed_event(integration_db, event_id, worker_type)

        # Verify database record
        record = integration_db.query(ProcessedEvent).filter(
            ProcessedEvent.event_id == event_id,
            ProcessedEvent.worker_type == worker_type,
        ).first()
        assert record is not None
        assert record.status == ProcessedEventStatus.SUCCESS
        assert record.result == "Test result"


class TestOutboxPattern:
    """Test outbox pattern implementation."""

    def test_order_and_outbox_atomic_transaction(self, integration_client, integration_db):
        """Test that order and outbox event are committed atomically."""
        payload = {
            "customer_name": "Frank Green",
            "customer_email": "frank@example.com",
            "customer_phone": "5556666666",
            "delivery_address": "753 Birch Blvd",
            "items": [{"name": "Donuts", "quantity": 1, "price": "5.99"}],
            "total_amount": "5.99",
        }

        response = integration_client.post("/orders", json=payload)
        assert response.status_code == 201

        order_id = response.json()["id"]

        # Both order and outbox event should exist
        order = integration_db.query(Order).filter(Order.id == order_id).first()
        assert order is not None

        outbox_events = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == order_id
        ).all()
        assert len(outbox_events) == 1

        outbox_event = outbox_events[0]
        assert outbox_event.event_type == "order.created"
        assert outbox_event.published == "N"

    def test_outbox_event_has_event_id(self, integration_client, integration_db):
        """Test that outbox events include event_id for idempotency."""
        payload = {
            "customer_name": "Grace Hill",
            "customer_email": "grace@example.com",
            "customer_phone": "5557777777",
            "delivery_address": "951 Ash Ct",
            "items": [{"name": "Coffee", "quantity": 1, "price": "3.99"}],
            "total_amount": "3.99",
        }

        response = integration_client.post("/orders", json=payload)
        order_id = response.json()["id"]

        outbox_event = integration_db.query(OutboxEvent).filter(
            OutboxEvent.order_id == order_id
        ).first()

        payload_data = json.loads(outbox_event.payload)
        assert "event_id" in payload_data
        assert payload_data["event_id"] == outbox_event.id
