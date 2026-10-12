"""SQLAlchemy models for QuickBite orders and events."""

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import Column, DateTime, Enum as SQLEnum, String, Text, UniqueConstraint

from app.database import Base


def _generate_uuid():
    """Generate a UUID string."""
    return str(uuid.uuid4())


class OrderStatus(str, Enum):
    """Order status enumeration."""

    PENDING = "PENDING"
    PAID = "PAID"
    CONFIRMED = "CONFIRMED"
    DRIVER_ASSIGNED = "DRIVER_ASSIGNED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"  # Message exhausted retries / dead-lettered


class Order(Base):
    """Order model with status tracking and payment outcome."""

    __tablename__ = "orders"

    # Primary key (as string UUID for database compatibility)
    id = Column(String(36), primary_key=True, default=_generate_uuid, unique=True, nullable=False)

    # Order details
    customer_name = Column(String(255), nullable=False)
    customer_email = Column(String(255), nullable=False)
    customer_phone = Column(String(20), nullable=False)
    delivery_address = Column(Text, nullable=False)

    # Order items (stored as JSON for simplicity)
    items = Column(Text, nullable=False)  # JSON string

    # Status and payment
    status = Column(SQLEnum(OrderStatus), nullable=False, default=OrderStatus.PENDING)
    total_amount = Column(String(10), nullable=False)  # Store as string to avoid float precision issues
    payment_status = Column(String(50), nullable=True)  # null, "pending", "success", "failed"

    # Idempotency key for duplicate request prevention
    idempotency_key = Column(String(255), nullable=True, unique=True, index=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Constraint to ensure idempotency key is used when provided
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_idempotency_key"),)

    def __repr__(self) -> str:
        return f"Order(id={self.id}, status={self.status}, customer={self.customer_name})"


class OutboxEvent(Base):
    """Outbox pattern: events awaiting publication to RabbitMQ."""

    __tablename__ = "outbox_events"

    # Primary key (as string UUID for database compatibility)
    id = Column(String(36), primary_key=True, default=_generate_uuid, unique=True, nullable=False)

    # Event details
    event_type = Column(String(100), nullable=False)  # e.g., "order.created"
    order_id = Column(String(36), nullable=False, index=True)  # Reference to Order.id
    payload = Column(Text, nullable=False)  # JSON string

    # Publication status
    published = Column(String(1), nullable=False, default="N")  # "Y" or "N"
    published_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"OutboxEvent(id={self.id}, type={self.event_type}, order_id={self.order_id}, published={self.published})"
