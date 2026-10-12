"""FastAPI routes for QuickBite orders API."""

import asyncio
import json
import uuid
from datetime import datetime
from uuid import UUID

import aio_pika
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_session
from app.models import Order, OrderStatus, OutboxEvent
from app.schemas import CreateOrderRequest, HealthResponse, OrderResponse

router = APIRouter()


async def _check_rabbitmq(timeout_seconds: float = 3.0) -> str:
    """Verify RabbitMQ is reachable via an AMQP handshake.

    Scope: TCP + AMQP handshake + authentication only. It does not verify
    queue depths, consumer counts, or topology. A separate operational
    check (management API / rabbitmq-diagnostics) covers those.
    """
    rabbitmq_url = (
        f"amqp://{settings.rabbitmq_user}:{settings.rabbitmq_password}"
        f"@{settings.rabbitmq_host}:{settings.rabbitmq_port}/{settings.rabbitmq_vhost}"
    )
    try:
        connection = await asyncio.wait_for(
            aio_pika.connect_robust(rabbitmq_url), timeout=timeout_seconds
        )
        await connection.close()
        return "healthy"
    except Exception as e:
        return f"unhealthy: {str(e)[:80]}"


@router.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check(session: Session = Depends(get_session)):
    """Health check endpoint verifying database and RabbitMQ connectivity.

    Returns overall ``status`` = "ok" when every dependency is healthy,
    otherwise "degraded" (HTTP 200 in both cases; per-dependency fields
    carry the detail). Scope: DB ping + AMQP handshake only.
    """
    try:
        # Simple query to verify database connectivity
        result = session.execute(select(1))
        result.fetchone()
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)[:50]}"

    rabbit_status = await _check_rabbitmq()

    overall = (
        "ok"
        if db_status == "healthy" and rabbit_status == "healthy"
        else "degraded"
    )

    return HealthResponse(
        status=overall,
        database=db_status,
        rabbitmq=rabbit_status,
        version="0.1.0",
        timestamp=datetime.utcnow(),
    )


@router.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED, tags=["orders"])
def create_order(
    request: CreateOrderRequest,
    session: Session = Depends(get_session),
):
    """
    Create a new order with all details and generate an outbox event.
    
    Implements the outbox pattern:
    - Order and OutboxEvent are created in a single transaction.
    - Publication to RabbitMQ will be handled by a separate outbox publisher (Phase 2).
    
    Supports idempotency: if idempotency_key is provided and already exists, returns existing order.
    """
    # Check idempotency: if key provided, try to find existing order
    if request.idempotency_key:
        stmt = select(Order).where(Order.idempotency_key == request.idempotency_key)
        existing_order = session.execute(stmt).scalar_one_or_none()
        if existing_order:
            return OrderResponse.model_validate(existing_order)

    # Create new order with explicit ID
    order_id = str(uuid.uuid4())
    order = Order(
        id=order_id,
        customer_name=request.customer_name,
        customer_email=request.customer_email,
        customer_phone=request.customer_phone,
        delivery_address=request.delivery_address,
        items=json.dumps([item.model_dump() for item in request.items]),
        status=OrderStatus.PENDING,
        total_amount=request.total_amount,
        payment_status="pending",
        idempotency_key=request.idempotency_key,
    )

    session.add(order)
    session.flush()  # Flush to get the ID without committing

    # Create outbox event for order.created with event_id
    event_id = str(uuid.uuid4())
    outbox_event = OutboxEvent(
        id=event_id,
        event_type="order.created",
        order_id=order.id,
        payload=json.dumps({
            "event_id": event_id,
            "event_type": "order.created",
            "order_id": order.id,
            "customer_name": order.customer_name,
            "customer_email": order.customer_email,
            "customer_phone": order.customer_phone,
            "delivery_address": order.delivery_address,
            "items": json.loads(order.items),
            "total_amount": order.total_amount,
            "status": order.status.value,
        }),
    )

    session.add(outbox_event)

    # Commit both order and outbox event in a single transaction
    session.commit()
    session.refresh(order)

    return OrderResponse.model_validate(order)


@router.get("/orders/{order_id}", response_model=OrderResponse, tags=["orders"])
def get_order(
    order_id: UUID,
    session: Session = Depends(get_session),
):
    """Retrieve order details by ID."""
    # Convert UUID to string for database query
    order_id_str = str(order_id)
    stmt = select(Order).where(Order.id == order_id_str)
    order = session.execute(stmt).scalar_one_or_none()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order {order_id} not found",
        )

    return OrderResponse.model_validate(order)
