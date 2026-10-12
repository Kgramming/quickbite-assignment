"""Pydantic request/response schemas."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class OrderItemSchema(BaseModel):
    """Order item in request."""

    name: str = Field(..., min_length=1, max_length=255)
    quantity: int = Field(..., gt=0)
    price: str = Field(..., pattern=r"^\d+\.\d{2}$")  # Format: "9.99"


class CreateOrderRequest(BaseModel):
    """Request schema for creating a new order."""

    customer_name: str = Field(..., min_length=1, max_length=255)
    customer_email: EmailStr
    customer_phone: str = Field(..., min_length=10, max_length=20)
    delivery_address: str = Field(..., min_length=5, max_length=1000)
    items: list[OrderItemSchema] = Field(..., min_items=1, max_items=100)
    total_amount: str = Field(..., pattern=r"^\d+\.\d{2}$")  # Format: "99.99"
    idempotency_key: Optional[str] = Field(None, max_length=255)


class OrderResponse(BaseModel):
    """Response schema for order retrieval and creation."""

    id: UUID
    customer_name: str
    customer_email: str
    customer_phone: str
    delivery_address: str
    items: str  # JSON string in response
    status: str
    total_amount: str
    payment_status: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    database: str
    rabbitmq: str
    version: str
    timestamp: datetime
