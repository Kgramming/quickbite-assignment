"""Idempotency tracking for duplicate event prevention."""

from datetime import datetime
from enum import Enum
from uuid import UUID

from sqlalchemy import Column, DateTime, Enum as SQLEnum, String
from sqlalchemy.orm import Session

from app.database import Base


class ProcessedEventStatus(str, Enum):
    """Status of a processed event."""

    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class ProcessedEvent(Base):
    """Track processed events for idempotency and audit."""

    __tablename__ = "processed_events"

    # Composite key: event_id + worker_type
    event_id = Column(String(36), primary_key=True)  # UUID string
    worker_type = Column(String(50), primary_key=True)  # e.g., "payment", "restaurant"

    # Processing status
    status = Column(SQLEnum(ProcessedEventStatus), nullable=False, default=ProcessedEventStatus.SUCCESS)
    result = Column(String(500), nullable=True)  # Details or error message

    # Timestamps
    processed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"ProcessedEvent(event_id={self.event_id}, worker={self.worker_type}, status={self.status})"


def has_processed_event(session: Session, event_id: str, worker_type: str) -> bool:
    """
    Check if an event has already been processed by a worker (idempotency check).

    Args:
        session: Database session
        event_id: UUID of the event
        worker_type: Name of the worker type

    Returns:
        True if event was previously processed, False otherwise
    """
    event = session.query(ProcessedEvent).filter(
        ProcessedEvent.event_id == event_id,
        ProcessedEvent.worker_type == worker_type,
    ).first()

    return event is not None


def record_processed_event(
    session: Session,
    event_id: str,
    worker_type: str,
    status: ProcessedEventStatus = ProcessedEventStatus.SUCCESS,
    result: str = None,
):
    """
    Record that an event was processed by a worker.

    Args:
        session: Database session
        event_id: UUID of the event
        worker_type: Name of the worker type
        status: Whether processing succeeded or failed
        result: Optional details about the processing result
    """
    processed = ProcessedEvent(
        event_id=event_id,
        worker_type=worker_type,
        status=status,
        result=result,
    )
    session.add(processed)
    session.commit()


def claim_event(session: Session, event_id: str, worker_type: str) -> bool:
    """Atomically claim an event for processing.

    Uses a single ``INSERT ... ON CONFLICT DO NOTHING`` so the
    check-and-record is one atomic statement: two worker instances (or a
    redelivered message racing a retry) cannot both claim the same
    (event_id, worker_type). Must be called inside the worker's database
    transaction, before any business writes.

    Args:
        session: Database session (with an open transaction)
        event_id: UUID of the event
        worker_type: Name of the worker type

    Returns:
        True if this call claimed the event (caller should process it),
        False if it was already claimed (duplicate -> skip processing).
    """
    bind = session.get_bind()
    dialect = bind.dialect.name if bind is not None else "postgresql"
    values = {
        "event_id": event_id,
        "worker_type": worker_type,
        "status": ProcessedEventStatus.SUCCESS,
    }
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = (
            pg_insert(ProcessedEvent)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["event_id", "worker_type"])
            .returning(ProcessedEvent.event_id)
        )
    elif dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as lite_insert

        stmt = (
            lite_insert(ProcessedEvent)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["event_id", "worker_type"])
            .returning(ProcessedEvent.event_id)
        )
    else:  # pragma: no cover - defensive fallback for other dialects
        from sqlalchemy import insert as generic_insert

        stmt = generic_insert(ProcessedEvent).values(**values)

    row = session.execute(stmt).fetchone()
    # fetchone() returns a row only when the INSERT actually inserted.
    return row is not None
