"""Database configuration and session management."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

# Base class for all models (defined before model imports)
Base = declarative_base()

# Create sync engine (psycopg supports sync only)
engine = create_engine(
    settings.database_url,
    echo=settings.database_echo,
    future=True,
)

# Create sync session factory
SessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    future=True,
)


# Import all models to register them with Base
from app.idempotency import ProcessedEvent  # noqa: F401, E402
from app.models import Order, OrderStatus, OutboxEvent  # noqa: F401, E402


def get_session():
    """Dependency to get database session for FastAPI routes."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def init_db():
    """Initialize database by creating all tables."""
    Base.metadata.create_all(bind=engine)


def close_db():
    """Close database connection."""
    engine.dispose()
