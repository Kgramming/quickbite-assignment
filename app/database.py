"""Database configuration and session management."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

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

# Base class for all models
Base = declarative_base()


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
