"""FastAPI application entry point."""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.config import settings
from app.database import close_db, init_db
from app.routes import router

# Configure logging
logging.basicConfig(level=settings.log_level.upper())
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="QuickBite Food Delivery API",
    description="Asynchronous backend for food delivery orders with RabbitMQ messaging",
    version=__version__,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router)


@app.on_event("startup")
def startup():
    """Initialize database on app startup."""
    logger.info(f"Starting QuickBite API v{__version__}")
    logger.info(f"Environment: {settings.environment}")
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


@app.on_event("shutdown")
def shutdown():
    """Close database connection on app shutdown."""
    logger.info("Shutting down QuickBite API")
    close_db()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level,
    )
