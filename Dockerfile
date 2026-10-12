# Dockerfile for QuickBite workers (Phase 2)
# This builds a container for running the four independent worker consumers

FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Set environment variables (can be overridden at runtime)
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=info

# Default command: run all four workers (override per service in compose)
CMD ["python", "-m", "app.worker_service"]
