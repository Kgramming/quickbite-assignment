# QuickBite Food Delivery - Asynchronous Backend

A FastAPI-based asynchronous backend for a food delivery platform with RabbitMQ message brokers, durable messaging, and independent worker consumers for payment, restaurant, delivery, and notification processing.

## Overview

QuickBite is designed with the following architecture:
- **API Server**: FastAPI running on port 8002
- **Database**: PostgreSQL (port 5434) for persistent order storage
- **Message Broker**: RabbitMQ (port 5672) for asynchronous event processing
- **Outbox Pattern**: Orders and events stored together; separate publisher ensures reliable delivery
- **Workers**: Four independent consumer workers (Payment, Restaurant, Delivery, Notification)

## System Design

### Order Flow
```
PENDING → PAID → CONFIRMED → DRIVER_ASSIGNED
```

### Failure Handling
- **Payment Failure**: Order status transitions to `PAYMENT_FAILED` and does not progress
- **Retry Logic**: Dead-letter queue captures poison pills; configurable retry behavior
- **Idempotency**: All consumers implement idempotent processing with state checks

### Outbox Pattern
The system implements the outbox pattern to guarantee at-least-once delivery of events:
1. When `/orders` POST endpoint is called, the order and an outbox event are persisted in a **single database transaction**
2. A separate outbox publisher worker (Phase 2) polls the outbox table and publishes unpublished events to RabbitMQ
3. Consumers pull events from queues, process them, and acknowledge receipt
4. If a consumer crashes between processing and acknowledgement, RabbitMQ redelivers the message

This design ensures:
- No lost events due to database crashes
- No duplicate processing (consumers check state before transitioning)
- No orphaned orders in pending state

## Quick Start

### Prerequisites
- Python 3.13+
- Docker and Docker Compose
- 550 MB available RAM (minimal deployment)

### Setup

1. **Clone and navigate to the project**
   ```bash
   cd /Users/keval/Documents/quickbite-assignment
   ```

2. **Create and activate Python virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start Docker containers** (PostgreSQL + RabbitMQ)
   ```bash
   docker-compose up -d
   ```

5. **Wait for services to be healthy**
   ```bash
   docker-compose ps
   # Wait until both postgres and rabbitmq show "healthy"
   ```

6. **Run the FastAPI server**
   ```bash
   python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8002
   ```

The API will be available at `http://127.0.0.1:8002`

### Configuration

The application reads environment variables from `.env`. Key settings:

```env
# API Configuration
API_HOST=127.0.0.1
API_PORT=8002
LOG_LEVEL=info

# Database
DATABASE_URL=postgresql+asyncpg://quickbite_user:quickbite_password@127.0.0.1:5434/quickbite_db
DATABASE_ECHO=false

# RabbitMQ
RABBITMQ_HOST=127.0.0.1
RABBITMQ_PORT=5672
RABBITMQ_USER=quickbite
RABBITMQ_PASSWORD=quickbite_password
```

## API Endpoints

### Health Check
```bash
GET /health
```
Response:
```json
{
  "status": "ok",
  "database": "healthy",
  "version": "0.1.0",
  "timestamp": "2024-10-12T14:30:00"
}
```

### Create Order
```bash
POST /orders
Content-Type: application/json

{
  "customer_name": "John Doe",
  "customer_email": "john@example.com",
  "customer_phone": "5551234567",
  "delivery_address": "123 Main St",
  "items": [
    {
      "name": "Pizza Margherita",
      "quantity": 1,
      "price": "12.99"
    },
    {
      "name": "Coca-Cola",
      "quantity": 2,
      "price": "2.99"
    }
  ],
  "total_amount": "18.97",
  "idempotency_key": "optional-unique-key"
}
```

Response (201 Created):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_name": "John Doe",
  "customer_email": "john@example.com",
  "customer_phone": "5551234567",
  "delivery_address": "123 Main St",
  "items": "[{\"name\": \"Pizza Margherita\", \"quantity\": 1, \"price\": \"12.99\"}]",
  "status": "PENDING",
  "total_amount": "18.97",
  "payment_status": "pending",
  "created_at": "2024-10-12T14:30:00",
  "updated_at": "2024-10-12T14:30:00"
}
```

### Get Order
```bash
GET /orders/{order_id}
```

Response (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_name": "John Doe",
  "status": "PENDING",
  ...
}
```

## Testing

Run all tests:
```bash
pytest
```

Run specific test file:
```bash
pytest tests/test_routes.py
```

Run with coverage:
```bash
pytest --cov=app tests/
```

### Test Coverage
- **Schemas**: Validation of all request/response models
- **Routes**: API endpoint behavior, error handling, idempotency
- **Models**: Database model defaults and enumerations
- **Database**: (Integration tests in Phase 2)

## Project Structure

```
quickbite-assignment/
├── app/
│   ├── __init__.py          # Package metadata
│   ├── main.py              # FastAPI app entry point
│   ├── config.py            # Pydantic settings
│   ├── database.py          # SQLAlchemy setup, session management
│   ├── models.py            # Order, OutboxEvent models
│   ├── schemas.py           # Pydantic request/response schemas
│   └── routes.py            # API endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py          # Pytest fixtures
│   ├── test_schemas.py      # Schema validation tests
│   ├── test_routes.py       # API endpoint tests
│   └── test_models.py       # Model tests
├── docker-compose.yml       # PostgreSQL + RabbitMQ services
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (git-ignored in .gitignore)
├── .env.example             # Example environment template
├── pytest.ini               # Pytest configuration
└── README.md                # This file
```

## RabbitMQ Configuration (Phase 2)

The following RabbitMQ resources will be created by the outbox publisher and workers:

| Resource | Type | Durable | Notes |
|----------|------|---------|-------|
| `quickbite.orders` | Topic Exchange | Yes | Main exchange for order events |
| `payment.queue` | Queue | Yes | Payment processing |
| `restaurant.queue` | Queue | Yes | Restaurant confirmation |
| `delivery.queue` | Queue | Yes | Delivery assignment |
| `notification.queue` | Queue | Yes | Notifications (email, SMS) |
| `orders.dead-letter.queue` | Queue | Yes | Poison pills and failures |

## Docker Management

### View container logs
```bash
docker-compose logs -f postgres
docker-compose logs -f rabbitmq
```

### Access RabbitMQ Management UI
Open browser to: `http://127.0.0.1:15672`
- Username: `quickbite`
- Password: `quickbite_password`

### Stop services
```bash
docker-compose down
```

### Stop and remove volumes (data loss!)
```bash
docker-compose down -v
```

## Known Limitations (Phase 1)

1. **Outbox Publisher Not Implemented**: Orders are created with outbox events, but they are not yet published to RabbitMQ. This will be implemented in Phase 2.
2. **Workers Not Implemented**: Payment, Restaurant, Delivery, and Notification consumers are not yet implemented.
3. **No Async Task Runner**: Background job scheduling is not yet implemented.

## What's Next (Phase 2)

1. Implement OutboxPublisher worker that polls the outbox table and publishes events to RabbitMQ
2. Implement four independent consumer workers with manual acknowledgements and idempotent processing
3. Add order status transitions driven by consumer events
4. Implement dead-letter queue handling and retry logic
5. Add comprehensive integration tests
6. Deploy to isolated Docker network with worker scaling support

## Development Notes

- Use `DATABASE_ECHO=true` in `.env` to see SQL queries
- Use `LOG_LEVEL=debug` for verbose logging
- Tests use SQLite in-memory database for isolation (not PostgreSQL)
- API uses async SQLAlchemy with asyncpg driver for PostgreSQL

## Support & Troubleshooting

### Database connection errors
- Ensure `docker-compose up -d` was run and PostgreSQL shows as "healthy"
- Check `DATABASE_URL` in `.env` matches the container configuration
- Verify port 5434 is not blocked

### RabbitMQ connection errors
- Ensure RabbitMQ container is running and healthy
- Verify credentials in `.env` match docker-compose.yml
- Check port 5672 is not blocked

### Port conflicts
- Current ports: API=8002, PostgreSQL=5434, RabbitMQ AMQP=5672, RabbitMQ UI=15672
- If ports are occupied, update docker-compose.yml and .env accordingly

## Author Notes

This implementation prioritizes reliability and separation of concerns:
- Database and event publishing are atomic (outbox pattern)
- Workers are independent and can be scaled separately
- Configuration is externalized to environment variables
- Secrets are not committed to version control
- Tests isolate components and mock infrastructure dependencies
