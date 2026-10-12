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
- **Retry Logic**: Bounded retries (max 3, 5s/10s/20s TTL backoff via the `quickbite.retry` loop); exhausted or poison messages are rejected to `orders.dead-letter.queue` and the order is marked `FAILED`
- **Idempotency**: Atomic `INSERT ... ON CONFLICT DO NOTHING` claim per event/worker — safe across redeliveries and concurrent worker instances

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

4. **Start the stack** — either fully containerized:
   ```bash
   docker compose up -d
   # Services: postgres, rabbitmq, api, publisher,
   #           worker-payment, worker-restaurant, worker-delivery, worker-notification
   ```
   or infrastructure-only, then run Python processes manually:
   ```bash
   docker compose up -d postgres rabbitmq
   python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8002   # terminal 1
   python3 -m app.outbox_publisher                                 # terminal 2
   python3 -m app.worker_service                                  # terminal 3
   ```

5. **Wait for services to be healthy**
   ```bash
   docker compose ps
   # Wait until postgres and rabbitmq show "healthy"
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
  "rabbitmq": "healthy",
  "version": "0.1.0",
  "timestamp": "2024-10-12T14:30:00"
}
```
`status` is `"ok"` only when every dependency is healthy, otherwise
`"degraded"`. Scope: database ping + AMQP handshake (it does not check
queue depths — use the RabbitMQ management UI for that).

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
│   ├── routes.py            # API endpoints
│   ├── events.py            # Event types, routing keys, queue config
│   ├── rabbitmq.py          # Connection, topology, DLX, retry loop
│   ├── retry.py             # Bounded-retry policy (max 3, TTL backoff)
│   ├── worker_common.py     # Shared ack/settlement + DLQ helpers
│   ├── idempotency.py       # Atomic processed_events claim
│   ├── outbox_publisher.py  # Outbox -> RabbitMQ relay (confirms)
│   ├── worker_service.py    # Worker runner (--workers subset flag)
│   └── workers/             # payment, restaurant, delivery, notification
├── tests/
│   ├── conftest.py              # Pytest fixtures (SQLite)
│   ├── test_routes.py           # API endpoint tests
│   ├── test_models.py           # Model tests
│   ├── test_schemas.py          # Schema validation tests
│   ├── test_integration.py      # Worker logic vs SQLite (direct calls)
│   ├── test_reliability.py      # Reliability mechanisms (mocked broker)
│   └── test_integration_live.py # Real Postgres+RabbitMQ (marked live)
├── docs/
│   ├── REQUIREMENTS_CHECKLIST.md
│   ├── TESTING.md
│   └── migrations/          # SQL migrations for existing databases
├── docker-compose.yml       # postgres, rabbitmq, api, publisher, 4 workers
├── Dockerfile               # Worker/API image
├── demo.sh                  # End-to-end demo checklist
├── requirements.txt         # Python dependencies
├── .env.example             # Example environment template
├── pytest.ini               # Pytest configuration
├── ARCHITECTURE.md          # Architecture + reliability design
├── TECHNICAL_REPORT.md      # Technical report + fixes addendum
├── CHANGELOG.md             # Change history
└── README.md                # This file
```

## RabbitMQ Configuration (Phase 2)

The following RabbitMQ resources will be created by the outbox publisher and workers:

| Resource | Type | Durable | Notes |
|----------|------|---------|-------|
| `quickbite.orders` | Topic Exchange | Yes | Main exchange for order events |
| `quickbite.retry` | Fanout Exchange | Yes | Delayed retries (per-message TTL) |
| `dlx.quickbite.orders` | Topic Exchange | Yes | Dead-letter exchange |
| `payment.queue` | Queue | Yes | `order.created` → payment processing |
| `restaurant.queue` | Queue | Yes | `order.paid` → restaurant confirmation |
| `delivery.queue` | Queue | Yes | `order.confirmed` → delivery assignment |
| `notification.queue` | Queue | Yes | `order.#` → notifications (email, SMS) |
| `orders.retry.queue` | Queue | Yes | TTL wait; dead-letters back to `quickbite.orders` |
| `orders.dead-letter.queue` | Queue | Yes | Rejected/poison messages, original routing key kept |

Every worker queue declares `x-dead-letter-exchange: dlx.quickbite.orders`.

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

## Known Limitations & Future Work (Phase 3)

1. **Mock Payment Processor**: Production would integrate Stripe, PayPal, or similar
2. **Mock Notifications**: Production would use SendGrid, Twilio, or similar
3. **No Monitoring**: Prometheus, Grafana, or distributed tracing not included
4. **No Encryption**: Messages sent in plaintext (acceptable for test environment)
5. **DLQ Manual Inspection**: Dead-letter queue messages cannot be replayed automatically
6. **Single-Instance Workers**: No coordination between multiple instances

## Phase 2: Worker Execution & Outbox Publishing

### Overview

Phase 2 implements reliable asynchronous messaging with four independent worker processes:

1. **Payment Worker**: Processes payments (PENDING → PAID or PAYMENT_FAILED)
2. **Restaurant Worker**: Confirms orders (PAID → CONFIRMED)
3. **Delivery Worker**: Assigns drivers (CONFIRMED → DRIVER_ASSIGNED)
4. **Notification Worker**: Sends notifications (read-only)

### Running Workers

In separate terminal windows:

```bash
# Terminal 2: Start outbox publisher (DB outbox -> RabbitMQ relay)
python3 -m app.outbox_publisher

# Terminal 3: Start all four workers (each on its own AMQP channel)
python3 -m app.worker_service

# Or run a subset (e.g. for the notification-downtime demo):
python3 -m app.worker_service --workers payment,restaurant,delivery
```

Or run the whole stack containerized (API + publisher + workers):

```bash
docker compose up -d
# Scale workers to 3 instances:
docker compose up -d --scale worker-payment=3
```

### Order Status Flow

```
┌─────────────┐
│   PENDING   │  (order created via API)
└──────┬──────┘
       │ payment.queue
       ▼
┌─────────────────┐
│ PAID or FAILED  │  (payment worker processes)
└────────┬────────┘
         │ (only PAID progresses)
         ▼
┌─────────────────┐
│   CONFIRMED     │  (restaurant worker confirms)
└────────┬────────┘
         │ restaurant.queue
         ▼
┌─────────────────┐
│ DRIVER_ASSIGNED │  (delivery worker assigns)
└─────────────────┘
```

### Reliability Features

- **Durable Queues**: Messages survive broker restarts
- **Persistent Messages**: PERSISTENT delivery mode
- **Publisher Confirms**: Explicitly enabled; outbox events marked published only after broker ACK
- **Idempotent Consumers**: Atomic `INSERT ... ON CONFLICT DO NOTHING` claim per event/worker — safe across redeliveries and concurrent instances
- **Manual Acknowledgements**: Workers ack only after the DB transaction commits (or after the broker confirms the retry copy); no `message.process()` double-settlement
- **Bounded Retries**: Max 3 attempts, 5s → 10s → 20s TTL backoff via the `quickbite.retry` loop — no infinite redelivery
- **Dead-Letter Queue**: `dlx.quickbite.orders` + per-queue `x-dead-letter-exchange`; exhausted/poison messages land in `orders.dead-letter.queue` and the order is marked `FAILED`
- **State Machine Enforcement**: Workers check current status before transitioning
- **Per-worker Channels**: Each consumer gets its own channel with `prefetch_count=1`

### Testing Phase 2

```bash
# Unit + mocked-reliability + SQLite integration tests
pytest tests/ -v
# Live broker/Postgres tests (needs: docker compose up -d postgres rabbitmq)
QUICKBITE_RUN_LIVE=1 pytest -m live -v

# Run only Phase 2 integration tests
pytest tests/test_integration.py -v

# Run specific test
pytest tests/test_integration.py::TestOrderFlow::test_payment_success_flow -v
```

### Demo Script

An interactive demo shows successful and failed orders:

```bash
chmod +x demo.sh
./demo.sh
```

The demo:
1. Creates a successful order and processes it through all workers
2. Creates a failing order (amount 999.99) and shows payment failure
3. Demonstrates idempotency (duplicate events processed once)
4. Verifies order status transitions

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
