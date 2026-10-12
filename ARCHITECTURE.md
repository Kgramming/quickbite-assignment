# QuickBite Architecture Document

## System Overview

QuickBite is a distributed food delivery backend implementing the outbox pattern with asynchronous event processing. The system separates concerns across multiple independent services communicating via RabbitMQ.

```
┌─────────────────────────────────────────────────────────────────────┐
│                          QuickBite System                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────┐                                           │
│  │  FastAPI Server      │                                           │
│  │  (port 8002)         │                                           │
│  │  - POST /orders      │ ────────┬─────────────────────────────┐   │
│  │  - GET /orders/{id}  │         │                             │   │
│  │  - GET /health       │         │                             │   │
│  └──────────────────────┘         │                             │   │
│           │                       │                             │   │
│           │ (Single Transaction)  │                             │   │
│           ▼                       ▼                             ▼   │
│  ┌──────────────────────────────────────┐    ┌─────────────────────┐│
│  │      PostgreSQL Database             │    │  RabbitMQ (Phase 2) ││
│  ├──────────────────────────────────────┤    ├─────────────────────┤│
│  │ Table: orders                        │    │ Exchange: quickbite    ││
│  │  - id (UUID, PK)                     │    │ .orders (topic)        ││
│  │  - customer_* (name, email, phone)   │    │                        ││
│  │  - delivery_address                  │    │ Queues:                ││
│  │  - items (JSON)                      │    │ - payment.queue        ││
│  │  - status (ENUM)                     │    │ - restaurant.queue     ││
│  │  - total_amount                      │    │ - delivery.queue       ││
│  │  - payment_status                    │    │ - notification.queue   ││
│  │  - created/updated_at                │    │ - orders.dlq           ││
│  │                                      │    │                        ││
│  │ Table: outbox_events                 │    │ Topics:                ││
│  │  - id (UUID, PK)                     │    │ - order.created        ││
│  │  - event_type                        │    │ - order.paid           ││
│  │  - order_id (FK)                     │    │ - order.confirmed      ││
│  │  - payload (JSON)                    │    │ - order.driver.assigned││
│  │  - published (Y/N)                   │    │ - order.payment.failed ││
│  │  - published_at                      │    │                        ││
│  │  - created_at                        │    └────────────────────────┘│
│  └──────────────────────────────────────┘                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

Phase 2 (Workers) - Independent Consumers
┌──────────────────┬──────────────────┬─────────────────┬──────────────────┐
│ Payment Worker   │ Restaurant Worker│ Delivery Worker │ Notification Wkr │
│ (consumes from   │ (consumes from   │ (consumes from  │ (consumes from   │
│  payment.queue)  │ restaurant.q)    │  delivery.q)    │ notification.q)  │
│                  │                  │                 │                  │
│ Subscribes to:   │ Subscribes to:   │ Subscribes to:  │ Subscribes to:   │
│ - order.created  │ - order.paid     │ - order.confirmed│ - order.*        │
│                  │                  │                 │                  │
│ Updates order:   │ Updates order:   │ Updates order:  │ Logs events,     │
│ PENDING -> PAID  │ PAID -> CONFIRMED│ CONFIRMED ->    │ sends emails/SMS │
│ or PAYMENT_FAIL  │                  │ DRIVER_ASSIGNED │                  │
└──────────────────┴──────────────────┴─────────────────┴──────────────────┘
```

## Design Decisions

### 1. Outbox Pattern for Reliable Messaging

**Problem**: If the API server creates an order and then publishes to RabbitMQ, a crash between these operations could orphan the order in the database with no downstream processing.

**Solution**: Use the outbox pattern:
- Order and OutboxEvent are written to PostgreSQL in a **single ACID transaction**
- A separate outbox publisher worker (Phase 2) polls for unpublished events
- Events are published to RabbitMQ only after successful database commit
- If the publisher crashes mid-publish, it retries when restarted
- This guarantees at-least-once delivery with no lost orders

### 2. Async/Await with SQLAlchemy 2.x

**Decision**: Use `sqlalchemy.ext.asyncio` with `asyncpg` driver
**Rationale**:
- Supports high concurrency on FastAPI's async I/O model
- Modern Pydantic v2 integrates seamlessly
- asyncpg is one of the fastest PostgreSQL drivers
- Allows scaling beyond single-threaded limitations

### 3. Manual RabbitMQ Acknowledgements

**Decision**: Consumers explicitly acknowledge messages after processing
**Rationale**:
- Prevents message loss if worker crashes mid-processing
- Paired with durable queues and exchange for durability
- Dead-letter queue captures messages that fail after max retries
- Idempotency checks in consumers prevent duplicate state transitions

### 4. Order Status as Immutable Enum

**Decision**: Status values are constrained to enum
**Rationale**:
- Prevents invalid state transitions (e.g., DRIVER_ASSIGNED -> PENDING)
- Database constraints ensure data integrity
- API validation prevents malformed requests

### 5. Idempotency Keys for Order Creation

**Decision**: Optional `idempotency_key` field in POST /orders
**Rationale**:
- Allows clients to retry failed requests safely
- Returns same order if duplicate request arrives
- Prevents double-charging if payment call is retried

## Data Flow

### Create Order (POST /orders)

```
Client Request
    │
    ▼
FastAPI /orders endpoint
    │
    ├─ Validate request (Pydantic)
    │
    ├─ Check idempotency key (if provided)
    │  └─ Return existing order if found
    │
    ├─ Create Order model with status=PENDING, payment_status="pending"
    │
    ├─ FLUSH (get ID without committing)
    │
    ├─ Create OutboxEvent with event_type="order.created"
    │
    ├─ COMMIT (both Order and OutboxEvent in single transaction)
    │
    └─ Return OrderResponse (201 Created)

PostgreSQL (Single Transaction)
    ├─ INSERT into orders (all details, status=PENDING)
    │
    └─ INSERT into outbox_events (event_type=order.created, published=N)

RabbitMQ (Phase 2 - OutboxPublisher)
    │
    ├─ Poll outbox_events WHERE published='N'
    │
    ├─ Publish to exchange "quickbite.orders" with routing key "order.created"
    │
    └─ UPDATE outbox_events SET published='Y' WHERE id=...
```

### Order Processing (Phase 2 - Workers)

```
Outbox Publisher (runs periodically)
    └─ SELECT * FROM outbox_events WHERE published='N'
       └─ PUBLISH to RabbitMQ
          └─ UPDATE SET published='Y'

RabbitMQ (Message Broker)
    ├─ Routing key: order.created → payment.queue
    ├─ Routing key: order.paid → restaurant.queue
    ├─ Routing key: order.confirmed → delivery.queue
    └─ All events → notification.queue

Payment Worker
    ├─ Consume from payment.queue
    ├─ Process event: charge payment, validate
    ├─ Check order status (idempotency: ensure still PENDING)
    ├─ If success: UPDATE orders SET status=PAID, payment_status='success'
    ├─ If failure: UPDATE orders SET status=PAYMENT_FAILED, payment_status='failed'
    ├─ If failure: DO NOT publish order.paid (stops downstream processing)
    └─ Acknowledge message to RabbitMQ

Restaurant Worker
    ├─ Consume from restaurant.queue
    ├─ Check order status = PAID (idempotency)
    ├─ Confirm order with restaurant system
    ├─ UPDATE orders SET status=CONFIRMED
    └─ Acknowledge message

Delivery Worker
    ├─ Consume from delivery.queue
    ├─ Check order status = CONFIRMED (idempotency)
    ├─ Assign driver
    ├─ UPDATE orders SET status=DRIVER_ASSIGNED
    └─ Acknowledge message

Notification Worker
    ├─ Consume from notification.queue
    ├─ Send email/SMS based on order status
    └─ Acknowledge message
```

## RabbitMQ Configuration Details

### Exchanges (All Durable)
- **quickbite.orders** (topic): business events.
- **quickbite.retry** (fanout): delayed retries. Workers publish failed
  messages here with a per-message TTL; `orders.retry.queue` dead-letters
  them back to `quickbite.orders` with their original routing key.
- **dlx.quickbite.orders** (topic): dead-letter exchange.

### Queues (All Durable)
- **payment.queue** ← `order.created` → triggers payment processing
- **restaurant.queue** ← `order.paid` → triggers restaurant confirmation
- **delivery.queue** ← `order.confirmed` → triggers driver assignment
- **notification.queue** ← `order.#` → receives all order events
- **orders.retry.queue**: holds retry copies until their TTL expires;
  `x-dead-letter-exchange = quickbite.orders` (no plugins required).
- **orders.dead-letter.queue** ← `#` on `dlx.quickbite.orders`:
  receives messages rejected after max retries (or poison messages),
  with their original routing key preserved.

Every worker queue declares `x-dead-letter-exchange: dlx.quickbite.orders`,
so a rejected message can never be silently dropped.

### Message Flow
```
order.created (event_type="order.created")
    └─ Routes to payment.queue → Payment Worker
       └─ On success: Publishes event_type="order.paid"
          └─ Routes to restaurant.queue → Restaurant Worker
             └─ On success: Publishes event_type="order.confirmed"
                └─ Routes to delivery.queue → Delivery Worker
                   └─ On success: Publishes event_type="order.driver.assigned"

order.paid (event_type="order.paid")
    └─ Routes to restaurant.queue AND notification.queue

order.confirmed (event_type="order.confirmed")
    └─ Routes to delivery.queue AND notification.queue

order.driver.assigned (event_type="order.driver.assigned")
    └─ Routes to notification.queue

order.payment.failed (event_type="order.payment.failed")
    └─ If payment fails: Published by Payment Worker
    └─ Routes to notification.queue (notifies customer of failure)
```

### Reliability Mechanics
- **Publisher confirms**: enabled on every connection
  (`connect_robust(..., publisher_confirms=True)`). The outbox publisher
  marks a row `published='Y'` only after the broker confirms receipt;
  a `basic.nack` raises and the row stays `'N'` for the next poll.
- **Manual acknowledgements**: workers never use
  `message.process()`; `settle_message()` acks only after the worker's
  database transaction (idempotency claim + state transition + outbox
  row) has committed — or, on the retry path, after the broker confirms
  the retry copy.
- **Bounded retries**: `x-retry-count` header, max 3 attempts, backoff
  5s → 10s → 20s via per-message TTL on the retry loop. Exhaustion (or
  a poison message) → `reject(requeue=False)` → DLX →
  `orders.dead-letter.queue`, and the order is marked `FAILED`
  (never overwriting a terminal state).
- **Idempotency**: `claim_event()` performs a single atomic
  `INSERT ... ON CONFLICT DO NOTHING` on
  `processed_events(event_id, worker_type)` inside the worker's
  transaction — safe across redeliveries and concurrent worker
  instances.
- **Isolation**: each worker runs on its own AMQP channel with
  `prefetch_count=1`, so a channel error in one consumer cannot take
  down the others and prefetch is per worker, not shared.

## Failure Scenarios & Handling

### Scenario 1: API Server Crashes After Creating Order

**What Happens**:
- Order and OutboxEvent are written to PostgreSQL before crash
- Server restarts
- Outbox publisher (Phase 2) finds unpublished events
- Events are published to RabbitMQ
- Processing continues normally

**Guarantee**: Order is not lost.

### Scenario 2: Payment Processing Fails

**What Happens**:
1. Payment Worker receives `order.created` event
2. Attempts to charge payment and fails
3. Updates order: `status=PAYMENT_FAILED, payment_status='failed'`
4. Does NOT publish `order.paid` event
5. Acknowledges message to RabbitMQ

**Guarantee**: Order does not progress beyond PENDING. Restaurant and Delivery workers do not process it.

### Scenario 3: Payment Worker Crashes Mid-Processing

**What Happens**:
1. Payment Worker processes event from payment.queue
2. Updates database with payment success
3. Crashes before acknowledging message to RabbitMQ
4. Message is redelivered to payment.queue
5. New Payment Worker instance processes the same event
6. Idempotency check: order status is already PAID, so no duplicate charge
7. Acknowledges message

**Guarantee**: Message is not lost; duplicate processing is prevented by the atomic idempotency claim (`INSERT ... ON CONFLICT DO NOTHING`).

### Scenario 4: RabbitMQ Connection Fails

**What Happens**:
1. Consumer loses connection mid-processing
2. Message is not acknowledged
3. After 30s timeout (configurable), message returns to queue
4. Another consumer instance picks it up
5. Processing resumes

**Guarantee**: Message is not lost.

### Scenario 5: Poison Pill (Unconvertible Message)

**What Happens**:
1. Consumer receives message it cannot parse
2. Sends message to dead-letter exchange
3. Message lands in orders.dead-letter.queue
4. Operator reviews and decides to discard or retry

**Guarantee**: Bad messages don't block the queue indefinitely.

### Scenario 6: Transient Failure Exhausts Retries

**What Happens**:
1. Worker hits a transient error (e.g. database blip) processing a message
2. `x-retry-count` (0, 1, 2) < max (3): publishes a retry copy to the
   `quickbite.retry` exchange with TTL backoff (5s → 10s → 20s) and acks
   the original only after the broker confirms the copy
3. Each expiry redelivers the message to the originating worker queue
4. On the 4th failure the worker marks the order `FAILED` (unless already
   terminal) and rejects with `requeue=False`
5. The queue's `x-dead-letter-exchange` routes it to
   `orders.dead-letter.queue` with the original routing key preserved

**Guarantee**: Bounded retries (no infinite redelivery loops); every
exhausted message is inspectable in the DLQ; the order reflects the
failure.

## Performance Characteristics

### Latencies (Estimated)
- **POST /orders**: 50-100ms (database write + outbox entry)
- **GET /orders/{id}**: 10-20ms (single row query)
- **GET /health**: 5-10ms (connectivity check)
- **Order Processing Pipeline**: 1-5 seconds end-to-end (payment + confirmation + assignment)

### Concurrency
- **FastAPI**: Async/await allows hundreds of concurrent requests
- **PostgreSQL**: Connection pool (configurable, default 10 connections)
- **RabbitMQ**: Supports thousands of concurrent consumers per queue
- **Workers**: Can be horizontally scaled (multiple instances per worker type)

### Storage
- **PostgreSQL**: ~1 KB per order + outbox entry
- **RabbitMQ**: In-memory queue + disk persistence
- **Estimated**: 1 million orders ≈ 1-2 GB PostgreSQL, 10-20 GB RabbitMQ (depends on retention policy)

## Security Considerations

### Not Implemented in Phase 1
- No authentication/authorization on API endpoints
- No rate limiting
- No input sanitization beyond Pydantic validation
- No encryption in transit (should use TLS in production)
- Secrets in `.env` file (not version-controlled, but world-readable on shared systems)

### Recommendations for Production
1. Add JWT authentication to API endpoints
2. Implement rate limiting per client IP
3. Use TLS for all connections (API, PostgreSQL, RabbitMQ)
4. Store secrets in environment variables or secrets vault (e.g., AWS Secrets Manager)
5. Implement audit logging for all order modifications
6. Add honeypot fields to order requests to detect scrapers

## Database Schema

### orders Table
```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    customer_phone VARCHAR(20) NOT NULL,
    delivery_address TEXT NOT NULL,
    items TEXT NOT NULL,  -- JSON
    status ENUM NOT NULL DEFAULT 'PENDING',
    total_amount VARCHAR(10) NOT NULL,
    payment_status VARCHAR(50),
    idempotency_key VARCHAR(255) UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### outbox_events Table
```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    order_id UUID NOT NULL REFERENCES orders(id),
    payload TEXT NOT NULL,  -- JSON
    published VARCHAR(1) NOT NULL DEFAULT 'N',
    published_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

## Scalability Notes

### Vertical Scaling
- Increase PostgreSQL connection pool
- Increase RabbitMQ memory/CPU
- Add more FastAPI workers (multiple uvicorn processes)

### Horizontal Scaling
- Deploy multiple API instances behind a load balancer
- Deploy multiple worker instances per consumer type
- Use consumer groups (RabbitMQ) to distribute work
- Implement distributed tracing (e.g., OpenTelemetry) for debugging

### Bottlenecks
1. **Database**: Single PostgreSQL instance becomes bottleneck at ~5000 orders/minute
   - Solution: Read replicas, query optimization, archival of old orders
2. **RabbitMQ**: Single broker handles ~100k msgs/sec comfortably
   - Solution: RabbitMQ cluster for high availability
3. **Network**: Latency between services
   - Solution: Deploy all services in same datacenter/VPC

## Future Enhancements

1. **Phase 2**: Implement workers and outbox publisher
2. **Phase 3**: Add monitoring (Prometheus metrics, Grafana dashboards)
3. **Phase 4**: Implement distributed tracing (Jaeger, DataDog)
4. **Phase 5**: Add order cancellation and refunds
5. **Phase 6**: Implement customer notifications (email, SMS, push)
6. **Phase 7**: Add driver app and real-time location tracking
