# System Architecture

[This document is included in source_code/ARCHITECTURE.md — see that file for the complete architecture overview, including:
- System diagram (Mermaid)
- Component responsibilities
- Message flows
- Design decisions
- Reliability guarantees
]

**Quick Summary**:

- **API**: FastAPI on port 8002, three endpoints (health, POST /orders, GET /orders)
- **Database**: PostgreSQL 16, three tables (orders, outbox_events, processed_events)
- **Messaging**: RabbitMQ 3.12 with topic exchange `quickbite.orders`
- **Topology**: 4 durable queues + 1 dead-letter queue
- **Order Flow**: PENDING → PAID → CONFIRMED → DRIVER_ASSIGNED
- **Failure Mode**: PAYMENT_FAILED → (stops)
- **Workers**: 4 independent consumers (payment, restaurant, delivery, notification)
- **Reliability Pattern**: Outbox + at-least-once delivery + idempotent consumers

**File References**:
- Exchange declaration: `app/rabbitmq.py:35-40`
- Queue creation: `app/rabbitmq.py:declare_queue()`
- Event publishing: `app/outbox_publisher.py:publish_events()`
- Workers: `app/workers/*.py`
- Data models: `app/models.py`
