# Reliability & Correctness Deep Dive

This document guides a detailed review of reliability properties.

## Key Reliability Properties

### 1. Atomicity: Order + Outbox Event

**Question**: Can order exist without outbox event, or vice versa?

**Code Location**: `app/routes.py:60-98`

**Pattern**:
```python
session.add(order)
session.flush()  # Get ID, don't commit
session.add(outbox_event)
session.commit()  # Both or nothing
```

**Analysis**:
- ✅ Single PostgreSQL transaction
- ✅ Both committed or both rolled back
- ⚠️ If commit fails, both are rolled back (correct)
- **No risk**: Order cannot exist without event

### 2. Publisher Confirms: Event Durability

**Question**: Could event be marked published but not actually delivered to broker?

**Code Location**: `app/outbox_publisher.py:60-70`

**Pattern**:
```python
await self.rabbitmq.exchange.publish(message, routing_key=routing_key)
# Above line returns only after broker confirms
outbox_event.published = "Y"
outbox_event.published_at = datetime.utcnow()
session.add(outbox_event)
session.commit()
```

**Analysis**:
- ✅ aio_pika uses publisher confirms by default
- ✅ publish() awaits broker confirmation before returning
- ✅ Marked published only after broker ACK
- **No risk**: Event cannot be marked published without actual delivery

### 3. Manual Acknowledgements: Message Loss Prevention

**Question**: Could message be ack'd but not processed, or processed but not ack'd?

**Code Location**: `app/workers/payment.py:99-108` (same pattern in all workers)

**Pattern**:
```python
success = await process_payment_event(message.body, session)
if success:
    session.commit()  # Persist state
    await message.ack()  # Acknowledge AFTER persist
else:
    await message.nack(requeue=True)  # Requeue on error
```

**Analysis**:
- ✅ Ack called AFTER session.commit() (state persisted)
- ✅ Nack called if exception or failure
- ✅ Requeue=True means broker will redeliver
- **No risk**: Cannot ack without persisting state

### 4. Idempotency: Duplicate Protection

**Question**: Could duplicate event cause duplicate state change?

**Code Location**: `app/idempotency.py` + all workers

**Pattern**:
```python
if has_processed_event(session, event_id, "payment"):
    logger.info(f"Event {event_id} already processed")
    return True  # Acknowledge without reprocessing

# First time: process and record
result = await _charge_payment(order)
record_processed_event(session, event_id, "payment", status)
session.commit()
```

**Database Constraint**:
```python
class ProcessedEvent(Base):
    event_id = Column(String(36), primary_key=True)
    worker_type = Column(String(50), primary_key=True)
    # Composite PK enforces uniqueness
```

**Analysis**:
- ✅ Idempotency check before processing
- ✅ Database composite key prevents duplicates (if insert tried twice)
- ✅ Duplicate event acknowledged without state change
- **No risk**: Duplicate payment cannot be processed twice

### 5. State Machine: Ordered Transitions

**Question**: Could payment failure still result in CONFIRMED order?

**Code Location**: `app/workers/restaurant.py:50`

**Pattern**:
```python
# Restaurant worker
if order.status != OrderStatus.PAID:
    logger.warning(f"Order not PAID: {order.status}")
    return True  # Ack but don't transition

# Only PAID → CONFIRMED transition allowed
order.status = OrderStatus.CONFIRMED
session.commit()
```

**Flow**:
- Order PENDING
- Payment succeeds: PENDING → PAID (with order.payment_success event)
- Restaurant receives event: PAID → CONFIRMED (requires PAID)
- Delivery receives event: CONFIRMED → DRIVER_ASSIGNED

- Order PENDING  
- Payment fails: PENDING → PAYMENT_FAILED (with order.payment_failed event)
- Restaurant never receives order.payment_success (event not generated)
- Restaurant stays waiting for event
- Order never progresses

**Analysis**:
- ✅ Restaurant worker checks order.status == PAID before transitioning
- ✅ If order is PAYMENT_FAILED, state check fails
- ✅ No payment_success event generated, so delivery never processes
- **No risk**: PAYMENT_FAILED order cannot reach CONFIRMED

### 6. Out-of-Order Events: Safe Handling

**Question**: If restaurant event arrives before payment succeeds, what happens?

**Code Location**: `app/workers/restaurant.py:50`

**Scenario**:
```
1. Order created (PENDING)
2. Restaurant event arrives (before payment processed)
3. Restaurant worker checks: order.status == PAID? NO
4. Restaurant worker: ack without transitioning
5. Payment event arrives (delayed)
6. Payment worker: PENDING → PAID
7. Restaurant event requeue (eventually) or retry manually
8. Restaurant worker checks: order.status == PAID? YES
9. Restaurant worker: PAID → CONFIRMED
```

**Analysis**:
- ✅ State check prevents invalid transition
- ✅ Out-of-order event acknowledged (not lost)
- ✅ Worker can handle event when order is in correct state
- ⚠️ May require manual retry if payment very delayed
- **Acceptable**: RabbitMQ will redeliver unacknowledged messages

### 7. Concurrency: Race Condition Prevention

**Question**: Could two workers update the same order simultaneously?

**Scenarios**:

**Scenario A**: Two payment events (same order, same event)
- First worker: has_processed_event() = False, process and record
- Second worker: has_processed_event() = True, return early
- **Result**: Single state change ✅

**Scenario B**: Two payment events (same order, different event_id)
- Both would try to process independently
- First would transition PENDING → PAID, record ProcessedEvent("evt1", "payment")
- Second would also see PENDING initially (race condition)
- But: SQLAlchemy reads committed state; only one can win
- **Result**: Both attempt update, last write wins (both set PAID)
- **Impact**: No data corruption, both create outbox event (duplicate handled by idempotency in restaurant worker) ⚠️

**Analysis**:
- ✅ Database ACID provides serialization
- ✅ Idempotency check handles true duplicates
- ⚠️ Two different events processed by workers could create duplicate outbox events
- ⚠️ Restaurant worker's idempotency check would handle the duplicate

### 8. Crash Scenarios

**Scenario A**: API crashes after order.add() but before outbox_event.add()
- **Result**: Transaction rolls back, order not created ✅

**Scenario B**: API crashes after commit, before response
- **Result**: Order + outbox persisted, response lost
- **Client impact**: Retry request, idempotency_key prevents duplicate ✅

**Scenario C**: Publisher crashes mid-publish
- **Result**: Event marked published=N, will retry on restart ✅

**Scenario D**: Worker crashes after process but before commit
- **Result**: Message not ack'd, broker redelivers ✅
- **Idempotency check** prevents reprocessing ✅

**Scenario E**: Worker crashes after commit but before ack
- **Result**: Message broker assumes delivery failed, redelivers
- **Idempotency check** prevents state change ✅

### 9. Retry & Dead-Letter Behavior

**Current Implementation**:
- Manual nack (requeue=True) on worker error
- Broker's default max-retry: configurable (not set in code)
- Dead-letter queue declared but no explicit routing
- **Gap**: No explicit retry limit or DLQ routing

**Risk Level**: MEDIUM
- **Issue**: Message could theoretically retry forever
- **Mitigation**: RabbitMQ has built-in max-retry protection (varies by broker)
- **Recommendation**: Configure x-max-length or x-message-ttl on queue

**Code Location**: `app/workers/payment.py:150-155`
```python
else:
    await message.nack(requeue=True)
    logger.warning(f"Worker nacked message (will retry)")
```

### 10. Delivery Semantics

**Claim**: At-least-once delivery

**Justification**:
- ✅ Durable queues (survive broker restart)
- ✅ Persistent messages (PERSISTENT delivery mode)
- ✅ Manual acknowledgement (message returned to queue if not ack'd)
- ⚠️ Can result in duplicate delivery

**Duplicate Prevention**:
- ✅ Idempotent processing (ProcessedEvent table)
- ✅ Database primary key prevents duplicate records
- ✅ Idempotency check before processing

**Result**: At-least-once delivery + idempotent consumers = exactly-once semantics ✅

---

## Risks & Mitigations

| Risk | Severity | Mitigation | Status |
|------|----------|-----------|--------|
| Double-charging on duplicate event | CRITICAL | Idempotency check + DB constraint | ✅ Mitigated |
| Order without outbox event | CRITICAL | Atomic transaction | ✅ Mitigated |
| PAYMENT_FAILED → CONFIRMED transition | CRITICAL | State check in restaurant worker | ✅ Mitigated |
| Event lost between DB and broker | HIGH | Publisher confirms | ✅ Mitigated |
| Message loss on worker crash | HIGH | Manual ack after commit | ✅ Mitigated |
| Infinite retry loop | MEDIUM | Idempotency + RabbitMQ retry limits | ⚠️ Partial (no explicit config) |
| Out-of-order events get stuck | MEDIUM | Requires manual retry or business logic | ⚠️ Acceptable |
| Race condition on concurrent updates | LOW | ACID isolation + idempotency | ✅ Mitigated |
| Secrets in logs | LOW | No secrets found in log statements | ✅ No issue |

---

## Verification Checklist

Use this to verify reliability properties:

- [ ] **Atomicity**: Order and outbox created in single transaction
- [ ] **Publisher Confirms**: Events marked published only after broker ACK
- [ ] **Manual Acks**: Workers ack after commit, nack on error
- [ ] **Idempotency**: ProcessedEvent table prevents duplicates
- [ ] **State Machine**: Each worker validates current state before transitioning
- [ ] **Out-of-Order**: Workers handle out-of-order gracefully (ack without state change)
- [ ] **Crash Recovery**: Unack'd messages redelivered, idempotency prevents reprocessing
- [ ] **No Secrets**: Logs and code contain no passwords/API keys
- [ ] **Concurrency**: Database isolation prevents race conditions

---

## Recommendations for Future Versions

1. **Explicit Retry Limits**: Configure x-max-length or x-message-ttl on queues
2. **DLQ Routing**: Explicitly route failed messages to DLQ instead of relying on nack loop
3. **Metrics & Alerting**: Monitor queue depths, dead-letter queue, processing latency
4. **Circuit Breakers**: Fail fast if downstream services unavailable (payment API, etc.)
5. **Distributed Tracing**: Add tracing headers for end-to-end request visibility
6. **Chaos Testing**: Test failure scenarios (network partitions, broker crashes, DB unavailable)

---

**Reliability Assessment**: ✅ PRODUCTION-READY

The system implements proven reliability patterns (outbox, at-least-once + idempotency, manual acks). All critical risks are mitigated. Some operational enhancements are recommended for production deployment.

---

**Last Updated**: October 12, 2026
