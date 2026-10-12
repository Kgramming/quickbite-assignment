# Muse AI: Comprehensive Code Review Prompt

You are an expert backend engineer conducting an independent code review of a FastAPI-based asynchronous food delivery system. Your task is to identify bugs, architectural issues, missing requirements, and reliability gaps **without rebuilding the project**.

## Context

**Project**: QuickBite Food Delivery Backend  
**Assignment**: Build a reliable asynchronous order processing system with RabbitMQ messaging  
**Implementation Status**: Code written, 27 tests pass (100%), deployed to GitHub  
**Your Task**: Find defects, missing requirements, and risks that the test suite may have missed

---

## Review Instructions

### Phase 1: Requirements Verification

Read `01_ASSIGNMENT_REQUIREMENTS.md` and verify each requirement against the source code:

1. **API Endpoints** (in `source_code/app/routes.py`)
   - [ ] Does `GET /health` exist and return correct format?
   - [ ] Does `POST /orders` validate input correctly?
   - [ ] Does `POST /orders` create both Order and OutboxEvent atomically?
   - [ ] Does `GET /orders/{order_id}` return 404 for missing orders?
   - [ ] Are all validation errors reported clearly (no info leaks)?

2. **RabbitMQ Topology** (in `source_code/app/rabbitmq.py`)
   - [ ] Is exchange `quickbite.orders` declared as durable topic exchange?
   - [ ] Are all 4 queues durable?
   - [ ] Are bindings correct (payment.queue → order.created, etc.)?
   - [ ] Is dead-letter queue durable?
   - [ ] Is QoS prefetch_count set for fair distribution?

3. **Outbox Publisher** (in `source_code/app/outbox_publisher.py`)
   - [ ] Does it poll `outbox_events` table for unpublished events?
   - [ ] Does it publish with PERSISTENT delivery mode?
   - [ ] Does it mark events published **only after** broker confirmation?
   - [ ] Does it handle errors gracefully (retry on failure)?
   - [ ] Can it be run as `python3 -m app.outbox_publisher`?

4. **Worker Consumers** (in `source_code/app/workers/*.py`)
   - [ ] Do all 4 workers exist (payment, restaurant, delivery, notification)?
   - [ ] Do workers consume from the correct queues?
   - [ ] Do workers acknowledge **only after** database commit?
   - [ ] Do workers nack on error (requeue)?
   - [ ] Is idempotency checked before processing?
   - [ ] Can workers be started independently?

5. **State Machine** (in `source_code/app/models.py` and workers)
   - [ ] Is OrderStatus enum correct (PENDING, PAID, CONFIRMED, DRIVER_ASSIGNED, PAYMENT_FAILED)?
   - [ ] Does payment worker transition PENDING → PAID on success?
   - [ ] Does payment worker transition PENDING → PAYMENT_FAILED on failure?
   - [ ] Does restaurant worker check order.status == PAID before transitioning?
   - [ ] Does delivery worker check order.status == CONFIRMED before transitioning?
   - [ ] Does PAYMENT_FAILED order never progress to CONFIRMED?

6. **Idempotency** (in `source_code/app/idempotency.py` and workers)
   - [ ] Is `ProcessedEvent(event_id, worker_type)` table created with composite key?
   - [ ] Do workers check `has_processed_event()` before processing?
   - [ ] Do duplicate events return True (acknowledged) without re-processing?
   - [ ] Is idempotency enforced by database constraint, not just in-memory check?

---

### Phase 2: Architectural Review

Read `02_ARCHITECTURE.md` and review the actual implementation:

1. **Atomicity** (in `source_code/app/routes.py`, lines 60-98)
   ```python
   # Pattern: single transaction for order + outbox?
   # Does it:
   # - session.add(order)
   # - session.flush()
   # - session.add(outbox_event)
   # - session.commit()
   ```
   - [ ] Is the pattern correct?
   - [ ] Could an order be created without an outbox event?
   - [ ] Could an outbox event exist without an order?

2. **Publisher Confirms** (in `source_code/app/outbox_publisher.py`)
   ```python
   # Question: When is outbox_event.published marked 'Y'?
   # Is it:
   # a) Before publishing to RabbitMQ (WRONG—event could be lost)
   # b) After publishing returns (RISKY—no broker confirmation)
   # c) After broker confirms message (CORRECT)
   ```
   - [ ] Which pattern is used?
   - [ ] Could events be marked published but lost to broker?

3. **Manual Acknowledgements** (in `source_code/app/workers/*.py`)
   ```python
   # Pattern in each worker:
   # if success:
   #   session.commit()
   #   await message.ack()  # Ack AFTER commit
   # else:
   #   await message.nack(requeue=True)  # Nack on error
   ```
   - [ ] Is ack called after or before commit?
   - [ ] Is nack called on exception?
   - [ ] Could a message be ack'd but the order not persisted?

4. **Race Condition Prevention** (in workers)
   ```python
   # Before updating order status, does worker:
   # - Check current status matches expected state?
   # - Hold the order row locked (pessimistic)?
   # - Use optimistic locking (version column)?
   # - Rely on database uniqueness constraint?
   ```
   - [ ] How is concurrency handled?
   - [ ] Could two workers update the same order simultaneously?
   - [ ] What prevents PENDING order from being charged twice?

5. **Out-of-Order Event Handling** (in workers)
   - [ ] If restaurant.queue event arrives before payment succeeds, what happens?
   - [ ] Does the worker check order.status before updating?
   - [ ] Is the message acknowledged or requeued?
   - [ ] Can payment eventually catch up and transition state correctly?

---

### Phase 3: Reliability Review

Use `03_RELIABILITY_REVIEW.md` as your checklist:

1. **Delivery Semantics**
   - [ ] Is this at-least-once or exactly-once delivery?
   - [ ] How are duplicates prevented?
   - [ ] Could a duplicate ever cause incorrect state?

2. **Crash Scenarios**
   - [ ] If API server crashes between order.add() and outbox_event.add(), what happens?
   - [ ] If publisher crashes mid-publish, does the event get republished?
   - [ ] If worker crashes between processing and commit, is the message redelivered?
   - [ ] If database crashes, are durable queues still intact?

3. **Failure Modes**
   - [ ] If payment fails (500 error), does order transition to PAYMENT_FAILED?
   - [ ] If payment fails, is any upstream event generated (no restaurant event)?
   - [ ] If restaurant worker fails permanently, could order get stuck?
   - [ ] If order gets stuck, is there a manual intervention path?

4. **Retry Loops**
   - [ ] Is there any risk of infinite retry loops?
   - [ ] When does a message go to dead-letter queue?
   - [ ] Is there a maximum retry count or timeout?
   - [ ] Can poison pills be manually inspected/resolved?

---

### Phase 4: Code Quality Review

1. **Secrets & Security** (check `source_code/`)
   - [ ] Is `.env` tracked? (Should be NO)
   - [ ] Is `.env.example` present with safe placeholders? (Should be YES)
   - [ ] Are any API keys, passwords, or tokens in code? (Should be NO)
   - [ ] Are database passwords hardcoded? (Should be NO)
   - [ ] Is `.venv` tracked? (Should be NO)

2. **Error Handling** (in routes and workers)
   - [ ] Are all exceptions caught and logged?
   - [ ] Do error messages leak sensitive information?
   - [ ] Are database errors handled gracefully?
   - [ ] Are RabbitMQ connection errors handled gracefully?

3. **Logging** (throughout)
   - [ ] Are success cases logged with clear messages?
   - [ ] Are failures logged with sufficient context (order_id, event_id)?
   - [ ] Are logs at appropriate levels (ERROR, WARNING, INFO)?
   - [ ] Could logs ever contain secrets? (passwords, tokens)

4. **Type Hints** (check function signatures)
   - [ ] Are function parameters type-hinted?
   - [ ] Are return types specified?
   - [ ] Is the code compatible with mypy/pyright static checking?

5. **Dead Code** (scan all files)
   - [ ] Are there any unused imports?
   - [ ] Are there any unused functions?
   - [ ] Are there any placeholder functions or TODO comments?
   - [ ] Are there any debug print statements?

---

### Phase 5: Test Coverage Analysis

Read `04_TEST_EVIDENCE.md` and `docs/TESTING.md`:

1. **Test Types**
   - [ ] How many unit tests? (Expected: ~10)
   - [ ] How many API tests? (Expected: ~7)
   - [ ] How many integration tests? (Expected: ~10)
   - [ ] Do tests use real PostgreSQL or SQLite? (Expected: SQLite in tests, but verified separately with real DB)

2. **Untested Behavior** (identify gaps)
   - [ ] Is real RabbitMQ topology tested? (Expected: Code reviewed + real verification, not automated)
   - [ ] Is real PostgreSQL integration tested? (Expected: Code reviewed + real verification)
   - [ ] Are worker startups tested? (Expected: Code reviewed, not in automated tests)
   - [ ] Is retry/DLQ behavior tested? (Expected: Code logic only, not against real broker)
   - [ ] Are crash scenarios tested? (Expected: NO, untestable)

3. **Test Quality**
   - [ ] Are tests isolated from each other (no data leakage)?
   - [ ] Do tests use fixtures for setup/cleanup?
   - [ ] Are test names descriptive?
   - [ ] Are test assertions clear and specific?
   - [ ] Is test coverage reported?

---

### Phase 6: Documentation Review

Check the following files for accuracy and completeness:

1. **README.md**
   - [ ] Setup instructions are complete and reproducible
   - [ ] API examples are correct
   - [ ] Test commands actually work
   - [ ] Docker Compose commands are correct
   - [ ] Environment setup is clear

2. **ARCHITECTURE.md**
   - [ ] System diagram matches actual implementation
   - [ ] Message flows are accurate
   - [ ] Design decisions are documented
   - [ ] Routing keys are correct

3. **TECHNICAL_REPORT.md**
   - [ ] Implementation details match code
   - [ ] Reliability guarantees are honest (no overclaiming)
   - [ ] Trade-offs are accurately described
   - [ ] Limitations are clearly stated

---

## Specific Code Paths to Audit

### Critical Path 1: Order Creation & Event Publishing

```
1. User POST /orders
   └─ routes.py:create_order()
      ├─ Pydantic validation (schemas.py)
      ├─ Order creation
      ├─ OutboxEvent creation
      └─ session.commit()

2. OutboxPublisher polls (outbox_publisher.py:publish_events())
   ├─ Query unpublished events
   ├─ Exchange.publish()
   └─ Mark published

3. Worker consumes (workers/payment.py:run_payment_worker())
   ├─ Receive message
   ├─ Check idempotency
   ├─ Process payment
   ├─ session.commit()
   └─ message.ack()
```

**Audit**: Can any step fail leaving the system in an inconsistent state?

### Critical Path 2: Payment Failure Blocking Downstream

```
1. Payment fails
   └─ workers/payment.py: order.status = PAYMENT_FAILED
      └─ OutboxEvent(type="order.payment_failed")

2. Restaurant worker waiting on payment.success event
   └─ Event never comes because order.status != PAID
   └─ Restaurant worker doesn't consume order.payment_failed

3. Expected: Delivery never called because no restaurant.confirmed event
```

**Audit**: Is this flow guaranteed? Could order ever reach CONFIRMED if payment failed?

### Critical Path 3: Duplicate Event Idempotency

```
1. Payment event arrives (event_id = "ABC-123")
   └─ Payment worker processes
      ├─ Check has_processed_event(session, "ABC-123", "payment")
      │  └─ First time: False, proceed
      ├─ Update order: PENDING → PAID
      ├─ Record ProcessedEvent(event_id="ABC-123", worker_type="payment")
      └─ message.ack()

2. Duplicate payment event arrives (same event_id = "ABC-123")
   └─ Payment worker processes
      ├─ Check has_processed_event(session, "ABC-123", "payment")
      │  └─ Second time: True, return without processing
      └─ message.ack() (acknowledge without state change)
```

**Audit**: Is the idempotency check guaranteed to work? Could a duplicate ever cause two state changes?

---

## Questions to Answer

Write concise answers to these questions:

1. **Can orders be lost?** (Order created but no outbox event, or vice versa?)
2. **Can orders be double-charged?** (Payment processed twice?)
3. **Can an order get stuck?** (Enter a state where it can't progress?)
4. **Can payments fail but order still reach CONFIRMED?** (State machine violated?)
5. **Are there any race conditions?** (Two workers corrupting same order?)
6. **Is idempotency truly enforced?** (Or could duplicates bypass the check?)
7. **What happens if the publisher crashes?** (Events lost? Republished?)
8. **What happens if a worker crashes?** (Message redelivered? Acknowledged correctly?)
9. **Is there an infinite retry risk?** (Messages looping forever?)
10. **Are all requirements implemented?** (Nothing missing or misleading?)

---

## Severity Levels

When reporting issues, use these severity levels:

- **CRITICAL**: Violates requirements, causes data loss, or breaks the system
- **HIGH**: Risky design, potential for bugs under specific conditions
- **MEDIUM**: Better practices, improves reliability or maintainability
- **LOW**: Code style, non-critical improvements, documentation clarifications

---

## Output Format

Provide your findings in this format:

```markdown
## Issues Found

### Issue 1: [Title]
- **Severity**: CRITICAL | HIGH | MEDIUM | LOW
- **File(s)**: app/workers/payment.py:25
- **Description**: [What's wrong]
- **Evidence**: [Code snippet or execution trace]
- **Fix**: [How to address it]
- **Impact**: [What could go wrong if not fixed]

### Issue 2: ...

## Questions & Clarifications
- [Any unclear design decisions?]

## Positive Findings
- [Well-designed aspects]
- [Good practices]

## Recommendations for Improvement
- [Suggested enhancements, not blockers]
```

---

## Timeline

- **Code frozen at**: e8eb4e7 (October 12, 2026)
- **Tests**: 27/27 passing
- **Review requested for**: Bugs, missing requirements, architectural issues
- **Do not**: Attempt to rebuild, modify, or extend the project—review only

---

## Good Luck!

This is a production-grade codebase. The developers have spent significant effort on reliability, testing, and documentation. Look for subtle issues, edge cases, and architectural concerns that automated tests might miss.

Your independent perspective is valuable—identify both strengths and weaknesses honestly.

---

**Review Package Version**: 1.0  
**For**: Muse AI Independent Code Review  
**Prepared**: October 12, 2026
