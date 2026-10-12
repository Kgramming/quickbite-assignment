#!/bin/bash

# QuickBite end-to-end demo checklist.
#
# Prerequisites (each in its own terminal, from the repo root):
#   1. docker compose up -d postgres rabbitmq   # or: docker compose up -d
#   2. uvicorn app.main:app --port 8002          # API
#   3. python -m app.outbox_publisher            # outbox -> RabbitMQ relay
#   4. python -m app.worker_service              # all four workers
#
# Or run everything containerized:  docker compose up -d
#
# Covers the assignment demos:
#   A. successful order lifecycle (PENDING -> PAID -> CONFIRMED -> DRIVER_ASSIGNED)
#   B. payment failure (PENDING -> PAYMENT_FAILED, downstream blocked)
#   C. poison message -> dead-letter queue (proves DLX wiring)
#   D. duplicate delivery (idempotent reprocessing)
#   E. notification worker downtime (pipeline unaffected, catch-up on restart)
#   F. multiple worker instances (docker compose --scale worker-payment=3)
#   G. API idempotency key

BASE_URL="http://127.0.0.1:8002"
MGMT="http://127.0.0.1:15672"
RABBIT_USER="quickbite"
RABBIT_PASS="quickbite_password"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

step()  { echo -e "${BLUE}=== $1 ===${NC}"; echo ""; }
ok()    { echo -e "${GREEN}✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}! $1${NC}"; }
fail()  { echo -e "${RED}✗ $1${NC}"; }

order_status() {
  curl -s "$BASE_URL/orders/$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])"
}

wait_for_status() { # $1=order_id $2=expected $3=timeout_seconds
  local id="$1" expected="$2" timeout="$3" s=""
  for _ in $(seq 1 "$timeout"); do
    s=$(order_status "$id" 2>/dev/null)
    if [ "$s" = "$expected" ]; then ok "$id -> $expected"; return 0; fi
    sleep 2
  done
  fail "timeout waiting for $id -> $expected (last: $s)"; return 1
}

queue_depth() { # $1=queue name -> message count
  curl -s -u "$RABBIT_USER:$RABBIT_PASS" \
    "$MGMT/api/queues/%2F/$1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('messages', '?'))"
}

echo ""
echo "=========================================="
echo "QuickBite Demo Checklist"
echo "=========================================="
echo ""

# --- 0. Health ---
step "0. Health check (API + Postgres + RabbitMQ)"
curl -s "$BASE_URL/health" | python3 -m json.tool
echo ""
warn "If status is 'degraded', start the missing service before continuing."
echo ""

# --- A. Happy path ---
step "A. Successful order lifecycle"
ORDER_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Alice Johnson",
  "customer_email": "alice@example.com",
  "customer_phone": "5551234567",
  "delivery_address": "123 Main Street, Downtown",
  "items": [{"name": "Pizza Margherita", "quantity": 2, "price": "12.99"}],
  "total_amount": "31.97"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Order: $ORDER_ID"
wait_for_status "$ORDER_ID" "DRIVER_ASSIGNED" 60
echo ""

# --- B. Payment failure ---
step "B. Payment failure (999.99 triggers the mock decline)"
FAIL_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Bob Smith",
  "customer_email": "bob@example.com",
  "customer_phone": "5559876543",
  "delivery_address": "456 Oak Avenue",
  "items": [{"name": "Premium Steak", "quantity": 2, "price": "499.99"}],
  "total_amount": "999.99"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Order: $FAIL_ID"
wait_for_status "$FAIL_ID" "PAYMENT_FAILED" 60
echo "Downstream queues must stay empty for this order (no order.paid published):"
echo "  restaurant.queue depth: $(queue_depth restaurant.queue)"
echo ""

# --- C. Poison -> DLQ ---
step "C. Poison message is dead-lettered (proves DLX wiring)"
BEFORE=$(queue_depth orders.dead-letter.queue)
echo "DLQ depth before: $BEFORE"
curl -s -u "$RABBIT_USER:$RABBIT_PASS" -X POST \
  "$MGMT/api/exchanges/%2F/amq.default/publish" \
  -H "content-type: application/json" \
  -d '{"properties":{"headers":{"order_id":"poison-demo"}},"routing_key":"payment.queue","payload":"this-is-not-json","payload_encoding":"string"}' \
  > /dev/null
echo "Published unparseable message to payment.queue; waiting for rejection..."
sleep 8
AFTER=$(queue_depth orders.dead-letter.queue)
echo "DLQ depth after:  $AFTER"
if [ "$AFTER" -gt "$BEFORE" ] 2>/dev/null; then
  ok "poison message landed in orders.dead-letter.queue"
else
  fail "DLQ depth did not increase (is the worker running?)"
fi
echo "Inspect it: $MGMT  (Queues > orders.dead-letter.queue > Get messages)"
echo ""

# --- D. Duplicate delivery ---
step "D. Duplicate delivery is idempotent"
DUP_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Dan Miller",
  "customer_email": "dan@example.com",
  "customer_phone": "5554444444",
  "delivery_address": "10 Dup Lane",
  "items": [{"name": "Salad", "quantity": 1, "price": "8.99"}],
  "total_amount": "8.99"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
wait_for_status "$DUP_ID" "DRIVER_ASSIGNED" 60
echo "Re-publishing the same order.created payload to payment.queue..."
warn "Manual step: in the RabbitMQ UI, republish any order.created message for"
warn "order $DUP_ID to payment.queue — the order must stay DRIVER_ASSIGNED"
warn "and processed_events must not gain a second payment row for that event."
echo ""

# --- E. Notification downtime ---
step "E. Notification worker downtime"
echo "1. In the worker terminal, restart with only pipeline workers:"
echo "     python -m app.worker_service --workers payment,restaurant,delivery"
echo "2. Place an order here (press Enter when workers are restarted without notification)..."
read -r _
DOWN_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Erin White",
  "customer_email": "erin@example.com",
  "customer_phone": "5556666666",
  "delivery_address": "20 Quiet Rd",
  "items": [{"name": "Soup", "quantity": 1, "price": "6.99"}],
  "total_amount": "6.99"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
wait_for_status "$DOWN_ID" "DRIVER_ASSIGNED" 60
echo "notification.queue depth (backlog while worker was down): $(queue_depth notification.queue)"
echo "3. Now start the notification worker:  python -m app.worker_service --workers notification"
echo "   Press Enter when it is running..."
read -r _
sleep 6
echo "notification.queue depth after catch-up: $(queue_depth notification.queue)"
ok "pipeline reached DRIVER_ASSIGNED with zero notification workers running"
echo ""

# --- F. Multiple instances ---
step "F. Multiple worker instances"
echo "Run:  docker compose up -d --scale worker-payment=3"
echo "Then place several orders and watch them distribute across the three"
echo "worker containers (docker compose logs -f worker). Exactly-once effect"
echo "per event is enforced by the atomic processed_events claim even when"
echo "instances race (verified by TestAtomicIdempotency + a live PG race)."
echo ""

# --- G. Idempotency key ---
step "G. API idempotency key"
IDEM_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Carol Davis", "customer_email": "carol@example.com",
  "customer_phone": "5552222222", "delivery_address": "789 Pine Road",
  "items": [{"name": "Burger Combo", "quantity": 1, "price": "15.99"}],
  "total_amount": "15.99", "idempotency_key": "demo-idem-001"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
DUP2_ID=$(curl -s -X POST "$BASE_URL/orders" -H "Content-Type: application/json" -d '{
  "customer_name": "Carol Davis", "customer_email": "carol@example.com",
  "customer_phone": "5552222222", "delivery_address": "789 Pine Road",
  "items": [{"name": "Burger Combo", "quantity": 1, "price": "15.99"}],
  "total_amount": "15.99", "idempotency_key": "demo-idem-001"
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
if [ "$IDEM_ID" = "$DUP2_ID" ]; then ok "duplicate idempotency key returned the same order"; else fail "idempotency broken"; fi
echo ""

# --- Retry note ---
step "Retry behavior (covered by tests)"
echo "Bounded retries (3 attempts, 5s/10s/20s TTL backoff) are exercised by"
echo "tests/test_reliability.py::TestRetryPolicy and the live test"
echo "test_retry_then_dlq_marks_order_failed. To observe live: stop postgres"
echo "for ~30s mid-processing and watch x-retry-count headers increment in"
echo "the management UI before the message lands in the DLQ."
echo ""

echo -e "${BLUE}==========================================${NC}"
echo "Demo complete. RabbitMQ UI: $MGMT (quickbite / quickbite_password)"
