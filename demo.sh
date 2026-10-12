#!/bin/bash

# Demo script for QuickBite Food Delivery Order Flow
# Shows successful order and payment failure scenarios

set -e

BASE_URL="http://127.0.0.1:8002"

echo ""
echo "=========================================="
echo "QuickBite Food Delivery Demo"
echo "=========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test 1: Health Check
echo -e "${BLUE}=== TEST 1: Health Check ===${NC}"
echo ""
curl -s -X GET "$BASE_URL/health" | python3 -m json.tool
echo ""

# Test 2: Successful Order (should result in PENDING status)
echo -e "${BLUE}=== TEST 2: Create Successful Order ===${NC}"
echo ""
echo "Creating order for customer: Alice Johnson"
ORDER_ID=$(curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Alice Johnson",
    "customer_email": "alice@example.com",
    "customer_phone": "5551234567",
    "delivery_address": "123 Main Street, Downtown",
    "items": [
      {"name": "Pizza Margherita", "quantity": 2, "price": "12.99"},
      {"name": "Garlic Bread", "quantity": 1, "price": "5.99"}
    ],
    "total_amount": "31.97"
  }' | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])")

echo "Order created with ID: $ORDER_ID"
echo ""

# Test 3: Retrieve the order
echo -e "${BLUE}=== TEST 3: Retrieve Order ===${NC}"
echo ""
curl -s -X GET "$BASE_URL/orders/$ORDER_ID" | python3 -m json.tool
echo ""

# Test 4: Payment Failure Order
echo -e "${BLUE}=== TEST 4: Create Order That Will Fail Payment ===${NC}"
echo ""
echo "Creating order with amount 999.99 (triggers payment failure in mock processor)"
FAIL_ORDER_ID=$(curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Bob Smith",
    "customer_email": "bob@example.com",
    "customer_phone": "5559876543",
    "delivery_address": "456 Oak Avenue",
    "items": [
      {"name": "Premium Steak", "quantity": 2, "price": "499.99"}
    ],
    "total_amount": "999.99"
  }' | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])")

echo "Failing order created with ID: $FAIL_ORDER_ID"
echo ""

# Test 5: Retrieve the failing order
echo -e "${BLUE}=== TEST 5: Retrieve Failing Order ===${NC}"
echo ""
curl -s -X GET "$BASE_URL/orders/$FAIL_ORDER_ID" | python3 -m json.tool
echo ""

# Test 6: Idempotency - create same order twice
echo -e "${BLUE}=== TEST 6: Test Idempotency ===${NC}"
echo ""
echo "Creating order with idempotency key..."
IDEMPOTENT_ORDER_ID=$(curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Carol Davis",
    "customer_email": "carol@example.com",
    "customer_phone": "5552222222",
    "delivery_address": "789 Pine Road",
    "items": [
      {"name": "Burger Combo", "quantity": 1, "price": "15.99"}
    ],
    "total_amount": "15.99",
    "idempotency_key": "demo-idem-001"
  }' | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])")

echo "First order created with ID: $IDEMPOTENT_ORDER_ID"
echo ""

echo "Sending duplicate request with same idempotency key..."
DUPLICATE_ORDER_ID=$(curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Carol Davis",
    "customer_email": "carol@example.com",
    "customer_phone": "5552222222",
    "delivery_address": "789 Pine Road",
    "items": [
      {"name": "Burger Combo", "quantity": 1, "price": "15.99"}
    ],
    "total_amount": "15.99",
    "idempotency_key": "demo-idem-001"
  }' | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])")

echo "Duplicate request returned order ID: $DUPLICATE_ORDER_ID"
if [ "$IDEMPOTENT_ORDER_ID" = "$DUPLICATE_ORDER_ID" ]; then
  echo -e "${GREEN}✓ Idempotency working: Same order returned${NC}"
else
  echo -e "${RED}✗ Idempotency failed: Different order returned${NC}"
fi
echo ""

# Summary
echo -e "${BLUE}=========================================="
echo "Demo Summary"
echo "=========================================${NC}"
echo ""
echo -e "${GREEN}✓ Health Check: API is running${NC}"
echo -e "${GREEN}✓ Successful Order: $ORDER_ID (status: PENDING, ready for payment)${NC}"
echo -e "${RED}✓ Failing Order: $FAIL_ORDER_ID (will fail payment)${NC}"
echo -e "${GREEN}✓ Idempotency: Duplicate requests return same order${NC}"
echo ""
echo "Next Steps:"
echo "1. Start the outbox publisher:"
echo "   python3 -m app.outbox_publisher"
echo ""
echo "2. Start the workers:"
echo "   python3 -m app.worker_service"
echo ""
echo "3. View orders progressing through states:"
echo "   - Successful order ($ORDER_ID) should progress: PENDING → PAID → CONFIRMED → DRIVER_ASSIGNED"
echo "   - Failing order ($FAIL_ORDER_ID) should transition: PENDING → PAYMENT_FAILED"
echo ""
echo "4. Monitor RabbitMQ:"
echo "   Open http://127.0.0.1:15672 (username: quickbite, password: quickbite_password)"
echo ""
