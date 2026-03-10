#!/bin/bash
# Delete all products from Puppet Vendors portal
# Usage: bash delete_portal_products.sh "your_connect.sid_value"

COOKIE="connect.sid=$1"
BASE="https://app.puppetvendors.com"
LIMIT=50
OFFSET=0
ALL_IDS=()

echo "=== Fetching all product IDs ==="

while true; do
  RESPONSE=$(curl -s "$BASE/api/portal/v2/products?limit=$LIMIT&offset=$OFFSET" \
    -H "cookie: $COOKIE" \
    -H "accept: application/json")

  # Extract product IDs using python
  IDS=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
products = data.get('products', data.get('data', []))
if isinstance(products, list):
    for p in products:
        pid = p.get('_id', p.get('id', ''))
        if pid:
            print(pid)
" 2>/dev/null)

  if [ -z "$IDS" ]; then
    echo "No more products at offset=$OFFSET"
    break
  fi

  COUNT=0
  while IFS= read -r id; do
    ALL_IDS+=("$id")
    COUNT=$((COUNT + 1))
  done <<< "$IDS"

  echo "  Fetched $COUNT products (offset=$OFFSET)"

  if [ "$COUNT" -lt "$LIMIT" ]; then
    break
  fi

  OFFSET=$((OFFSET + LIMIT))
done

TOTAL=${#ALL_IDS[@]}
echo ""
echo "=== Total products to delete: $TOTAL ==="
echo ""

if [ "$TOTAL" -eq 0 ]; then
  echo "Nothing to delete."
  exit 0
fi

read -p "Proceed with deleting $TOTAL products? (y/N) " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
  echo "Aborted."
  exit 0
fi

# Delete in batches of 50
BATCH=50
for ((i=0; i<TOTAL; i+=BATCH)); do
  BATCH_IDS=("${ALL_IDS[@]:i:BATCH}")
  BATCH_NUM=$(( (i / BATCH) + 1 ))
  BATCH_END=$((i + ${#BATCH_IDS[@]}))

  # Build JSON array
  JSON_ARRAY=$(printf '"%s",' "${BATCH_IDS[@]}")
  JSON_ARRAY="[${JSON_ARRAY%,}]"

  echo "Deleting batch $BATCH_NUM ($((i+1))-$BATCH_END of $TOTAL)..."

  RESULT=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$BASE/api/portal/bulk-action" \
    -H "cookie: $COOKIE" \
    -H "content-type: application/json" \
    -H "accept: application/json" \
    -d "{\"request\":\"productDelete\",\"products\":$JSON_ARRAY}")

  if [ "$RESULT" = "200" ]; then
    echo "  OK (HTTP $RESULT)"
  else
    echo "  ERROR (HTTP $RESULT)"
  fi

  sleep 1
done

echo ""
echo "=== Done ==="
