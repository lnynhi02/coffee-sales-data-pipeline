#!/bin/bash
set -e

echo "Waiting for Kafka Connect..."
until curl -s http://kafka-connect:8083/connectors >/dev/null; do
  echo "Waiting for Kafka Connect..."
  sleep 3
done

echo "Registering MySQL CDC connector..."

RESPONSE=$(curl -s -w "\n%{http_code}" \
  -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/config/mysql-src-connector.json)

BODY=$(echo "$RESPONSE" | sed '$d')
HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)

echo "$BODY"

if [ "$HTTP_CODE" = "201" ]; then
  echo "✅ Connector created successfully"
elif [ "$HTTP_CODE" = "409" ]; then
  echo "⚠️ Connector already exists"
else
  echo "❌ Failed to create connector (HTTP $HTTP_CODE)"
  exit 1
fi
