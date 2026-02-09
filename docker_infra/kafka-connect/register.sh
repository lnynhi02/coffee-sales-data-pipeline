#!/bin/bash
set -e

echo "Waiting for Kafka Connect..."
until curl --fail -s http://kafka-connect:8083/connectors >/dev/null; do
  echo "Waiting for Kafka Connect..."
  sleep 3
done

echo "Registering MySQL CDC connector..."

curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/config/mysql-src-connector.json \
|| echo "Connector already exists"
