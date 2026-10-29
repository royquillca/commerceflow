#!/bin/bash
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --cli-binary-format raw-in-base64-out \
  --payload '{}' \
  response.json

cat response.json


# Load Customers and Products
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --payload '{"action":"seed_reference"}' \
  response.json

cat response.json

# Load Orders
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --payload '{"action":"seed_orders","start_id":1,"count":100}' \
  response.json

cat response.json