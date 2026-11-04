#!/bin/bash
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --cli-binary-format raw-in-base64-out \
  --payload '{}' \
  response.json

cat response.json


# Load Customers and Products to Aurora
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --payload '{"action":"seed_reference"}' \
  response.json

cat response.json

# Load Orders to Aurora
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --payload '{"action":"seed_orders","start_id":1,"count":100}' \
  response.json

cat response.json

#  Load table from Aurora to Silver Layer
aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"table_name"}'

# - Example
aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"customers"}'


aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"customers"}'

aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"products"}'