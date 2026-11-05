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

aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"order_items"}'

aws glue start-job-run \
  --job-name commerceflow-dev-aurora-to-bronze \
  --region us-east-1 \
  --arguments '{"--SOURCE_TABLE":"payments"}'


# test de aurora
aws lambda invoke \
  --function-name commerceflow-dev-aurora-test \
  --region us-east-1 \
  --payload '{"action":"test"}' \
  aurora-test-response.json

cat aurora-test-response.json


# estado de aurora
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS \
  --metric-name ServerlessDatabaseCapacity \
  --dimensions Name=DBInstanceIdentifier,Value=commerceflow-dev-writer \
  --start-time "$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 60 \
  --statistics Average \
  --region us-east-1 \
  --query 'sort_by(Datapoints,&Timestamp)[].{Time:Timestamp,ACU:Average}' \
  --output table