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

# Ejecucion del Step Functions
aws stepfunctions list-state-machines \
  --region us-east-1 \
  --query "stateMachines[?name=='commerceflow-dev-batch-pipeline'].[name,stateMachineArn]" \
  --output table

aws stepfunctions start-execution \
  --state-machine-arn "PEGA_AQUI_EL_ARN" \
  --input '{}' \
  --region us-east-1

# obtener ARN de ejecución
aws stepfunctions list-executions \
  --state-machine-arn "ARN_DE_TU_STATE_MACHINE" \
  --max-results 1 \
  --region us-east-1 \
  --query 'executions[0].executionArn' \
  --output text

# Verificar el estado
aws stepfunctions describe-execution \
  --execution-arn "PEGA_AQUI_EL_EXECUTION_ARN_DE_EJECUCION" \
  --region us-east-1 \
  --query '{Status:status,Output:output,Error:error,Cause:cause}'

EXEC_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "ARN_DE_TU_STATE_MACHINE" \
  --region us-east-1 \
  --max-results 1 \
  --query 'executions[0].executionArn' \
  --output text)

aws stepfunctions describe-execution \
  --execution-arn "$EXEC_ARN" \
  --region us-east-1 \
  --query '{Status:status,Output:output,Error:error,Cause:cause}'

#########################################################################33

aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:us-east-1:716616997513:stateMachine:commerceflow-dev-batch-pipeline" \
  --max-results 1 \
  --region us-east-1 \
  --query 'executions[0].executionArn' \
  --output text


aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:us-east-1:716616997513:stateMachine:commerceflow-dev-batch-pipeline" \
  --region us-east-1 \
  --query '{Status:status,Output:output,Error:error,Cause:cause}'

aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:716616997513:stateMachine:commerceflow-dev-batch-pipeline" \
  --input '{}' \
  --region us-east-1


EXEC_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:us-east-1:716616997513:stateMachine:commerceflow-dev-batch-pipeline" \
  --region us-east-1 \
  --max-results 1 \
  --query 'executions[0].executionArn' \
  --output text)

aws stepfunctions describe-execution \
  --execution-arn "$EXEC_ARN" \
  --region us-east-1 \
  --query '{Status:status,Output:output,Error:error,Cause:cause}'