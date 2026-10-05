#!/usr/bin/env bash
# Deletes what Lab 09 created. Set BUCKET to your bucket name first: export BUCKET=cx-lab-12345
set -uo pipefail
export AWS_REGION=${AWS_REGION:-ap-south-1}
aws kinesis delete-stream --stream-name cx-events --enforce-consumer-deletion && echo "Kinesis stream deleted"
if [ -n "${BUCKET:-}" ]; then
  aws s3 rm "s3://$BUCKET" --recursive && aws s3 rb "s3://$BUCKET" && echo "Bucket deleted"
fi
echo "Also delete by hand in the console: the Lambda function, its IAM role, the CloudWatch alarm and log group."
