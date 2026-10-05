# Lab 09 — AWS hands-on on the free tier (Sat Part A, ~75 min)

**Before anything: Billing → Budgets → create a $5 monthly cost budget with an email alert.**
Use region `ap-south-1` (Mumbai). Run `bash labs/09_aws/cleanup.sh` at the end. **Kinesis is NOT free** (on-demand ≈ a few US cents per hour); it's fine for 30 minutes, but delete it the same day.

Install and configure the CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html then `aws configure` (use an IAM user or IAM Identity Center for the lab — never the root account's keys).

## Part A1 — S3 + IAM least privilege (20 min)
```bash
export AWS_REGION=ap-south-1 BUCKET=cx-lab-$RANDOM
aws s3 mb s3://$BUCKET
aws s3api put-public-access-block --bucket $BUCKET --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
echo '{"interaction_id":"1","text":"hello"}' > /tmp/e.json && aws s3 cp /tmp/e.json s3://$BUCKET/raw/
```
Read `iam_lambda_policy.json`: it lets the Lambda read ONLY `raw/` and write ONLY `processed/` in this bucket. Explain why that's better than `s3:*`.

## Part A2 — Lambda triggered by S3 (25 min)
Console is fine here: Lambda → Create function → Python 3.12 → paste `lambda_function.py`.
- Execution role: create one, then attach the inline policy from `iam_lambda_policy.json` (replace BUCKET_NAME).
- Add trigger: S3, your bucket, prefix `raw/`, event "All object create events".
- Upload a file to `raw/` with a phone number in `text` → check `processed/` has the redacted copy.
- CloudWatch → Log groups → `/aws/lambda/<name>`: find the log line. Create an **alarm** on the function's `Errors` metric (≥1 in 5 minutes).

## Part A3 — Kinesis Data Streams (25 min)
```bash
aws kinesis create-stream --stream-name cx-events --stream-mode-details StreamMode=ON_DEMAND
aws kinesis describe-stream-summary --stream-name cx-events     # wait for ACTIVE
python labs/09_aws/kinesis_demo.py                              # puts 50 records, reads them back
```
Note: partition key = interaction_id (same idea as the Kafka key). Look at the stream's monitoring tab: IncomingRecords, GetRecords.IteratorAgeMilliseconds (= Kinesis "consumer lag").

## Part A4 — read-only tour (no cost, 15 min)
Open the consoles for **EMR Serverless / EMR on EKS**, **Data Firehose** (create-flow wizard: Kinesis → S3 as Parquet), **Glue Data Catalog**, **Lake Formation**. Don't create anything; just be able to describe what each one is for.

## Clean up
`bash labs/09_aws/cleanup.sh` then check Billing → Bills in a day or two.
