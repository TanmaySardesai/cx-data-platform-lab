"""S3-triggered Lambda: reads a JSON event from raw/, redacts PII, writes it to processed/."""
import json
import logging
import re
import urllib.parse

import boto3

s3 = boto3.client("s3")
log = logging.getLogger()
log.setLevel(logging.INFO)

PHONE = re.compile(r"\+?\d[\d\s-]{8,}\d")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


def redact(text: str) -> str:
    return EMAIL.sub("<EMAIL>", PHONE.sub("<PHONE>", text or ""))


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        body = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        body["text"] = redact(body.get("text", ""))
        out_key = key.replace("raw/", "processed/", 1)
        s3.put_object(Bucket=bucket, Key=out_key, Body=json.dumps(body).encode(),
                      ServerSideEncryption="AES256")
        log.info(json.dumps({"msg": "redacted", "in": key, "out": out_key}))
    return {"processed": len(event["Records"])}
