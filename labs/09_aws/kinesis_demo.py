"""Put 50 contact-centre events on Kinesis, then read them back from every shard.
Run: python labs/09_aws/kinesis_demo.py   (stream 'cx-events' must be ACTIVE; AWS_REGION set)
"""
import json
import os
import time
import uuid

import boto3

STREAM = "cx-events"
kinesis = boto3.client("kinesis", region_name=os.environ.get("AWS_REGION", "ap-south-1"))

interactions = [str(uuid.uuid4()) for _ in range(5)]
records = [{
    "Data": json.dumps({"interaction_id": iid, "seq": i, "event_type": "message"}).encode(),
    "PartitionKey": iid,                     # same key -> same shard -> ordered per interaction
} for i in range(10) for iid in interactions]

resp = kinesis.put_records(StreamName=STREAM, Records=records)
print("failed puts:", resp["FailedRecordCount"])           # must retry these in real code
shards = {r["ShardId"] for r in resp["Records"] if "ShardId" in r}
print("shards used:", sorted(shards))

time.sleep(2)
for shard in kinesis.list_shards(StreamName=STREAM)["Shards"]:
    it = kinesis.get_shard_iterator(StreamName=STREAM, ShardId=shard["ShardId"],
                                    ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    out = kinesis.get_records(ShardIterator=it, Limit=100)
    print(f"{shard['ShardId']}: {len(out['Records'])} records, "
          f"millis behind latest={out.get('MillisBehindLatest')}")
