import boto3
import json

BUCKET = "wiki-lambda-x23352370"
PREFIX = "raw/"

s3 = boto3.client("s3", region_name="us-east-1")
paginator = s3.get_paginator("list_objects_v2")
fixed_count = 0

for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
        if not body.strip():
            continue

        decoder = json.JSONDecoder()
        records = []
        idx = 0
        body_stripped = body.strip()
        while idx < len(body_stripped):
            obj_json, end = decoder.raw_decode(body_stripped, idx)
            records.append(json.dumps(obj_json))
            idx = end
            while idx < len(body_stripped) and body_stripped[idx].isspace():
                idx += 1

        fixed_body = "\n".join(records) + "\n"
        s3.put_object(Bucket=BUCKET, Key=key, Body=fixed_body.encode("utf-8"))
        fixed_count += 1
        print(f"Fixed {key} ({len(records)} records)")

print(f"Done. Fixed {fixed_count} files.")