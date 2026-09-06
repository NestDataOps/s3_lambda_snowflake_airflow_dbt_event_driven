"""
Lambda entrypoint triggered by EventBridge on S3 "Object Created" events
for the raw/ prefix.

Responsibilities:
  1. Download the newly-uploaded JSON file from the raw bucket.
  2. Flatten nested JSON (records may have nested dicts/lists) into a
     tabular structure with pandas.json_normalize.
  3. Write the result as Parquet to the processed bucket, preserving the
     original filename (minus extension) so Airflow's sensor can find it.

This intentionally replaces the AWS Glue step from the original design --
Glue's cold-start (5-10 min per job) is painful for a project you'll be
iterating on, and a single JSON file per event doesn't need Spark's
distributed processing. Swap this for Glue if file sizes grow into the
GB+ range.
"""

import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
PROCESSED_PREFIX = "processed/"


def lambda_handler(event, context):
    """
    event shape (EventBridge -> S3 Object Created):
    {
      "detail": {
        "bucket": {"name": "..."},
        "object": {"key": "raw/some-file.json"}
      }
    }
    """
    detail = event["detail"]
    bucket = detail["bucket"]["name"]
    key = detail["object"]["key"]

    logger.info("Processing s3://%s/%s", bucket, key)

    if not key.lower().endswith(".json"):
        logger.info("Skipping non-JSON object: %s", key)
        return {"status": "skipped", "key": key}

    raw_obj = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = raw_obj["Body"].read()

    try:
        payload = json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse JSON for %s: %s", key, exc)
        raise

    # Normalize whether the file is a single object or a list of records
    records = payload if isinstance(payload, list) else [payload]

    df = pd.json_normalize(records, sep="_")

    file_stem = os.path.splitext(os.path.basename(key))[0]
    df["source_file_name"] = file_stem
    df["ingested_at"] = datetime.now(timezone.utc)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    processed_key = f"{PROCESSED_PREFIX}{file_stem}.parquet"
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=processed_key,
        Body=buffer.getvalue(),
    )

    logger.info(
        "Wrote %d rows to s3://%s/%s", len(df), PROCESSED_BUCKET, processed_key
    )

    return {
        "status": "success",
        "rows": len(df),
        "output": f"s3://{PROCESSED_BUCKET}/{processed_key}",
    }
