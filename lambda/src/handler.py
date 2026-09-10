"""
Lambda entrypoint triggered by EventBridge on S3 "Object Created" events
for the raw/ prefix.

Responsibilities:
  1. Download the newly-uploaded JSON file from the raw bucket.
  2. Flatten nested JSON (records may have nested dicts/lists) into a
     tabular structure with pandas.json_normalize.
  3. Write the result as Parquet to the processed bucket, preserving the
     original filename (minus extension) so Airflow's sensor can find it.
  4. Trigger the Airflow DAG directly via its REST API, passing the exact
     S3 key just written -- this is what makes the pipeline genuinely
     event-driven rather than relying on a polling schedule. Requires
     AIRFLOW_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD to be set
     (see terraform/modules/lambda/main.tf); if unset, this step is
     skipped entirely and the file is left for manual/scheduled recovery.

This intentionally replaces the AWS Glue step from the original design --
Glue's cold-start (5-10 min per job) is painful for a project you'll be
iterating on, and a single JSON file per event doesn't need Spark's
distributed processing. Swap this for Glue if file sizes grow into the
GB+ range.
"""

import base64
import io
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
PROCESSED_PREFIX = "processed/"

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "").rstrip("/")
AIRFLOW_API_USER = os.environ.get("AIRFLOW_API_USER", "")
AIRFLOW_API_PASSWORD = os.environ.get("AIRFLOW_API_PASSWORD", "")
AIRFLOW_DAG_ID = "s3_to_snowflake_dbt"


def trigger_airflow_dag(s3_key: str) -> None:
    """
    POSTs to Airflow's REST API to start a DAG run immediately, passing
    the exact processed S3 key as `conf` so the DAG's sensor can match it
    directly rather than waiting on a wildcard/polling schedule.

    Deliberately non-fatal: if this fails (network blip, EC2 box down for
    maintenance, credentials rotated, etc.), we log a warning and return
    rather than raising. The Parquet file is already safely written --
    losing the trigger call just means it waits for the next scheduled
    run or a manual trigger, not that data is lost.
    """
    if not AIRFLOW_BASE_URL:
        logger.info("AIRFLOW_BASE_URL not set -- skipping DAG trigger call")
        return

    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
    body = json.dumps({"conf": {"s3_key": s3_key}}).encode("utf-8")

    credentials = base64.b64encode(
        f"{AIRFLOW_API_USER}:{AIRFLOW_API_PASSWORD}".encode("utf-8")
    ).decode("ascii")

    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}",
        },
    )

    try:
        # Short timeout: this is a best-effort callback, not something
        # worth letting the whole Lambda invocation hang on.
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Triggered Airflow DAG run: HTTP %s", resp.status)
    except urllib.error.HTTPError as exc:
        logger.warning(
            "Airflow API returned HTTP %s when triggering DAG: %s",
            exc.code,
            exc.read().decode("utf-8", errors="replace"),
        )
    except urllib.error.URLError as exc:
        logger.warning("Could not reach Airflow to trigger DAG: %s", exc.reason)


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

    trigger_airflow_dag(processed_key)

    return {
        "status": "success",
        "rows": len(df),
        "output": f"s3://{PROCESSED_BUCKET}/{processed_key}",
    }
