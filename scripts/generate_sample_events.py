"""
Generates sample event JSON files matching the shape the Lambda handler
and dbt staging model expect, and optionally uploads them straight to the
raw S3 bucket to trigger the pipeline.

Usage:
    python generate_sample_events.py --count 5
    python generate_sample_events.py --count 5 --upload --bucket eventdriven-pipeline-raw

Each file is a JSON array of 1-20 nested event records, so you can also
exercise the pandas.json_normalize flattening logic (nested "metadata"
and "address" objects) rather than just flat records.
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

EVENT_TYPES = ["purchase", "refund", "signup", "subscription_renewal"]
CURRENCIES = ["USD", "EUR", "AUD", "GBP"]


def random_event(base_time: datetime) -> dict:
    event_type = random.choice(EVENT_TYPES)
    offset = timedelta(minutes=random.randint(0, 60 * 24))
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "user_id": f"u_{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5, 500), 2)
        if event_type in ("purchase", "refund", "subscription_renewal")
        else 0.0,
        "currency": random.choice(CURRENCIES),
        "created_at": (base_time + offset).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # nested fields to exercise the flattening logic
        "metadata": {
            "device": random.choice(["ios", "android", "web"]),
            "campaign": random.choice(["organic", "email", "paid_search", None]),
        },
        "address": {
            "country": random.choice(["AU", "US", "GB", "DE"]),
            "city": random.choice(["Sydney", "Austin", "London", "Berlin"]),
        },
    }


def generate_file(records_per_file: int, base_time: datetime) -> list:
    return [random_event(base_time) for _ in range(records_per_file)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=3, help="Number of files to generate")
    parser.add_argument(
        "--records-per-file",
        type=int,
        default=None,
        help="Records per file (default: random 1-20 per file)",
    )
    parser.add_argument("--out-dir", type=str, default="sample_data")
    parser.add_argument("--upload", action="store_true", help="Upload directly to S3 raw/ prefix")
    parser.add_argument("--bucket", type=str, help="Raw bucket name (required if --upload)")
    args = parser.parse_args()

    if args.upload and not args.bucket:
        parser.error("--bucket is required when using --upload")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_time = datetime.now(timezone.utc) - timedelta(days=1)

    s3 = None
    if args.upload:
        import boto3

        s3 = boto3.client("s3")

    for i in range(args.count):
        n = args.records_per_file or random.randint(1, 20)
        records = generate_file(n, base_time)
        filename = f"events_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{i}.json"
        filepath = out_dir / filename

        with open(filepath, "w") as f:
            json.dump(records, f, indent=2)

        print(f"Wrote {filepath} ({n} records)")

        if args.upload:
            key = f"raw/{filename}"
            s3.upload_file(str(filepath), args.bucket, key)
            print(f"  -> uploaded to s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
