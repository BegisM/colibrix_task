import csv
import io
import json
import os
from typing import Any, Dict, Tuple

import boto3

from models import validate_record

# Reuse a single S3 client for the whole Lambda container
s3 = boto3.client("s3")


def _extract_bucket_key_from_event(event: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extract (bucket, key) from the incoming event.

    I support two scenarios:
    1) Direct call from Airflow / custom code:
       { "bucket": "...", "key": "..." }

    2) S3 event notification:
       { "Records": [ { "s3": { "bucket": { "name": ... }, "object": { "key": ... } } } ] }
    """
    # Option 1: direct invocation (e.g. from Airflow)
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]

    # Option 2: S3 event notification
    if "Records" in event and event["Records"]:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
        return bucket, key

    # If neither pattern matches, I fail fast
    raise ValueError("Cannot determine S3 bucket/key from event")


def _build_output_keys(input_key: str) -> Tuple[str, str]:
    """
    Take an input key, for example:

      datalake/.../card_transaction_2025-11-10.csv

    and produce:

      datalake/.../card_transaction_2025-11-10_valid.jsonl
      datalake/.../card_transaction_2025-11-10_invalid.jsonl

    I keep the same path, only change the suffix.
    """
    if not input_key.endswith(".csv"):
        raise ValueError(f"Unexpected input key extension (expected .csv): {input_key}")

    base = input_key[:-4]  # remove ".csv"
    valid_key = f"{base}_valid.jsonl"
    invalid_key = f"{base}_invalid.jsonl"
    return valid_key, invalid_key


def handler(event, context):
    """
    Main Lambda entry point.

    High-level steps:
    1. Figure out which S3 object to read (bucket + key).
    2. Read the CSV from S3.
    3. Validate each row using the Pydantic model.
    4. Write valid and invalid rows as separate JSONL files to the green-zone bucket.
    """
    print("Received event:", json.dumps(event))

    # 1) Where is the input file?
    landing_bucket, input_key = _extract_bucket_key_from_event(event)

    # Bucket where I write the processed JSONL output.
    # Default is "green-zone-datalake", but it can be overridden via env var in AWS.
    green_bucket = os.environ.get("GREEN_ZONE_BUCKET", "green-zone-datalake")

    # 2) Read CSV file from S3
    obj = s3.get_object(Bucket=landing_bucket, Key=input_key)
    body = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(body))

    valid_lines = []   # List of JSON strings for valid records
    invalid_lines = [] # List of JSON strings for invalid records

    # 3) Validate each row using the Pydantic model
    for row in reader:
        ok, value, errors = validate_record(row)

        if ok:
            # value is a Pydantic model; mode="json" converts datetime to ISO8601 string
            valid_lines.append(json.dumps(value.model_dump(mode="json")))
        else:
            invalid_lines.append(
                json.dumps(
                    {
                        "row": row,
                        "errors": errors,
                    }
                )
            )

    # 4) Determine where to store the outputs in S3
    valid_key, invalid_key = _build_output_keys(input_key)

    # Write valid JSONL if there is at least one valid row
    if valid_lines:
        valid_payload = "\n".join(valid_lines).encode("utf-8")
        s3.put_object(
            Bucket=green_bucket,
            Key=valid_key,
            Body=valid_payload,
            ContentType="application/json",
        )

    # Write invalid JSONL if there is at least one invalid row
    if invalid_lines:
        invalid_payload = "\n".join(invalid_lines).encode("utf-8")
        s3.put_object(
            Bucket=green_bucket,
            Key=invalid_key,
            Body=invalid_payload,
            ContentType="application/json",
        )

    result = {
        "landing_bucket": landing_bucket,
        "input_key": input_key,
        "green_bucket": green_bucket,
        "valid_key": valid_key,
        "invalid_key": invalid_key,
        "valid_count": len(valid_lines),
        "invalid_count": len(invalid_lines),
    }

    print("Processing result:", json.dumps(result))
    return result


if __name__ == "__main__":
    """
    Convenience entry point for running the same logic locally.

    Here I don't use S3 at all – I just read the CSV file from disk
    and write the JSONL output into sample_output/.
    """
    csv_path = os.path.join(os.path.dirname(__file__), "..", "card_transaction_2025-11-10.csv")
    csv_path = os.path.abspath(csv_path)

    output_dir = os.path.join(os.path.dirname(__file__), "..", "sample_output")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Local test: reading {csv_path}")

    valid_lines = []
    invalid_lines = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ok, value, errors = validate_record(row)
            if ok:
                valid_lines.append(json.dumps(value.model_dump(mode="json")))
            else:
                invalid_lines.append(json.dumps({"row": row, "errors": errors}))

    base_name = "card_transaction_2025-11-10"
    valid_file = os.path.join(output_dir, f"{base_name}_valid.jsonl")
    invalid_file = os.path.join(output_dir, f"{base_name}_invalid.jsonl")

    with open(valid_file, "w", encoding="utf-8") as f:
        for line in valid_lines:
            f.write(line + "\n")

    with open(invalid_file, "w", encoding="utf-8") as f:
        for line in invalid_lines:
            f.write(line + "\n")

    print(f"Wrote {len(valid_lines)} valid rows to {valid_file}")
    print(f"Wrote {len(invalid_lines)} invalid rows to {invalid_file}")
