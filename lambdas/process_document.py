"""
Downstream processing handler.

Triggered by DynamoDB Streams when the document META item changes.
When status becomes UPLOADED, mark it PROCESSED, write an audit record,
and generate a processed output artifact in S3 (summary.json).
"""

import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# IMPORTANT: create clients lazily to avoid import-time AWS config errors in CI
_dynamodb = None
_s3 = None


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_meta_item(new_image: dict) -> bool:
    # DynamoDB Streams uses typed attribute values: {"S": "..."}
    return new_image.get("sk", {}).get("S") == "META#v1"


def _get_str_attr(new_image: dict, name: str) -> str | None:
    attr = new_image.get(name)
    if not attr:
        return None
    return attr.get("S")


def _get_num_attr(new_image: dict, name: str) -> int | None:
    attr = new_image.get(name)
    if not attr:
        return None
    n = attr.get("N")
    if n is None:
        return None
    try:
        return int(n)
    except ValueError:
        return None


def _derive_document_id_from_pk(pk: str) -> str | None:
    if pk.startswith("DOC#"):
        return pk.split("#", 1)[1]
    return None


def _build_processed_summary(
    new_image: dict, document_id: str, processed_at: str
) -> dict:
    return {
        "document_id": document_id,
        "status": "PROCESSED",
        "processed_at": processed_at,
        "filename": _get_str_attr(new_image, "filename"),
        "bucket": _get_str_attr(new_image, "bucket"),
        "s3_key": _get_str_attr(new_image, "s3_key"),
        "page_count": _get_num_attr(new_image, "page_count"),
        "text_preview": _get_str_attr(new_image, "text_preview") or "",
        "processing_version": 1,
    }


def lambda_handler(event, context):
    table_name = os.environ["DOCUMENTS_TABLE_NAME"]
    bucket_name = os.environ["DOCUMENTS_BUCKET_NAME"]

    dynamodb = _get_dynamodb()
    table = dynamodb.Table(table_name)
    s3 = _get_s3()

    records = event.get("Records", [])
    if not records:
        print(json.dumps({"event_type": "NO_RECORDS"}))
        return

    # Helpful: log batch arrival
    print(
        json.dumps(
            {
                "event_type": "STREAM_BATCH_RECEIVED",
                "ts": _utc_now_iso(),
                "record_count": len(records),
            }
        )
    )

    for record in records:
        event_name = record.get("eventName")
        if event_name not in ("INSERT", "MODIFY"):
            continue

        ddb = record.get("dynamodb", {})
        new_image = ddb.get("NewImage")
        if not new_image:
            continue

        if not _is_meta_item(new_image):
            continue

        status = _get_str_attr(new_image, "status")
        if status != "UPLOADED":
            continue

        pk = new_image["pk"]["S"]
        sk = new_image["sk"]["S"]

        document_id = _get_str_attr(
            new_image, "document_id"
        ) or _derive_document_id_from_pk(pk)
        if not document_id:
            print(json.dumps({"event_type": "SKIP_NO_DOCUMENT_ID", "pk": pk}))
            continue

        now = _utc_now_iso()
        audit_sk = f"AUDIT#{now}"

        processed_key = f"documents/{document_id}/processed/summary.json"

        print(
            json.dumps(
                {
                    "event_type": "PROCESS_ATTEMPT",
                    "ts": now,
                    "pk": pk,
                    "sk": sk,
                    "document_id": document_id,
                    "incoming_status": status,
                    "processed_output_key": processed_key,
                }
            )
        )

        try:
            # 1) Generate summary JSON from stream image
            summary = _build_processed_summary(new_image, document_id, now)
            summary_bytes = json.dumps(summary, indent=2).encode("utf-8")

            # 2) Upload processed artifact to S3
            s3.put_object(
                Bucket=bucket_name,
                Key=processed_key,
                Body=summary_bytes,
                ContentType="application/json",
            )

            # 3) Mark as PROCESSED (idempotent safety: only if still UPLOADED)
            table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression=(
                    "SET #st = :processed, processed_at = :p, processing_version = :v, updated_at = :u, "
                    "processed_output_bucket = :b, processed_output_key = :k"
                ),
                ConditionExpression="#st = :uploaded",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":uploaded": "UPLOADED",
                    ":processed": "PROCESSED",
                    ":p": now,
                    ":u": now,
                    ":v": 1,
                    ":b": bucket_name,
                    ":k": processed_key,
                },
            )

            # 4) Audit record
            table.put_item(
                Item={
                    "pk": pk,
                    "sk": audit_sk,
                    "document_id": document_id,
                    "event_type": "DOCUMENT_PROCESSED",
                    "timestamp": now,
                    "details": {
                        "processing_version": 1,
                        "processed_output_bucket": bucket_name,
                        "processed_output_key": processed_key,
                    },
                }
            )

            print(
                json.dumps(
                    {
                        "event_type": "PROCESS_SUCCESS",
                        "ts": _utc_now_iso(),
                        "pk": pk,
                        "document_id": document_id,
                        "new_status": "PROCESSED",
                        "processing_version": 1,
                        "processed_output_key": processed_key,
                    }
                )
            )

        except ClientError as e:
            # AWS client errors (S3/DDB)
            print(
                json.dumps(
                    {
                        "event_type": "PROCESS_AWS_ERROR",
                        "ts": _utc_now_iso(),
                        "pk": pk,
                        "document_id": document_id,
                        "error": str(e),
                    }
                )
            )
            raise
        except Exception as e:
            print(
                json.dumps(
                    {
                        "event_type": "PROCESS_ERROR",
                        "ts": _utc_now_iso(),
                        "pk": pk,
                        "document_id": document_id,
                        "error": str(e),
                    }
                )
            )
            raise
