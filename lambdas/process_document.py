"""
Downstream processing handler.

Triggered by DynamoDB Streams when the document META item changes.
When status becomes UPLOADED, mark it PROCESSED and write an audit record.

This version adds:
- Structured JSON logging
- Safe idempotency (ConditionalCheckFailedException is treated as a skip)
- No boto3 calls at import-time (helps tests/CI)
"""

import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(event_type: str, **fields) -> None:
    payload = {
        "event_type": event_type,
        "ts": _utc_now_iso(),
        **fields,
    }
    print(json.dumps(payload, default=str))


def _is_meta_item(new_image: dict) -> bool:
    # We use a known SK pattern: META#v1
    return new_image.get("sk", {}).get("S") == "META#v1"


def _get_status(new_image: dict) -> str | None:
    status_attr = new_image.get("status")
    if not status_attr:
        return None
    return status_attr.get("S")


def _get_document_id(new_image: dict, pk: str) -> str | None:
    doc_id_attr = new_image.get("document_id")
    if doc_id_attr and "S" in doc_id_attr:
        return doc_id_attr["S"]

    # pk is DOC#<id>
    if pk.startswith("DOC#") and "#" in pk:
        return pk.split("#", 1)[1]

    return None


def lambda_handler(event, context):
    table_name = os.environ["DOCUMENTS_TABLE_NAME"]

    # Create boto3 resources inside handler (avoids import-time AWS config issues)
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    records = event.get("Records", [])
    if not records:
        _log("NO_RECORDS")
        return

    _log("STREAM_BATCH_RECEIVED", record_count=len(records))

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

        status = _get_status(new_image)
        if status != "UPLOADED":
            # Only process transitions into UPLOADED
            continue

        pk = new_image["pk"]["S"]
        sk = new_image["sk"]["S"]
        document_id = _get_document_id(new_image, pk)

        now = _utc_now_iso()
        audit_sk = f"AUDIT#{now}"
        processing_version = 1

        _log(
            "PROCESS_ATTEMPT",
            pk=pk,
            sk=sk,
            document_id=document_id,
            incoming_status=status,
        )

        try:
            # Mark as PROCESSED, but only if currently UPLOADED (idempotent safety)
            table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression="SET #st = :processed, processed_at = :p, processing_version = :v, updated_at = :u",
                ConditionExpression="#st = :uploaded",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":uploaded": "UPLOADED",
                    ":processed": "PROCESSED",
                    ":p": now,
                    ":u": now,
                    ":v": processing_version,
                },
            )

            table.put_item(
                Item={
                    "pk": pk,
                    "sk": audit_sk,
                    "document_id": document_id,
                    "event_type": "DOCUMENT_PROCESSED",
                    "timestamp": now,
                    "details": {
                        "processing_version": processing_version,
                    },
                }
            )

            _log(
                "PROCESS_SUCCESS",
                pk=pk,
                document_id=document_id,
                new_status="PROCESSED",
                processing_version=processing_version,
            )

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")

            # This is expected sometimes due to retries / duplicates.
            # Treat it as a SKIP, not a hard failure.
            if code == "ConditionalCheckFailedException":
                _log(
                    "PROCESS_SKIP_ALREADY_HANDLED",
                    pk=pk,
                    document_id=document_id,
                    reason="ConditionalCheckFailedException",
                )
                continue

            _log(
                "PROCESS_ERROR",
                pk=pk,
                document_id=document_id,
                error_code=code,
                error_message=str(e),
            )
            raise

        except Exception as e:
            _log(
                "PROCESS_ERROR",
                pk=pk,
                document_id=document_id,
                error_message=str(e),
            )
            raise
