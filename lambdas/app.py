"""
Lambda handler for document registration.

This module handles POST /documents requests, creates a document record
in DynamoDB, and returns S3 upload details.
"""

import json
import os
import uuid
from datetime import datetime, timezone

import boto3

dynamodb = boto3.resource("dynamodb")


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    """
    POST /documents
    Body (JSON):
      {
        "filename": "example.pdf"
      }

    Behavior:
    - Generates a document_id
    - Creates a metadata record in DynamoDB
    - Creates an audit record in DynamoDB
    - Returns bucket + s3_key suggestion for upload
    """
    table_name = os.environ["DOCUMENTS_TABLE_NAME"]
    bucket_name = os.environ["DOCUMENTS_BUCKET_NAME"]
    table = dynamodb.Table(table_name)

    # API Gateway puts the request body into event["body"] as a string
    raw_body = event.get("body") or "{}"

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        return _response(400, {"error": "Request body must be valid JSON"})

    filename = body.get("filename")
    if not filename or not isinstance(filename, str):
        return _response(400, {"error": "filename is required and must be a string"})

    document_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    # We suggest a clean S3 key structure:
    # documents/<document_id>/original/<filename>
    s3_key = f"documents/{document_id}/original/{filename}"

    # DynamoDB single-table style keys:
    pk = f"DOC#{document_id}"
    meta_sk = "META#v1"
    audit_sk = f"AUDIT#{created_at}"

    # Metadata item
    metadata_item = {
        "pk": pk,
        "sk": meta_sk,
        "document_id": document_id,
        "filename": filename,
        "bucket": bucket_name,
        "s3_key": s3_key,
        "status": "REGISTERED",
        "version": 1,
        "created_at": created_at,
        "updated_at": created_at,
    }

    # Audit item
    audit_item = {
        "pk": pk,
        "sk": audit_sk,
        "document_id": document_id,
        "event_type": "DOCUMENT_REGISTERED",
        "timestamp": created_at,
        "details": {
            "filename": filename,
            "bucket": bucket_name,
            "s3_key": s3_key,
        },
    }

    # Write both items
    # We use a batch writer for convenience. It's not transactional,
    # but good enough for this stage. Later we can switch to TransactWriteItems.
    with table.batch_writer() as batch:
        batch.put_item(Item=metadata_item)
        batch.put_item(Item=audit_item)

    return _response(
        201,
        {
            "document_id": document_id,
            "bucket": bucket_name,
            "s3_key": s3_key,
            "status": "REGISTERED",
            "created_at": created_at,
        },
    )
