"""
Lambda handler for document registration + retrieval.

This module handles:
- POST /documents  -> creates a document record in DynamoDB and returns S3 upload details
- GET  /documents/{document_id} -> returns the current META record (status, output pointers, etc.)
"""

import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# Lazy init to avoid region issues during tests/import time.
_DDB_RESOURCE = None


def _ddb_resource():
    global _DDB_RESOURCE
    if _DDB_RESOURCE is None:
        region = (
            os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        _DDB_RESOURCE = boto3.resource("dynamodb", region_name=region)
    return _DDB_RESOURCE


def _table():
    table_name = os.environ["DOCUMENTS_TABLE_NAME"]
    return _ddb_resource().Table(table_name)


def _json_safe(obj):
    """
    Convert DynamoDB return types (notably Decimal) into JSON-serializable types.
    DynamoDB uses Decimal for all numbers.
    """
    if isinstance(obj, Decimal):
        # Convert clean integers to int; otherwise float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)

    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]

    return obj


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(_json_safe(body)),
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_document_id_from_event(event: dict) -> str | None:
    # API Gateway REST API (v1)
    path_params = event.get("pathParameters") or {}
    doc_id = path_params.get("document_id")
    if doc_id:
        return doc_id

    # Fallbacks for other integrations
    doc_id = path_params.get("proxy")
    if doc_id:
        return doc_id

    return None


def _handle_post_documents(event: dict):
    """
    POST /documents
    Body (JSON):
      { "filename": "example.pdf" }

    Behavior:
    - Generates a document_id
    - Creates a metadata record in DynamoDB
    - Creates an audit record in DynamoDB
    - Returns bucket + s3_key suggestion for upload
    """
    bucket_name = os.environ["DOCUMENTS_BUCKET_NAME"]
    table = _table()

    raw_body = event.get("body") or "{}"
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        return _response(400, {"error": "Request body must be valid JSON"})

    filename = body.get("filename")
    if not filename or not isinstance(filename, str):
        return _response(400, {"error": "filename is required and must be a string"})

    document_id = str(uuid.uuid4())
    created_at = _utc_now_iso()

    s3_key = f"documents/{document_id}/original/{filename}"

    pk = f"DOC#{document_id}"
    meta_sk = "META#v1"
    audit_sk = f"AUDIT#{created_at}"

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


def _handle_get_document(event: dict):
    """
    GET /documents/{document_id}
    Returns the META#v1 item.
    """
    doc_id = _get_document_id_from_event(event)
    if not doc_id:
        return _response(400, {"error": "document_id path parameter is required"})

    pk = f"DOC#{doc_id}"
    table = _table()

    resp = table.get_item(Key={"pk": pk, "sk": "META#v1"})
    item = resp.get("Item")
    if not item:
        return _response(404, {"error": "Document not found", "document_id": doc_id})

    return _response(200, {"document": item})


def lambda_handler(event, context):
    method = event.get("httpMethod") or event.get("requestContext", {}).get(
        "http", {}
    ).get("method")

    if method == "POST":
        return _handle_post_documents(event)

    if method == "GET":
        return _handle_get_document(event)

    return _response(405, {"error": "Method not allowed"})
