"""
S3 ingest handler.

Triggered when a PDF is uploaded to the DocumentsBucket under documents/.
Updates the document status in DynamoDB to reflect that the file is uploaded.
Also extracts basic PDF metadata using PyPDF2.
"""

import io
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from PyPDF2 import PdfReader


def extract_pdf_metadata(pdf_bytes: bytes) -> tuple[int, str]:
    """
    Extract basic metadata from a PDF.

    Returns:
      (page_count, text_preview)

    text_preview is a short snippet from the first page, max 500 chars.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page_count = len(reader.pages)

    text_preview = ""
    if page_count > 0:
        first_page_text = (
            reader.pages[0].extract_text() or ""
        )  # pylint: disable=no-member
        text_preview = first_page_text.strip().replace("\x00", "")[:500]

    return page_count, text_preview


def _extract_document_id_from_s3_key(s3_key: str) -> str | None:
    """
    Expected key format:
      documents/<document_id>/original/<filename>.pdf

    Returns document_id if valid, otherwise None.
    """
    parts = s3_key.split("/")
    if len(parts) < 4:
        return None
    if parts[0] != "documents":
        return None
    if not parts[1]:
        return None
    return parts[1]


def lambda_handler(event, context):
    table_name = os.environ["DOCUMENTS_TABLE_NAME"]

    # Create AWS clients/resources at runtime (not import time)
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.client("s3")

    table = dynamodb.Table(table_name)

    records = event.get("Records", [])
    if not records:
        print("No Records in event")
        return

    for record in records:
        s3_info = record.get("s3", {})
        bucket_name = s3_info.get("bucket", {}).get("name")
        raw_key = s3_info.get("object", {}).get("key")

        if not bucket_name or not raw_key:
            print(f"Missing bucket/key in record: {record}")
            continue

        s3_key = unquote_plus(raw_key)

        document_id = _extract_document_id_from_s3_key(s3_key)
        if not document_id:
            print(f"Could not extract document_id from key: {s3_key}")
            continue

        pk = f"DOC#{document_id}"
        meta_sk = "META#v1"

        now = datetime.now(timezone.utc).isoformat()
        audit_sk = f"AUDIT#{now}"

        try:
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            pdf_bytes = obj["Body"].read()

            page_count, text_preview = extract_pdf_metadata(pdf_bytes)

            table.update_item(
                Key={"pk": pk, "sk": meta_sk},
                UpdateExpression="SET #st = :s, updated_at = :u, page_count = :p, text_preview = :t",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":s": "UPLOADED",
                    ":u": now,
                    ":p": page_count,
                    ":t": text_preview,
                },
            )

            table.put_item(
                Item={
                    "pk": pk,
                    "sk": audit_sk,
                    "document_id": document_id,
                    "event_type": "DOCUMENT_UPLOADED",
                    "timestamp": now,
                    "details": {
                        "bucket": bucket_name,
                        "s3_key": s3_key,
                        "page_count": page_count,
                    },
                }
            )

            print(
                f"Updated document {document_id} to UPLOADED for key {s3_key} "
                f"(page_count={page_count})"
            )

        except Exception as e:
            print(f"Error processing upload for key={s3_key}: {e}")
            raise
