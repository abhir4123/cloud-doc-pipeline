from hello_world.s3_ingest import _extract_document_id_from_s3_key


def test_extract_document_id_valid_key():
    key = "documents/abc123/original/file.pdf"
    assert _extract_document_id_from_s3_key(key) == "abc123"


def test_extract_document_id_invalid_prefix():
    key = "uploads/abc123/original/file.pdf"
    assert _extract_document_id_from_s3_key(key) is None


def test_extract_document_id_too_short():
    key = "documents/abc123/file.pdf"
    assert _extract_document_id_from_s3_key(key) is None


def test_extract_document_id_empty_doc_id():
    key = "documents//original/file.pdf"
    assert _extract_document_id_from_s3_key(key) is None
