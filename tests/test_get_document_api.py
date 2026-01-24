import json
import os

import lambdas.app as app


class _FakeBatchWriter:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def put_item(self, Item):
        return None


class _FakeTable:
    def __init__(self, item=None):
        self._item = item

    def get_item(self, Key):
        if self._item is None:
            return {}
        return {"Item": self._item}

    def batch_writer(self):
        return _FakeBatchWriter()


def test_get_document_returns_200(monkeypatch):
    os.environ["DOCUMENTS_TABLE_NAME"] = "dummy"
    os.environ["DOCUMENTS_BUCKET_NAME"] = "dummy-bucket"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    fake_item = {
        "pk": "DOC#abc",
        "sk": "META#v1",
        "document_id": "abc",
        "status": "PROCESSED",
        "processed_output_bucket": "b",
        "processed_output_key": "k",
        "page_count": 1,
    }

    monkeypatch.setattr(app, "_table", lambda: _FakeTable(item=fake_item))

    event = {
        "httpMethod": "GET",
        "pathParameters": {"document_id": "abc"},
    }

    resp = app.lambda_handler(event, None)
    assert resp["statusCode"] == 200

    body = json.loads(resp["body"])
    assert body["document"]["document_id"] == "abc"
    assert body["document"]["status"] == "PROCESSED"


def test_get_document_returns_404(monkeypatch):
    os.environ["DOCUMENTS_TABLE_NAME"] = "dummy"
    os.environ["DOCUMENTS_BUCKET_NAME"] = "dummy-bucket"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    monkeypatch.setattr(app, "_table", lambda: _FakeTable(item=None))

    event = {
        "httpMethod": "GET",
        "pathParameters": {"document_id": "does-not-exist"},
    }

    resp = app.lambda_handler(event, None)
    assert resp["statusCode"] == 404
