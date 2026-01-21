from lambdas.process_document import _get_status, _is_meta_item


def test_is_meta_item_true():
    img = {"sk": {"S": "META#v1"}}
    assert _is_meta_item(img) is True


def test_is_meta_item_false():
    img = {"sk": {"S": "AUDIT#123"}}
    assert _is_meta_item(img) is False


def test_get_status_present():
    img = {"status": {"S": "UPLOADED"}}
    assert _get_status(img) == "UPLOADED"


def test_get_status_missing():
    img = {}
    assert _get_status(img) is None
