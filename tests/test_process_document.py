from lambdas.process_document import _get_str_attr, _is_meta_item


def test_is_meta_item_true():
    img = {"sk": {"S": "META#v1"}}
    assert _is_meta_item(img) is True


def test_is_meta_item_false():
    img = {"sk": {"S": "AUDIT#123"}}
    assert _is_meta_item(img) is False


def test_get_str_attr_present():
    img = {"status": {"S": "UPLOADED"}}
    assert _get_str_attr(img, "status") == "UPLOADED"


def test_get_str_attr_missing():
    img = {}
    assert _get_str_attr(img, "status") is None


def test_get_str_attr_wrong_type():
    img = {"status": {"N": "1"}}
    assert _get_str_attr(img, "status") is None
