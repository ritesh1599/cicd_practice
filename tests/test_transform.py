# tests/test_transform.py
from etl.transform import filter_records


def test_filter_basic():
    data = [
        {"id": "1", "name": "A", "amount": "500"},
        {"id": "2", "name": "B", "amount": "700"},
        {"id": "3", "name": "C", "amount": "1000"},
    ]
    out = filter_records(data, amount_threshold=600)
    ids = [r["id"] for r in out]
    assert ids == ["2", "3"]


def test_filter_invalid_amounts():
    data = [
        {"id": "1", "name": "A", "amount": "abc"},
        {"id": "2", "name": "B", "amount": None},
        {"id": "3", "name": "C", "amount": "800"},
    ]
    out = filter_records(data, amount_threshold=600)
    assert len(out) == 1
    assert out[0]["id"] == "3"
