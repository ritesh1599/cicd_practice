# etl/transform.py
from typing import List


def filter_records(records: List[dict], amount_threshold: int = 600) -> List[dict]:
    """
    records: list of dicts with keys 'id','name','amount' (amount as str or int)
    returns filtered list where amount > amount_threshold
    Pure function used for unit tests.
    """
    out = []
    for r in records:
        try:
            amt = int(r.get("amount", 0))
        except (ValueError, TypeError):
            continue
        if amt > amount_threshold:
            out.append(r)
    return out
