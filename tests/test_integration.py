# tests/test_integration.py
import subprocess
import csv
import tempfile
import os

def test_end_to_end_local(tmp_path):
    src = tmp_path / "input.csv"
    tgt = tmp_path / "out.csv"
    rows = [
        {"id":"1","name":"A","amount":"500"},
        {"id":"2","name":"B","amount":"700"},
        {"id":"3","name":"C","amount":"1000"},
    ]
    with open(src, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["id","name","amount"])
        writer.writeheader()
        writer.writerows(rows)

    # run the local mode
    cmd = ["python", "scripts/glue_filter_job.py", "--mode", "local",
           "--source", str(src), "--target", str(tgt), "--amount_threshold", "600"]
    subprocess.check_call(cmd)

    # validate output
    with open(tgt, newline='') as f:
        reader = csv.DictReader(f)
        outs = list(reader)
    assert len(outs) == 2
    assert outs[0]["id"] in ("2","3")


