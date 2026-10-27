import csv
import gzip
import importlib.util
import json
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location(
    "import_history", Path(__file__).parents[1] / "scripts/import_history.py"
)
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)


def source(tmp_path, rows):
    path = tmp_path / "fixture.csv.gz"
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(history.FIELDS)
        writer.writerows(rows)
    return path


def row(day="01", kind="view", price="130.76"):
    return [
        f"2019-10-{day} 00:01:46 UTC",
        kind,
        "1004856",
        "2053013555631882655",
        "electronics.smartphone",
        "samsung",
        price,
        "543272936",
        "session-a",
    ]


def test_reconcile_partition_quarantine_and_idempotence(tmp_path):
    root = tmp_path / "history"
    root.mkdir()
    path = source(tmp_path, [row(), row(kind="purchase"), row(day="02"), row(price="NaN")])
    report = history.convert(path, "2019-Oct", root, 0, batch_size=2)
    assert report["source_rows"] == 4
    assert report["accepted_rows"] == 3
    assert report["rejected_rows"] == 1
    assert report["event_types"] == {"view": 2, "purchase": 1}
    assert len(list((root / "months/2019-Oct/events").glob("event_date=*"))) == 2
    assert history.convert(path, "2019-Oct", root, 0) == report
    assert json.loads((root / "months/2019-Oct/rejected.jsonl").read_text())["source_row"] == 4


def test_corrupt_gzip_never_published(tmp_path):
    root = tmp_path / "history"
    root.mkdir()
    path = source(tmp_path, [row()])
    path.write_bytes(path.read_bytes()[:-5])
    with pytest.raises(EOFError):
        history.convert(path, "2019-Oct", root, 0)
    assert not (root / "months/2019-Oct").exists()
    assert not (root / "import.lock").exists()


def test_space_guard(tmp_path):
    with pytest.raises(RuntimeError, match="Stopped safely"):
        history.space(tmp_path, 10**20)


def test_distinct_month_identity_and_original_time():
    a = history.normalized(dict(zip(history.FIELDS, row())), "2019-Oct", 1)
    values = row()
    values[0] = "2019-11-01 00:01:46 UTC"
    b = history.normalized(dict(zip(history.FIELDS, values)), "2019-Nov", 1)
    assert a["event_id"] != b["event_id"]
    assert a["event_time"].year == 2019
    assert a["price_minor"] == 13076


def test_wrong_month_rejected():
    with pytest.raises(ValueError, match="outside requested month"):
        history.normalized(dict(zip(history.FIELDS, row())), "2019-Nov", 1)


def test_zip_archive_crc_and_members(tmp_path):
    import io
    import zipfile

    root = tmp_path / "history"
    root.mkdir()
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(history.FIELDS)
    writer.writerow(row())
    path = tmp_path / "fixture.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("2019-Oct.csv", buffer.getvalue())
    result = history.convert(path, "2019-Oct", root, 0)
    assert result["archive_crc_verified"]
    assert result["source_url"] is None
    assert "unverified" in result["coverage"]


def test_download_resume_requires_exact_range(tmp_path, monkeypatch):
    import io

    target = tmp_path / "2019-Oct.csv.gz.partial"
    target.write_bytes(b"abc")
    (tmp_path / "2019-Oct.csv.gz.download.json").write_text(json.dumps({"etag": '"stable"'}))

    class Response(io.BytesIO):
        status = 206
        headers = {"Content-Range": "bytes 2-5/6", "ETag": '"stable"', "Content-Length": "3"}

    monkeypatch.setattr(history.urllib.request, "urlopen", lambda *args, **kwargs: Response(b"def"))
    with pytest.raises(RuntimeError, match="inconsistent resume"):
        history.download("2019-Oct", tmp_path, 0)
    assert target.read_bytes() == b"abc"


def test_changed_archive_restarts_download(tmp_path, monkeypatch):
    import io

    target = tmp_path / "2019-Oct.csv.gz.partial"
    target.write_bytes(b"old")
    (tmp_path / "2019-Oct.csv.gz.download.json").write_text(json.dumps({"etag": '"old"'}))

    class Response(io.BytesIO):
        status = 200
        headers = {"ETag": '"new"', "Content-Length": "3"}

    monkeypatch.setattr(history.urllib.request, "urlopen", lambda *args, **kwargs: Response(b"new"))
    result = history.download("2019-Oct", tmp_path, 0)
    assert result.read_bytes() == b"new"
