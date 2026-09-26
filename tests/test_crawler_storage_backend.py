import importlib.util
import json
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "jobs"
    / "crawlers"
    / "s3_ingestion.py"
)


def load_storage_module():
    spec = importlib.util.spec_from_file_location("crawler_storage", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


class FakeMinioClient:
    def __init__(self):
        self.calls = []

    def bucket_exists(self, _bucket):
        return True

    def put_object(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_s3_upload_matches_glue_raw_prefix():
    module = load_storage_module()
    client = FakeS3Client()
    storage = module.MiniOIngestion(
        backend="s3",
        bucket_name="bronze-bucket",
        client=client,
    )

    key = storage.upload_jobs(
        "itviec",
        [{"title": "Data Engineer", "url": "https://example.test/1"}],
        "2026-09-25",
    )

    assert key == "raw/source=itviec/batch_date=2026-09-25/itviec_jobs.json"
    assert client.calls[0]["Bucket"] == "bronze-bucket"
    assert client.calls[0]["Key"] == key
    payload = json.loads(client.calls[0]["Body"].decode("utf-8"))
    assert payload["source"] == "itviec"
    assert payload["batch_date"] == "2026-09-25"
    assert payload["total"] == 1


def test_same_s3_batch_uses_same_key():
    module = load_storage_module()
    client = FakeS3Client()
    storage = module.MiniOIngestion(
        backend="s3",
        bucket_name="bronze-bucket",
        client=client,
    )

    first = storage.upload_jobs("topcv", [{"title": "A"}], "2026-09-25")
    second = storage.upload_jobs("topcv", [{"title": "B"}], "2026-09-25")

    assert first == second


def test_minio_layout_stays_compatible():
    module = load_storage_module()
    client = FakeMinioClient()
    storage = module.MiniOIngestion(
        backend="minio",
        endpoint="minio:9000",
        bucket_name="data-lake",
        client=client,
    )

    key = storage.upload_jobs("topcv", [{"title": "A"}], "2026-09-25")

    assert key == "topcv/2026-09-25/topcv_jobs.json"
    assert client.calls[0][0][0] == "data-lake"
    assert client.calls[0][0][1] == key
