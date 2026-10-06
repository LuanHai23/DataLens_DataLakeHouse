import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "aws" / "upload_topcv_raw.py"


def load_module():
    spec = importlib.util.spec_from_file_location("upload_topcv_raw", SCRIPT)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_payload(tmp_path: Path, payload) -> Path:
    target = tmp_path / "topcv_jobs.json"
    target.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    return target


def test_manual_uploader_exists_and_has_no_credential_literals():
    assert SCRIPT.is_file()

    source = SCRIPT.read_text(encoding="utf-8")
    lowered = source.casefold()

    assert "get_secret_value" not in lowered
    assert "aws_secret_access_key=" not in lowered
    assert "aws_access_key_id=" not in lowered
    assert "topcv_coookies_json" not in lowered
    assert "topcv_cookies_json" not in lowered


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("2026-09-30", "2026-09-30"),
        ("2024-02-29", "2024-02-29"),
    ],
)
def test_validate_batch_date_accepts_real_iso_dates(raw_value, expected):
    module = load_module()
    assert module.validate_batch_date(raw_value) == expected


@pytest.mark.parametrize(
    "raw_value",
    [
        "2026-9-30",
        "30-09-2026",
        "2026-02-30",
        "",
    ],
)
def test_validate_batch_date_rejects_invalid_values(raw_value):
    module = load_module()

    with pytest.raises(module.UploadContractError):
        module.validate_batch_date(raw_value)


def test_build_object_key_matches_bronze_contract():
    module = load_module()

    assert module.build_object_key("2026-09-30") == (
        "raw/source=topcv/batch_date=2026-09-30/topcv_jobs.json"
    )


def test_payload_validation_returns_canonical_metadata(tmp_path):
    module = load_module()
    input_path = write_payload(
        tmp_path,
        [
            {
                "title": "Data Engineer",
                "company": "Example A",
                "location": "Ho Chi Minh City",
            },
            {
                "title": "Data Analyst",
                "company": "Example B",
                "location": "Ha Noi",
            },
        ],
    )

    payload_bytes, record_count, payload_sha256 = (
        module.load_and_validate_payload(input_path)
    )

    assert record_count == 2
    assert len(payload_sha256) == 64
    assert payload_bytes.endswith(b"\n")
    assert len(json.loads(payload_bytes.decode("utf-8"))) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        ["not-an-object"],
        [{}],
    ],
)
def test_payload_validation_rejects_invalid_top_level_contract(
    tmp_path,
    payload,
):
    module = load_module()
    input_path = write_payload(tmp_path, payload)

    with pytest.raises(module.UploadContractError):
        module.load_and_validate_payload(input_path)


@pytest.mark.parametrize(
    "sensitive_key",
    [
        "cookie",
        "cookies",
        "session_token",
        "password",
        "Authorization",
    ],
)
def test_payload_validation_rejects_sensitive_object_keys(
    tmp_path,
    sensitive_key,
):
    module = load_module()
    input_path = write_payload(
        tmp_path,
        [
            {
                "title": "Data Engineer",
                "debug": {sensitive_key: "must-not-be-uploaded"},
            }
        ],
    )

    with pytest.raises(
        module.UploadContractError,
        match="SENSITIVE_KEY_FOUND_IN_JOB_PAYLOAD",
    ):
        module.load_and_validate_payload(input_path)


def test_dry_run_needs_no_aws_session_and_prints_no_payload(tmp_path):
    input_path = write_payload(
        tmp_path,
        [
            {
                "title": "PRIVATE_TEST_TITLE_MUST_NOT_BE_PRINTED",
                "company": "PRIVATE_TEST_COMPANY_MUST_NOT_BE_PRINTED",
            }
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--input",
            str(input_path),
            "--batch-date",
            "2026-09-30",
            "--dry-run",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    assert "TOPCV_MANUAL_INPUT_VALIDATION=PASS" in result.stdout
    assert "AWS_MUTATION_PERFORMED=NO" in result.stdout
    assert "TOPCV_MANUAL_UPLOAD_DRY_RUN=PASS" in result.stdout
    assert "PRIVATE_TEST_TITLE_MUST_NOT_BE_PRINTED" not in result.stdout
    assert "PRIVATE_TEST_COMPANY_MUST_NOT_BE_PRINTED" not in result.stdout


class FakeStsClient:
    def get_caller_identity(self):
        return {"Account": "490258149044"}


class FakeS3Client:
    def __init__(self):
        self.body = None
        self.metadata = None
        self.version_id = "test-version-1"

    def get_bucket_versioning(self, *, Bucket):
        assert Bucket == "test-bronze-bucket"
        return {"Status": "Enabled"}

    def head_object(self, *, Bucket, Key, VersionId=None):
        assert Bucket == "test-bronze-bucket"
        assert Key == (
            "raw/source=topcv/batch_date=2026-09-30/topcv_jobs.json"
        )

        if VersionId is None:
            error = RuntimeError("not found")
            error.response = {"Error": {"Code": "404"}}
            raise error

        assert VersionId == self.version_id
        return {
            "ContentLength": len(self.body),
            "Metadata": self.metadata,
            "VersionId": self.version_id,
        }

    def put_object(self, **kwargs):
        assert kwargs["ContentType"] == "application/json"
        assert kwargs["ServerSideEncryption"] == "AES256"

        self.body = kwargs["Body"]
        self.metadata = kwargs["Metadata"]
        return {
            "VersionId": self.version_id,
            "ETag": '"test-etag"',
        }


class FakeSession:
    def __init__(self):
        self.s3 = FakeS3Client()

    def client(self, service_name, region_name=None):
        assert region_name == "ap-southeast-1"
        if service_name == "sts":
            return FakeStsClient()
        if service_name == "s3":
            return self.s3
        raise AssertionError(f"unexpected service: {service_name}")


def test_upload_contract_checks_account_versioning_and_stored_object(
    tmp_path,
):
    module = load_module()
    input_path = write_payload(
        tmp_path,
        [{"title": "Data Engineer", "company": "Example"}],
    )
    payload_bytes, record_count, payload_sha256 = (
        module.load_and_validate_payload(input_path)
    )
    fake_session = FakeSession()

    result = module.upload_payload(
        payload_bytes=payload_bytes,
        record_count=record_count,
        payload_sha256=payload_sha256,
        batch_date="2026-09-30",
        bucket="test-bronze-bucket",
        region="ap-southeast-1",
        expected_account="490258149044",
        profile=None,
        allow_new_version=False,
        session_factory=lambda profile, region: fake_session,
    )

    assert result["status"] == "UPLOADED"
    assert result["version_id"] == "test-version-1"
    assert fake_session.s3.metadata["source"] == "topcv"
    assert fake_session.s3.metadata["ingestion-mode"] == "hybrid-local"
    assert fake_session.s3.metadata["payload-sha256"] == payload_sha256
