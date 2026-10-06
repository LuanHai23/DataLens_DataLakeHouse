#!/usr/bin/env python3
"""Validate and upload a locally crawled TopCV payload to Bronze S3.

TopCV blocks the AWS crawler at Cloudflare even when authenticated cookies are
injected correctly. This utility implements the approved hybrid ingestion
contract: crawl on a trusted local network, validate the resulting JSON, then
upload it to the same deterministic Bronze key used by the AWS pipeline.

The utility never reads browser cookies and never prints job payload contents.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable, Sequence


DEFAULT_REGION = "ap-southeast-1"
DEFAULT_BUCKET = "vnjobs-data-pipeline-bronze-ap-southeast-1-dev"
DEFAULT_EXPECTED_ACCOUNT = "490258149044"
SOURCE = "topcv"
MAX_INPUT_BYTES = 100 * 1024 * 1024
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Only JSON object keys are inspected. Job-description text values are never
# scanned or printed, avoiding false positives and accidental data disclosure.
SENSITIVE_KEY_FRAGMENTS = (
    "authorization",
    "cookie",
    "password",
    "secret_access_key",
    "session_token",
)


class UploadContractError(RuntimeError):
    """Raised when an input or AWS safety contract is not satisfied."""


def validate_batch_date(raw_value: str) -> str:
    """Return a strict ISO calendar date or raise UploadContractError."""

    if not DATE_PATTERN.fullmatch(raw_value):
        raise UploadContractError("BATCH_DATE_MUST_USE_YYYY_MM_DD")

    try:
        parsed = date.fromisoformat(raw_value)
    except ValueError as exc:
        raise UploadContractError("BATCH_DATE_IS_NOT_A_REAL_DATE") from exc

    if parsed.isoformat() != raw_value:
        raise UploadContractError("BATCH_DATE_IS_NOT_CANONICAL")

    return raw_value


def build_object_key(batch_date: str) -> str:
    """Build the deterministic Bronze key used by downstream Glue jobs."""

    validated_date = validate_batch_date(batch_date)
    return (
        f"raw/source={SOURCE}/batch_date={validated_date}/"
        f"{SOURCE}_jobs.json"
    )


def _find_sensitive_key(value: Any, path: str = "$") -> str | None:
    if isinstance(value, dict):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.casefold().replace("-", "_")
            if any(fragment in normalized for fragment in SENSITIVE_KEY_FRAGMENTS):
                return f"{path}.{key}"

            found = _find_sensitive_key(child, f"{path}.{key}")
            if found is not None:
                return found

    elif isinstance(value, list):
        for index, child in enumerate(value):
            found = _find_sensitive_key(child, f"{path}[{index}]")
            if found is not None:
                return found

    return None


def load_and_validate_payload(input_path: Path) -> tuple[bytes, int, str]:
    """Validate a TopCV JSON array and return canonical bytes and metadata."""

    if not input_path.is_file():
        raise UploadContractError("INPUT_FILE_NOT_FOUND")

    input_size = input_path.stat().st_size
    if input_size <= 0:
        raise UploadContractError("INPUT_FILE_IS_EMPTY")
    if input_size > MAX_INPUT_BYTES:
        raise UploadContractError("INPUT_FILE_EXCEEDS_100_MIB")

    try:
        text = input_path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadContractError("INPUT_FILE_IS_NOT_UTF8_JSON") from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise UploadContractError("INPUT_FILE_CONTAINS_INVALID_JSON") from exc

    if not isinstance(payload, list):
        raise UploadContractError("TOPCV_PAYLOAD_MUST_BE_A_JSON_ARRAY")
    if not payload:
        raise UploadContractError("TOPCV_PAYLOAD_MUST_NOT_BE_EMPTY")

    for index, record in enumerate(payload):
        if not isinstance(record, dict) or not record:
            raise UploadContractError(
                f"TOPCV_RECORD_MUST_BE_NONEMPTY_OBJECT:index={index}"
            )

    sensitive_path = _find_sensitive_key(payload)
    if sensitive_path is not None:
        raise UploadContractError(
            f"SENSITIVE_KEY_FOUND_IN_JOB_PAYLOAD:path={sensitive_path}"
        )

    canonical_bytes = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    payload_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    return canonical_bytes, len(payload), payload_sha256


def create_boto3_session(profile: str | None, region: str) -> Any:
    """Create the AWS session lazily so dry-run works without boto3."""

    try:
        import boto3
    except ImportError as exc:
        raise UploadContractError(
            "BOTO3_NOT_INSTALLED: install project AWS dependencies"
        ) from exc

    kwargs: dict[str, str] = {"region_name": region}
    if profile:
        kwargs["profile_name"] = profile

    return boto3.Session(**kwargs)


def _is_missing_object_error(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    if not isinstance(response, dict):
        return False

    error = response.get("Error", {})
    if not isinstance(error, dict):
        return False

    return str(error.get("Code", "")) in {
        "404",
        "NoSuchKey",
        "NotFound",
    }


def _head_object_if_present(s3_client: Any, bucket: str, key: str) -> Any | None:
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # boto3 exception type is loaded only at runtime
        if _is_missing_object_error(exc):
            return None
        raise UploadContractError("S3_HEAD_OBJECT_FAILED") from exc


def upload_payload(
    *,
    payload_bytes: bytes,
    record_count: int,
    payload_sha256: str,
    batch_date: str,
    bucket: str,
    region: str,
    expected_account: str,
    profile: str | None,
    allow_new_version: bool,
    session_factory: Callable[[str | None, str], Any] = create_boto3_session,
) -> dict[str, Any]:
    """Verify AWS safety contracts, upload, and verify the stored object."""

    object_key = build_object_key(batch_date)
    session = session_factory(profile, region)

    sts = session.client("sts", region_name=region)
    identity = sts.get_caller_identity()
    actual_account = str(identity.get("Account", ""))
    if actual_account != expected_account:
        raise UploadContractError(
            f"AWS_ACCOUNT_MISMATCH:expected={expected_account}:actual={actual_account}"
        )

    s3 = session.client("s3", region_name=region)
    versioning = s3.get_bucket_versioning(Bucket=bucket)
    if versioning.get("Status") != "Enabled":
        raise UploadContractError("S3_BUCKET_VERSIONING_NOT_ENABLED")

    existing = _head_object_if_present(s3, bucket, object_key)
    if existing is not None:
        existing_metadata = existing.get("Metadata", {})
        existing_sha256 = str(existing_metadata.get("payload-sha256", ""))

        if existing_sha256 == payload_sha256:
            return {
                "status": "SKIPPED_IDENTICAL",
                "account": actual_account,
                "bucket": bucket,
                "key": object_key,
                "record_count": record_count,
                "payload_sha256": payload_sha256,
                "version_id": str(existing.get("VersionId", "")),
                "size": int(existing.get("ContentLength", len(payload_bytes))),
            }

        if not allow_new_version:
            raise UploadContractError(
                "S3_KEY_ALREADY_EXISTS_WITH_DIFFERENT_PAYLOAD:"
                "rerun_with_--allow-new-version_after_review"
            )

    response = s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=payload_bytes,
        ContentType="application/json",
        ServerSideEncryption="AES256",
        Metadata={
            "batch-date": batch_date,
            "ingestion-mode": "hybrid-local",
            "payload-sha256": payload_sha256,
            "record-count": str(record_count),
            "source": SOURCE,
        },
    )

    version_id = str(response.get("VersionId", ""))
    if not version_id or version_id == "null":
        raise UploadContractError("S3_UPLOAD_DID_NOT_RETURN_VERSION_ID")

    stored = s3.head_object(
        Bucket=bucket,
        Key=object_key,
        VersionId=version_id,
    )

    if int(stored.get("ContentLength", -1)) != len(payload_bytes):
        raise UploadContractError("S3_STORED_SIZE_MISMATCH")

    stored_metadata = stored.get("Metadata", {})
    if stored_metadata.get("payload-sha256") != payload_sha256:
        raise UploadContractError("S3_STORED_SHA256_METADATA_MISMATCH")

    return {
        "status": "UPLOADED",
        "account": actual_account,
        "bucket": bucket,
        "key": object_key,
        "record_count": record_count,
        "payload_sha256": payload_sha256,
        "version_id": version_id,
        "size": len(payload_bytes),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and upload locally crawled TopCV jobs to the versioned "
            "AWS Bronze S3 contract."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to a local topcv_jobs.json file.",
    )
    parser.add_argument(
        "--batch-date",
        required=True,
        help="Pipeline batch date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET", DEFAULT_BUCKET),
        help="Bronze S3 bucket. Defaults to S3_BUCKET or the dev bucket.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", DEFAULT_REGION),
        help="AWS region. Defaults to AWS_REGION or ap-southeast-1.",
    )
    parser.add_argument(
        "--expected-account",
        default=os.getenv("AWS_ACCOUNT_ID", DEFAULT_EXPECTED_ACCOUNT),
        help="Required AWS account guard.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Optional local AWS named profile.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate locally and print safe metadata without calling AWS.",
    )
    parser.add_argument(
        "--allow-new-version",
        action="store_true",
        help=(
            "Allow a reviewed payload to create a new S3 version when the "
            "deterministic key already contains different data."
        ),
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    batch_date = validate_batch_date(args.batch_date)
    object_key = build_object_key(batch_date)
    payload_bytes, record_count, payload_sha256 = load_and_validate_payload(
        args.input
    )

    print("TOPCV_MANUAL_INPUT_VALIDATION=PASS")
    print(f"RECORD_COUNT={record_count}")
    print(f"PAYLOAD_BYTES={len(payload_bytes)}")
    print(f"PAYLOAD_SHA256={payload_sha256}")
    print(f"TARGET_BUCKET={args.bucket}")
    print(f"TARGET_KEY={object_key}")
    print("COOKIE_DATA_READ=NO")
    print("JOB_PAYLOAD_PRINTED=NO")

    if args.dry_run:
        print("AWS_MUTATION_PERFORMED=NO")
        print("TOPCV_MANUAL_UPLOAD_DRY_RUN=PASS")
        return 0

    result = upload_payload(
        payload_bytes=payload_bytes,
        record_count=record_count,
        payload_sha256=payload_sha256,
        batch_date=batch_date,
        bucket=args.bucket,
        region=args.region,
        expected_account=args.expected_account,
        profile=args.profile,
        allow_new_version=args.allow_new_version,
    )

    print("AWS_ACCOUNT_CHECK=PASS")
    print("S3_VERSIONING_CHECK=PASS")
    print(f"UPLOAD_STATUS={result['status']}")
    print(f"S3_URI=s3://{result['bucket']}/{result['key']}")
    print(f"S3_VERSION_ID={result['version_id']}")
    print(f"STORED_BYTES={result['size']}")
    print("S3_OBJECT_VERIFICATION=PASS")
    print("TOPCV_MANUAL_UPLOAD=PASS")
    return 0


def main() -> int:
    try:
        return run()
    except UploadContractError as exc:
        print(f"TOPCV_MANUAL_UPLOAD_ERROR={exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("TOPCV_MANUAL_UPLOAD_ERROR=INTERRUPTED", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
