import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error


def _default_endpoint():
    """Use Docker DNS from a container and localhost when run on the host."""
    return "minio:9000" if Path("/.dockerenv").exists() else "localhost:9000"


def _minio_config():
    """Return endpoint and TLS setting in the format expected by MinIO SDK."""
    raw_endpoint = os.getenv("MINIO_ENDPOINT", _default_endpoint()).strip().rstrip("/")
    if not raw_endpoint:
        raise ValueError("MINIO_ENDPOINT must not be empty")

    secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }
    if "://" in raw_endpoint:
        parsed = urlparse(raw_endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("MINIO_ENDPOINT must be a MinIO host and port")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("MINIO_ENDPOINT must not include a path or query")
        raw_endpoint = parsed.netloc
        secure = parsed.scheme == "https"

    return raw_endpoint, secure


class BronzeIngestion:
    def __init__(self):
        endpoint, secure = _minio_config()
        self.source_bucket = os.getenv("RAW_BUCKET", "data-lake").strip()
        self.bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze").strip()
        self.source_prefix = os.getenv("RAW_PREFIX", "").strip().strip("/")

        if not self.source_bucket or not self.bronze_bucket:
            raise ValueError("RAW_BUCKET and BRONZE_BUCKET must not be empty")

        self.client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin").strip(),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password").strip(),
            secure=secure,
        )

    def _ensure_buckets(self):
        if not self.client.bucket_exists(self.source_bucket):
            raise RuntimeError(f"Source bucket '{self.source_bucket}' does not exist")

        if not self.client.bucket_exists(self.bronze_bucket):
            self.client.make_bucket(self.bronze_bucket)
            print(f"Created Bronze bucket: {self.bronze_bucket}")

    def _is_current_copy(self, source_object):
        """Skip a Bronze object only when it has the same source ETag."""
        try:
            bronze_object = self.client.stat_object(self.bronze_bucket, source_object.object_name)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                return False
            raise
        return bronze_object.etag == source_object.etag

    def copy_raw_json_to_bronze(self):
        """Copy new or changed raw JSON objects and return summary counters."""
        self._ensure_buckets()
        prefix = f"{self.source_prefix}/" if self.source_prefix else None
        copied = skipped = failed = 0

        print(
            f"Copying JSON objects: s3://{self.source_bucket}/{self.source_prefix or ''} "
            f"-> s3://{self.bronze_bucket}/"
        )
        for source_object in self.client.list_objects(
            self.source_bucket, prefix=prefix, recursive=True
        ):
            if not source_object.object_name.lower().endswith(".json"):
                continue

            try:
                if self._is_current_copy(source_object):
                    skipped += 1
                    continue

                self.client.copy_object(
                    self.bronze_bucket,
                    source_object.object_name,
                    CopySource(self.source_bucket, source_object.object_name),
                )
                copied += 1
                print(
                    f"Copied s3://{self.source_bucket}/{source_object.object_name} "
                    f"-> s3://{self.bronze_bucket}/{source_object.object_name}"
                )
            except Exception as exc:
                failed += 1
                print(f"Failed to copy '{source_object.object_name}': {exc}")

        print(f"Bronze ingestion complete: copied={copied}, skipped={skipped}, failed={failed}")
        return copied, skipped, failed


def main():
    try:
        _, _, failed = BronzeIngestion().copy_raw_json_to_bronze()
    except Exception as exc:
        print(f"Bronze ingestion failed: {exc}", file=sys.stderr)
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())