import json
import io
import time
from datetime import date, datetime
from dotenv import load_dotenv
import os
from pathlib import Path
from urllib.parse import urlparse

load_dotenv()


def _default_minio_endpoint():
    """Use the Docker service name in containers and localhost on the host."""
    return "minio:9000" if Path("/.dockerenv").exists() else "localhost:9000"


def _as_bool(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_endpoint(endpoint, secure=None):
    """Return the ``host:port`` format expected by the MinIO Python SDK."""
    raw_endpoint = (endpoint or _default_minio_endpoint()).strip().rstrip("/")
    if not raw_endpoint:
        raise ValueError("MINIO_ENDPOINT must not be empty")

    endpoint_secure = secure
    if "://" in raw_endpoint:
        parsed = urlparse(raw_endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(
                "MINIO_ENDPOINT must look like 'minio:9000' or "
                "'http://localhost:9000'"
            )
        if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
            raise ValueError("MINIO_ENDPOINT must not contain a path, query, or fragment")
        raw_endpoint = parsed.netloc
        if endpoint_secure is None:
            endpoint_secure = parsed.scheme == "https"

    if endpoint_secure is None:
        endpoint_secure = _as_bool(os.getenv("MINIO_SECURE", "false"))

    return raw_endpoint, endpoint_secure


# Host process:    MINIO_ENDPOINT=localhost:9000
# Docker process:  MINIO_ENDPOINT=minio:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", _default_minio_endpoint())
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data-lake")


def _resolve_batch_date(value=None):
    raw_value = value or os.getenv("BATCH_DATE") or date.today().isoformat()
    try:
        return date.fromisoformat(str(raw_value).strip()).isoformat()
    except ValueError as exc:
        raise ValueError("BATCH_DATE must use YYYY-MM-DD") from exc


class MiniOIngestion:
    def __init__(
        self,
        endpoint=None,
        access_key=None,
        secret_key=None,
        bucket_name=None,
        secure=None,
        backend=None,
        region=None,
        raw_prefix=None,
        client=None,
    ):
        self.backend = (backend or os.getenv("STORAGE_BACKEND", "minio")).strip().lower()
        if self.backend not in {"minio", "s3"}:
            raise ValueError("STORAGE_BACKEND must be 'minio' or 's3'")

        if self.backend == "s3":
            self.bucket_name = (
                bucket_name
                or os.getenv("S3_BUCKET")
                or os.getenv("BRONZE_BUCKET")
                or ""
            ).strip()
            if not self.bucket_name:
                raise ValueError("S3_BUCKET or BRONZE_BUCKET is required for S3 storage")

            self.raw_prefix = (raw_prefix or os.getenv("S3_RAW_PREFIX", "raw")).strip("/")
            if client is None:
                import boto3

                client = boto3.client(
                    "s3",
                    region_name=(
                        region
                        or os.getenv("AWS_REGION")
                        or os.getenv("AWS_DEFAULT_REGION")
                        or "ap-southeast-1"
                    ),
                )
            self.client = client
            return

        endpoint, secure = _normalize_endpoint(endpoint or MINIO_ENDPOINT, secure)
        self.endpoint = endpoint
        self.secure = secure
        if client is None:
            from minio import Minio

            client = Minio(
                endpoint,
                access_key=access_key or MINIO_ACCESS_KEY,
                secret_key=secret_key or MINIO_SECRET_KEY,
                secure=secure,
            )
        self.client = client
        self.bucket_name = bucket_name or MINIO_BUCKET
        self._ensure_bucket()

    def _ensure_bucket(self):
        attempts = max(1, int(os.getenv("MINIO_CONNECT_RETRIES", "5")))
        delay_seconds = max(0.0, float(os.getenv("MINIO_RETRY_DELAY_SECONDS", "2")))
        scheme = "https" if self.secure else "http"

        for attempt in range(1, attempts + 1):
            try:
                if not self.client.bucket_exists(self.bucket_name):
                    self.client.make_bucket(self.bucket_name)
                    print(f"Bucket '{self.bucket_name}' created.")
                return
            except Exception as exc:
                if attempt == attempts:
                    raise ConnectionError(
                        f"Cannot connect to the MinIO S3 API at "
                        f"{scheme}://{self.endpoint} after {attempts} attempts. "
                        "Use port 9000 (not the Console port 9001) and, when "
                        "running in Docker, use the MinIO service name such as "
                        "'minio:9000'."
                    ) from exc

                print(
                    f"MinIO is not ready at {scheme}://{self.endpoint} "
                    f"(attempt {attempt}/{attempts}); retrying in "
                    f"{delay_seconds:g}s..."
                )
                time.sleep(delay_seconds)

    def upload_file(self, local_path, object_name, content_type="application/json"):
        if self.backend == "s3":
            self.client.upload_file(
                str(local_path),
                self.bucket_name,
                object_name,
                ExtraArgs={"ContentType": content_type},
            )
        else:
            self.client.fput_object(
                self.bucket_name,
                object_name,
                str(local_path),
                content_type=content_type,
            )
        print(f"Uploaded → s3://{self.bucket_name}/{object_name}")
        return object_name

    def upload_jobs(self, source, jobs_list, batch_date=None):
        if not jobs_list:
            raise ValueError(f"{source} crawler returned no jobs")

        batch_date = _resolve_batch_date(batch_date)
        now = datetime.now()
        if self.backend == "s3":
            object_name = (
                f"{self.raw_prefix}/source={source}/"
                f"batch_date={batch_date}/{source}_jobs.json"
            )
        else:
            object_name = f"{source}/{batch_date}/{source}_jobs.json"

        payload = {
            "source": source,
            "batch_date": batch_date,
            "scraped_at": now.isoformat(),
            "total": len(jobs_list),
            "jobs": jobs_list,
        }

        data_bytes  = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        data_stream = io.BytesIO(data_bytes)

        if self.backend == "s3":
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=data_bytes,
                ContentType="application/json",
            )
        else:
            self.client.put_object(
                self.bucket_name,
                object_name,
                data_stream,
                length=len(data_bytes),
                content_type="application/json"
            )

        print(f"☁️  Uploaded {len(jobs_list)} records → {self.bucket_name}/{object_name}")
        return object_name
