from minio import Minio
import json
import io
import time
from datetime import datetime
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


class MiniOIngestion:
    def __init__(
        self,
        endpoint=None,
        access_key=None,
        secret_key=None,
        bucket_name=None,
        secure=None,
    ):
        endpoint, secure = _normalize_endpoint(endpoint or MINIO_ENDPOINT, secure)
        self.endpoint = endpoint
        self.secure = secure
        self.client = Minio(
            endpoint,
            access_key=access_key or MINIO_ACCESS_KEY,
            secret_key=secret_key or MINIO_SECRET_KEY,
            secure=secure,
        )
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
        self.client.fput_object(
            self.bucket_name,
            object_name,
            str(local_path),
            content_type=content_type,
        )
        print(f"Uploaded → s3://{self.bucket_name}/{object_name}")

    def upload_jobs(self, source, jobs_list):
        now = datetime.now()
        object_name = f"{source}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"

        payload = {
            "source": source,
            "scraped_at": now.isoformat(),
            "total": len(jobs_list),
            "jobs": jobs_list,
        }

        data_bytes  = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        data_stream = io.BytesIO(data_bytes)

        try:
            self.client.put_object(
                self.bucket_name,
                object_name,
                data_stream,
                length=len(data_bytes),
                content_type="application/json"
            )
            print(f"☁️  Uploaded {len(jobs_list)} records → {self.bucket_name}/{object_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to upload: {e}")
            return False
