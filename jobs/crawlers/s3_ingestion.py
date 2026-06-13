from minio import Minio
import json
import io
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

class MiniOIngestion:
    def __init__(self):
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        self.bucket_name = "data-lake"
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            print(f"Bucket '{self.bucket_name}' created.")

    def upload_jobs(self, source, jobs_list):
        now = datetime.now()
        object_name = f"{source}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"

        # Wrap giống itviec format: {"source": ..., "jobs": [...]}
        # Bronze flatten_json sẽ xử lý đúng cả 2 nguồn
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
        except Exception as e:
            print(f"❌ Failed to upload: {e}")