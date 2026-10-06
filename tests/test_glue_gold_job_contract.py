"""Static contract for the AWS Glue Gold job.

These tests deliberately avoid importing AWS Glue or PySpark locally.
"""

import ast
from pathlib import Path
import unittest


JOB_PATH = Path(__file__).resolve().parents[1] / "jobs" / "aws" / "glue_gold_job.py"


class GlueGoldJobContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = JOB_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source, filename=str(JOB_PATH))

    def test_job_script_is_tracked_at_expected_path_and_compiles(self):
        self.assertTrue(JOB_PATH.is_file())
        compile(self.source, str(JOB_PATH), "exec")

    def test_job_requires_batch_and_both_bucket_arguments(self):
        for argument in (
            '"JOB_NAME"',
            '"SILVER_BUCKET"',
            '"GOLD_BUCKET"',
            '"BATCH_DATE"',
        ):
            self.assertIn(argument, self.source)

    def test_job_uses_deterministic_batch_paths(self):
        for path_fragment in (
            "jobs/",
            "source=*/batch_date={batch_date}/*.parquet",
            "marts/{mart_name}/batch_date={batch_date}/",
        ):
            self.assertIn(path_fragment, self.source)
        self.assertNotIn("current_date(", self.source)

    def test_job_publishes_only_the_small_mvp_mart_set(self):
        for mart_name in (
            "job_market_overview",
            "source_performance",
            "location_summary",
        ):
            self.assertIn(f'"{mart_name}"', self.source)
        for unnecessary_dependency in (
            "mart_skill_demand",
            "daily_alerts",
            "PostgreSQL",
            "Iceberg",
        ):
            self.assertNotIn(unnecessary_dependency, self.source)

    def test_job_has_no_local_runtime_coupling_or_embedded_credentials(self):
        forbidden = (
            "MINIO_",
            "minio:9000",
            "hive-metastore",
            "s3a://",
            "access_key",
            "secret_key",
            "jdbc:postgresql",
        )
        for value in forbidden:
            self.assertNotIn(value, self.source)

    def test_job_fails_closed_and_rewrites_only_each_mart_batch(self):
        for marker in (
            "GOLD_EMPTY_INPUT",
            "GOLD_SOURCE_MISSING",
            "GOLD_MART_EMPTY",
            "GOLD_INPUT_TOTAL_MISMATCH",
            "delete_prefix(s3_client, gold_bucket, prefix)",
            '.write.mode("append")',
            'print("GOLD_GLUE_JOB=PASS")',
            "raise",
        ):
            self.assertIn(marker, self.source)


if __name__ == "__main__":
    unittest.main()
