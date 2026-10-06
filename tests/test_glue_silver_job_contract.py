"""Static contract for the AWS Glue Silver job.

These tests deliberately avoid importing AWS Glue or PySpark locally.
"""

import ast
from pathlib import Path
import unittest


JOB_PATH = (
    Path(__file__).resolve().parents[1] / "jobs" / "aws" / "glue_silver_job.py"
)


class GlueSilverJobContractTests(unittest.TestCase):
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
            '"BRONZE_BUCKET"',
            '"SILVER_BUCKET"',
            '"BATCH_DATE"',
        ):
            self.assertIn(argument, self.source)

    def test_job_uses_deterministic_batch_paths(self):
        self.assertIn(
            'validated/jobs/',
            self.source,
        )
        self.assertIn(
            'source=*/batch_date={batch_date}/*.parquet',
            self.source,
        )
        self.assertIn(
            'jobs/source={source}/batch_date={batch_date}/',
            self.source,
        )

    def test_job_has_no_local_minio_or_hive_metastore_coupling(self):
        forbidden = (
            "MINIO_",
            "minio:9000",
            "hive-metastore",
            "s3a://",
            "access_key",
            "secret_key",
        )
        for value in forbidden:
            self.assertNotIn(value, self.source)

    def test_salary_patterns_are_real_regexes_not_double_escaped_literals(self):
        self.assertIn('r"usd|\\$"', self.source)
        self.assertIn('r"triệu|tr\\b|million"', self.source)
        self.assertIn('r"(\\d{1,3}', self.source)
        self.assertNotIn('r"(\\\\d{1,3}', self.source)

    def test_job_fails_closed_and_rewrites_only_one_batch_prefix(self):
        for marker in (
            "SILVER_EMPTY_INPUT",
            "SILVER_SOURCE_MISSING",
            "SILVER_EMPTY_OUTPUT",
            "SILVER_ROW_COUNT_MISMATCH",
            "delete_prefix(s3_client, silver_bucket, prefix)",
            'write.mode("append")',
            'print("SILVER_GLUE_JOB=PASS")',
            "raise",
        ):
            self.assertIn(marker, self.source)


if __name__ == "__main__":
    unittest.main()
