"""AWS Glue Gold job for the VNJobs DataLens AWS MVP.

Reads one deterministic Silver batch and publishes three small, interview-
friendly analytical marts to the dedicated Gold S3 bucket.
"""

import re
import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    current_timestamp,
    lit,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
)


EXPECTED_SOURCES = ("itviec", "topcv")
REQUIRED_COLUMNS = (
    "title",
    "url",
    "source",
    "company",
    "location_std",
    "min_salary",
    "max_salary",
    "currency",
)
MART_NAMES = (
    "job_market_overview",
    "source_performance",
    "location_summary",
)


def validate_batch_date(value):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError(
            f"BATCH_DATE_INVALID: expected YYYY-MM-DD, received {value!r}"
        ) from error

    if parsed.strftime("%Y-%m-%d") != value:
        raise ValueError(
            f"BATCH_DATE_INVALID: expected YYYY-MM-DD, received {value!r}"
        )
    return value


def validate_bucket_name(value):
    pattern = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
    if not re.fullmatch(pattern, value):
        raise ValueError(f"S3_BUCKET_INVALID: {value!r}")
    return value


def delete_prefix(s3_client, bucket, prefix):
    """Delete one Gold mart batch prefix before its deterministic rewrite."""
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not objects:
            continue
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects, "Quiet": True},
        )
        deleted += len(objects)

    print(f"GOLD_PREFIX_RESET prefix=s3://{bucket}/{prefix} deleted={deleted}")


def load_silver_batch(spark, silver_bucket, batch_date):
    base_path = f"s3://{silver_bucket}/jobs/"
    input_path = f"{base_path}source=*/batch_date={batch_date}/*.parquet"

    dataframe = (
        spark.read
        .option("basePath", base_path)
        .option("mergeSchema", "true")
        .parquet(input_path)
        .cache()
    )
    input_rows = dataframe.count()
    if input_rows == 0:
        raise RuntimeError(f"GOLD_EMPTY_INPUT: batch_date={batch_date}")

    missing_columns = [
        name for name in REQUIRED_COLUMNS if name not in dataframe.columns
    ]
    if missing_columns:
        raise RuntimeError(
            "GOLD_REQUIRED_COLUMNS_MISSING: " + ",".join(missing_columns)
        )

    source_counts = {
        row["source"]: row["count"]
        for row in dataframe.groupBy("source").count().collect()
    }
    missing_sources = [
        source for source in EXPECTED_SOURCES if source_counts.get(source, 0) == 0
    ]
    if missing_sources:
        raise RuntimeError("GOLD_SOURCE_MISSING: " + ",".join(missing_sources))

    print(
        "GOLD_INPUT "
        f"batch_date={batch_date} rows={input_rows} path={input_path} "
        f"itviec={source_counts.get('itviec', 0)} "
        f"topcv={source_counts.get('topcv', 0)}"
    )
    return dataframe, input_rows


def build_marts(dataframe, batch_date):
    prepared = (
        dataframe
        .withColumn("report_date", to_date(lit(batch_date)))
        .withColumn(
            "has_salary",
            col("min_salary").isNotNull() | col("max_salary").isNotNull(),
        )
        .cache()
    )

    overview = (
        prepared
        .groupBy("report_date")
        .agg(
            countDistinct("source", "url").alias("total_jobs"),
            countDistinct("company").alias("total_companies"),
            countDistinct("source").alias("total_sources"),
            countDistinct("location_std").alias("total_locations"),
            spark_sum(when(col("has_salary"), 1).otherwise(0)).alias(
                "jobs_with_salary"
            ),
        )
        .withColumn("created_at", current_timestamp())
    )

    source_performance = (
        prepared
        .groupBy("source", "report_date")
        .agg(
            countDistinct("url").alias("job_count"),
            countDistinct("company").alias("company_count"),
            spark_sum(when(col("has_salary"), 1).otherwise(0)).alias(
                "jobs_with_salary"
            ),
        )
    )

    location_summary = (
        prepared
        .groupBy("location_std", "source", "currency", "report_date")
        .agg(
            countDistinct("url").alias("job_count"),
            spark_sum(when(col("has_salary"), 1).otherwise(0)).alias(
                "jobs_with_salary"
            ),
            spark_round(avg("min_salary"), 2).alias("avg_min_salary"),
            spark_round(avg("max_salary"), 2).alias("avg_max_salary"),
            spark_max("max_salary").alias("highest_salary"),
        )
    )

    return {
        "job_market_overview": overview,
        "source_performance": source_performance,
        "location_summary": location_summary,
    }


def publish_mart(dataframe, mart_name, gold_bucket, batch_date, s3_client):
    rows = dataframe.count()
    if rows == 0:
        raise RuntimeError(f"GOLD_MART_EMPTY: {mart_name}")

    prefix = f"marts/{mart_name}/batch_date={batch_date}/"
    delete_prefix(s3_client, gold_bucket, prefix)
    output_uri = f"s3://{gold_bucket}/{prefix}"

    (
        dataframe
        .coalesce(1)
        .write.mode("append")
        .parquet(output_uri)
    )
    print(f"GOLD_MART_OUTPUT mart={mart_name} rows={rows} uri={output_uri}")
    return rows


def run():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET", "BATCH_DATE"],
    )
    silver_bucket = validate_bucket_name(args["SILVER_BUCKET"])
    gold_bucket = validate_bucket_name(args["GOLD_BUCKET"])
    batch_date = validate_batch_date(args["BATCH_DATE"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    silver, input_rows = load_silver_batch(
        spark,
        silver_bucket,
        batch_date,
    )
    marts = build_marts(silver, batch_date)
    if tuple(marts) != MART_NAMES:
        raise RuntimeError("GOLD_MART_CONTRACT_MISMATCH")

    s3_client = boto3.client("s3")
    mart_rows = {
        name: publish_mart(
            dataframe,
            name,
            gold_bucket,
            batch_date,
            s3_client,
        )
        for name, dataframe in marts.items()
    }

    overview_total_jobs = marts["job_market_overview"].first()["total_jobs"]
    if overview_total_jobs != input_rows:
        raise RuntimeError(
            "GOLD_INPUT_TOTAL_MISMATCH: "
            f"input={input_rows} overview_total_jobs={overview_total_jobs}"
        )

    print(
        "GOLD_METRICS "
        f"batch_date={batch_date} input={input_rows} "
        f"job_market_overview={mart_rows['job_market_overview']} "
        f"source_performance={mart_rows['source_performance']} "
        f"location_summary={mart_rows['location_summary']}"
    )
    print("GOLD_GLUE_JOB=PASS")
    job.commit()


if __name__ == "__main__":
    try:
        run()
    except Exception as error:
        print(f"GOLD_GLUE_JOB=FAIL error_type={type(error).__name__} error={error}")
        raise
