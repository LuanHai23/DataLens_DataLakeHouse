"""AWS Glue Silver job for the VNJobs DataLens AWS MVP.

Reads one validated Bronze batch, applies the existing lightweight Silver
normalisation rules, removes duplicate job URLs, and publishes deterministic
Parquet partitions to the dedicated Silver bucket.
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
    coalesce,
    col,
    current_timestamp,
    length,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    row_number,
    to_json,
    trim,
    when,
)
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql.window import Window


EXPECTED_SOURCES = ("itviec", "topcv")
REQUIRED_COLUMNS = ("title", "url", "source")
OPTIONAL_COLUMNS = (
    "keyword",
    "company",
    "location",
    "work_type",
    "salary",
    "tags",
    "posted",
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
    """Delete only one deterministic Silver batch prefix before rewriting it."""
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

    print(f"SILVER_PREFIX_RESET prefix=s3://{bucket}/{prefix} deleted={deleted}")


def ensure_column(dataframe, name, data_type=StringType()):
    if name not in dataframe.columns:
        return dataframe.withColumn(name, lit(None).cast(data_type))
    return dataframe


def load_validated_batch(spark, bronze_bucket, batch_date):
    base_path = f"s3://{bronze_bucket}/validated/jobs/"
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
        raise RuntimeError(f"SILVER_EMPTY_INPUT: batch_date={batch_date}")

    missing_columns = [
        name for name in REQUIRED_COLUMNS if name not in dataframe.columns
    ]
    if missing_columns:
        raise RuntimeError(
            "SILVER_REQUIRED_COLUMNS_MISSING: " + ",".join(missing_columns)
        )

    source_counts = {
        row["source"]: row["count"]
        for row in dataframe.groupBy("source").count().collect()
    }
    missing_sources = [
        source for source in EXPECTED_SOURCES if source_counts.get(source, 0) == 0
    ]
    if missing_sources:
        raise RuntimeError(
            "SILVER_SOURCE_MISSING: " + ",".join(missing_sources)
        )

    print(
        "SILVER_INPUT "
        f"batch_date={batch_date} rows={input_rows} path={input_path} "
        f"itviec={source_counts.get('itviec', 0)} "
        f"topcv={source_counts.get('topcv', 0)}"
    )
    return dataframe, input_rows


def transform(dataframe, batch_date):
    for name in OPTIONAL_COLUMNS:
        dataframe = ensure_column(dataframe, name)

    if isinstance(dataframe.schema["tags"].dataType, ArrayType):
        dataframe = dataframe.withColumn("tags", to_json(col("tags")))

    salary_lower = lower(trim(coalesce(col("salary"), lit(""))))
    location_lower = lower(trim(coalesce(col("location"), lit(""))))

    transformed = (
        dataframe
        .withColumn("salary_lower", salary_lower)
        .withColumn("location_lower", location_lower)
        .withColumn(
            "currency",
            when(col("salary_lower").rlike(r"usd|\$"), "USD").otherwise("VND"),
        )
        .withColumn(
            "multiplier",
            when(col("currency") == "USD", lit(1.0))
            .when(
                col("salary_lower").rlike(r"triệu|tr\b|million"),
                lit(1_000_000.0),
            )
            .when(
                col("salary_lower").rlike(r"nghìn|k\b|thousand"),
                lit(1_000.0),
            )
            .otherwise(lit(1.0)),
        )
    )

    number_first = r"(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"
    number_after_dash = (
        r"-\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"
    )

    transformed = (
        transformed
        .withColumn("num1", regexp_extract("salary_lower", number_first, 1))
        .withColumn("num2", regexp_extract("salary_lower", number_after_dash, 1))
        .withColumn(
            "val1",
            regexp_replace(col("num1"), r"[,.]", "").cast(FloatType()),
        )
        .withColumn(
            "val2",
            regexp_replace(col("num2"), r"[,.]", "").cast(FloatType()),
        )
        .withColumn(
            "min_salary",
            when(
                col("salary_lower").contains("up to"),
                lit(None).cast(FloatType()),
            )
            .when(col("val1").isNotNull(), col("val1") * col("multiplier"))
            .otherwise(lit(None).cast(FloatType())),
        )
        .withColumn(
            "max_salary",
            when(
                col("salary_lower").contains("up to"),
                col("val1") * col("multiplier"),
            )
            .when(
                col("val2").isNotNull() & (col("val2") > 0),
                col("val2") * col("multiplier"),
            )
            .when(
                col("val1").isNotNull() & col("val2").isNull(),
                col("val1") * col("multiplier"),
            )
            .otherwise(lit(None).cast(FloatType())),
        )
        .withColumn(
            "location_std",
            when(
                col("location_lower").rlike(
                    r"hồ chí minh|ho chi minh|hcm|sài gòn|saigon"
                ),
                "Ho Chi Minh",
            )
            .when(
                col("location_lower").rlike(r"hà nội|ha noi|hanoi|hn\b"),
                "Ha Noi",
            )
            .when(
                col("location_lower").rlike(r"đà nẵng|da nang|danang"),
                "Da Nang",
            )
            .when(col("location_lower").contains("remote"), "Remote")
            .otherwise("Other"),
        )
    )

    selected = transformed.select(
        trim(col("title")).alias("title"),
        trim(col("url")).alias("url"),
        col("source"),
        col("keyword"),
        col("company"),
        col("work_type"),
        col("salary").alias("salary_raw"),
        col("tags"),
        col("posted"),
        col("min_salary"),
        col("max_salary"),
        col("currency"),
        col("location_std"),
        lit(batch_date).alias("batch_date"),
        current_timestamp().alias("processed_at"),
    )

    dedupe_window = Window.partitionBy("source", "url").orderBy(
        coalesce(col("max_salary"), col("min_salary"), lit(0.0)).desc(),
        col("title").asc(),
    )
    return (
        selected
        .filter(
            col("title").isNotNull()
            & (length(col("title")) > 0)
            & col("url").isNotNull()
            & (length(col("url")) > 0)
        )
        .withColumn("_row_number", row_number().over(dedupe_window))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def publish_batch(dataframe, silver_bucket, batch_date):
    s3_client = boto3.client("s3")
    output_rows = 0

    for source in EXPECTED_SOURCES:
        source_frame = dataframe.filter(col("source") == source).cache()
        source_rows = source_frame.count()
        if source_rows == 0:
            raise RuntimeError(f"SILVER_SOURCE_EMPTY_AFTER_TRANSFORM: {source}")

        prefix = f"jobs/source={source}/batch_date={batch_date}/"
        delete_prefix(s3_client, silver_bucket, prefix)

        output_uri = f"s3://{silver_bucket}/{prefix}"
        (
            source_frame
            .drop("source", "batch_date")
            .coalesce(1)
            .write.mode("append")
            .parquet(output_uri)
        )
        output_rows += source_rows
        print(
            "SILVER_SOURCE_OUTPUT "
            f"source={source} rows={source_rows} uri={output_uri}"
        )

    return output_rows


def run():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET", "BATCH_DATE"],
    )
    bronze_bucket = validate_bucket_name(args["BRONZE_BUCKET"])
    silver_bucket = validate_bucket_name(args["SILVER_BUCKET"])
    batch_date = validate_batch_date(args["BATCH_DATE"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    validated, input_rows = load_validated_batch(
        spark,
        bronze_bucket,
        batch_date,
    )
    silver = transform(validated, batch_date).cache()
    output_rows = silver.count()
    if output_rows == 0:
        raise RuntimeError("SILVER_EMPTY_OUTPUT")

    written_rows = publish_batch(silver, silver_bucket, batch_date)
    if written_rows != output_rows:
        raise RuntimeError(
            f"SILVER_ROW_COUNT_MISMATCH: transformed={output_rows} "
            f"written={written_rows}"
        )

    print(
        "SILVER_METRICS "
        f"batch_date={batch_date} input={input_rows} output={output_rows} "
        f"duplicates_or_invalid_removed={input_rows - output_rows}"
    )
    print("SILVER_GLUE_JOB=PASS")
    job.commit()


if __name__ == "__main__":
    try:
        run()
    except Exception as error:
        print(f"SILVER_GLUE_JOB=FAIL error_type={type(error).__name__} error={error}")
        raise
