import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, current_timestamp, lit, explode, to_json
)
import great_expectations as gx

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minio_password")
}

INPUT_PATH  = "s3a://data-lake/*/*/*.json"
OUTPUT_PATH = "s3a://warehouse/bronze/jobs/"

def create_spark_session() -> SparkSession:    
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
    return (
        SparkSession.builder
        .appName("JobHunter_Bronze_Validation")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hive")
        .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def flatten_json(df):
    """
    Xử lý 2 dạng JSON:
      - Flat:   [{title, url, source, ...}, ...]        → giữ nguyên
      - Nested: {source, jobs: [{title, url, ...}]}     → giữ source rồi explode
    """
    if "jobs" in df.columns:
        print("   📦 Detected nested JSON (itviec format) — exploding 'jobs' array...")
        # Giữ lại source từ top-level trước khi explode
        df = df.select(
            col("source"),                          # top-level source
            explode(col("jobs")).alias("job")
        ).select(
            col("source"),                          # giữ source
            col("job.*")                            # tất cả fields trong job object
        )
    else:
        print("   📦 Detected flat JSON format")

    # tags là array → convert sang string JSON
    if "tags" in df.columns:
        from pyspark.sql.types import ArrayType
        if isinstance(df.schema["tags"].dataType, ArrayType):
            df = df.withColumn("tags", to_json(col("tags")))

    return df


def validate_with_great_expectations(df, spark_context):
    print("🛡️ Initializing Great Expectations...")
    context = gx.get_context(mode="ephemeral")
    
    datasource_name = "spark_datasource"
    data_asset_name = "raw_jobs"
    suite_name      = "bronze_quality_checks"
    
    existing_datasources = [ds.get("name") for ds in context.list_datasources()]
    if datasource_name in existing_datasources:
        context.delete_datasource(datasource_name)
    
    datasource = context.sources.add_spark(name=datasource_name)
    data_asset = datasource.add_dataframe_asset(name=data_asset_name, dataframe=df)
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    batch_request = data_asset.build_batch_request(dataframe=df)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    
    validator.expect_column_values_to_not_be_null("title")
    validator.expect_column_values_to_not_be_null("url")
    validator.expect_column_values_to_not_be_null("source")
    validator.expect_column_values_to_match_regex("url", regex=r"^https?://.*", mostly=0.95)
    validator.expect_column_value_lengths_to_be_between("title", min_value=5, max_value=200, mostly=0.98)

    checkpoint = context.add_or_update_checkpoint(
        name="bronze_checkpoint",
        validations=[{"batch_request": batch_request, "expectation_suite_name": suite_name}],
    )
    results = checkpoint.run()
    validation_passed = results["success"]
    print(f"   Validation overall: {'✅ PASSED' if validation_passed else '⚠️  FAILED'}")

    df_clean = df.filter(
        col("title").isNotNull() &
        col("url").isNotNull() &
        col("source").isNotNull() &
        col("url").rlike(r"^https?://.*")
    )
    df_dirty = df.filter(
        col("title").isNull() |
        col("url").isNull() |
        col("source").isNull() |
        (~col("url").rlike(r"^https?://.*"))
    )

    total = df.count()
    clean = df_clean.count()
    dirty = df_dirty.count()
    print(f"   📊 Clean: {clean} ({100*clean/total:.1f}%)  Dirty: {dirty} ({100*dirty/total:.1f}%)")

    return df_clean, df_dirty, results


def run_validation():
    spark = create_spark_session()
    
    print(f"📦 Loading data from: {INPUT_PATH}")
    try:
        df_raw = spark.read.option("multiline", "true").option("mergeSchema", "true").json(INPUT_PATH)
        df_raw = df_raw.withColumn("source_file", input_file_name()) \
                       .withColumn("ingested_at", current_timestamp())
        
        count = df_raw.count()
        print(f"✅ Loaded {count} records (before flatten)")
        if count == 0:
            print("⚠️  No data found. Exiting.")
            return

        # ── Flatten nested JSON (itviec có dạng {jobs: [...]}) ──────────────
        df_raw = flatten_json(df_raw)
        count_flat = df_raw.count()
        print(f"✅ After flatten: {count_flat} records")
        print(f"   Columns: {df_raw.columns}")
        df_raw.select("title", "url", "source", "salary",
                      *[c for c in ["keyword","company","location","work_type","tags"] 
                        if c in df_raw.columns]
                     ).show(3, truncate=False)

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    df_clean, df_dirty, _ = validate_with_great_expectations(df_raw, spark)

    # Debug: xem dirty records bị reject vì lý do gì
    if df_dirty.count() > 0:
        print("\n🔍 DEBUG — Dirty records sample:")
        df_dirty.select("title", "url", "source").show(5, truncate=False)
        print("URL patterns:")
        df_dirty.select("url").distinct().show(5, truncate=False)

    if df_clean.count() > 0:
        print(f"\n💾 Saving clean data to Bronze: {OUTPUT_PATH}")
        df_clean = df_clean.withColumn("crawled_date", lit(datetime.now().strftime("%Y-%m-%d")))
        df_clean.write.mode("append").partitionBy("source").parquet(OUTPUT_PATH)
        print(f"   ✅ Wrote {df_clean.count()} clean records to Bronze")
    else:
        print("⚠️  No clean data to save!")

    if df_dirty.count() > 0:
        quarantine_path = "s3a://warehouse/quarantine/jobs/"
        df_dirty.write.mode("append").partitionBy("source").parquet(quarantine_path)
        print(f"⚠️  {df_dirty.count()} dirty records → {quarantine_path}")

    print("\n🏁 BRONZE VALIDATION COMPLETE!")


if __name__ == "__main__":
    run_validation()