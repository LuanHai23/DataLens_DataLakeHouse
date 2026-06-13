from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, regexp_replace, when, lower, trim,
    lit, current_timestamp, coalesce, row_number, to_json
)
from pyspark.sql.types import StringType, FloatType, ArrayType
from pyspark.sql.window import Window
import os

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minio_password")
}

SOURCE_PATH  = "s3a://warehouse/bronze/jobs/"
TARGET_TABLE = "demo.silver.jobs"

def create_spark_session() -> SparkSession:
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
    return (
        SparkSession.builder
        .appName("JobHunter_Silver_ETL")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.demo","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type","hive")
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def _ensure_column(df, column, default_type=StringType()):
    if column not in df.columns:
        print(f"⚠️  Column '{column}' not found — filling with NULL.")
        df = df.withColumn(column, lit(None).cast(default_type))
    return df

def transform_data_silver():
    spark = create_spark_session()

    print("📥 Loading data from MinIO bronze layer …")
    try:
        df = spark.read.parquet(SOURCE_PATH)
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    print(f"   Schema: {df.columns}")
    print(f"   Row count: {df.count()}")

    # SCHEMA GUARD — đảm bảo đủ cột
    for c in ["location", "salary", "title", "url", "source",
              "keyword", "company", "work_type", "tags", "posted"]:
        df = _ensure_column(df, c, StringType())

    # tags có thể là ArrayType — convert sang string JSON để lưu silver
    if dict(df.dtypes).get("tags") == "array<string>":
        df = df.withColumn("tags", to_json(col("tags")))

    # NORMALISE
    df_clean = (
        df
        .withColumn("salary_lower",   lower(trim(col("salary"))))
        .withColumn("location_lower", lower(trim(col("location"))))
    )

    # CURRENCY
    df_clean = df_clean.withColumn(
        "currency",
        when(col("salary_lower").rlike(r"usd|\$"), "USD").otherwise("VND")
    )

    # MULTIPLIER
    df_clean = df_clean.withColumn(
        "multiplier",
        when(col("currency") == "USD", 1.0)
        .when(col("salary_lower").rlike(r"triệu|tr\b|million"), 1_000_000.0)
        .when(col("salary_lower").rlike(r"nghìn|k\b|thousand"),     1_000.0)
        .otherwise(1.0)
    )

    # EXTRACT numbers
    NUM_FIRST      = r"(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"
    NUM_AFTER_DASH = r"-\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"

    df_extract = (
        df_clean
        .withColumn("num1", regexp_extract("salary_lower", NUM_FIRST,      1))
        .withColumn("num2", regexp_extract("salary_lower", NUM_AFTER_DASH, 1))
        .withColumn("val1", regexp_replace(col("num1"), r"[,.]", "").cast(FloatType()))
        .withColumn("val2", regexp_replace(col("num2"), r"[,.]", "").cast(FloatType()))
    )

    # MIN / MAX SALARY
    df_final = (
        df_extract
        .withColumn("min_salary",
            when(col("salary_lower").contains("up to"), lit(None).cast(FloatType()))
            .when(col("val1").isNotNull(), col("val1") * col("multiplier"))
            .otherwise(lit(None).cast(FloatType()))
        )
        .withColumn("max_salary",
            when(col("salary_lower").contains("up to"), col("val1") * col("multiplier"))
            .when((col("val2").isNotNull()) & (col("val2") > 0), col("val2") * col("multiplier"))
            .when((col("val1").isNotNull()) & (col("val2").isNull()), col("val1") * col("multiplier"))
            .otherwise(lit(None).cast(FloatType()))
        )
    )

    # STANDARDISE location
    df_final = df_final.withColumn(
        "location_std",
        when(col("location_lower").rlike(r"ho chi minh|hcm|saigon"),  "Ho Chi Minh")
        .when(col("location_lower").rlike(r"ha noi|hn\b|hanoi"),       "Ha Noi")
        .when(col("location_lower").rlike(r"da nang|danang"),          "Da Nang")
        .when(col("location_lower").contains("remote"),                "Remote")
        .otherwise("Other")
    )

    # SELECT — giữ thêm keyword, company, work_type, tags, posted, salary (raw)
    df_silver = df_final.select(
        col("title"),
        col("url"),
        col("source"),
        col("keyword"),
        col("company"),
        col("work_type"),
        col("salary").alias("salary_raw"),   # raw string để Discord hiển thị
        col("tags"),                          # JSON string
        col("posted"),
        col("min_salary"),
        col("max_salary"),
        col("currency"),
        col("location_std"),
        current_timestamp().alias("processed_at"),
    )

    # DEDUP
    dedup_window = Window.partitionBy("url").orderBy(
        coalesce(col("max_salary"), col("min_salary"), lit(0.0)).desc(),
        col("title").asc(),
    )
    df_silver = (
        df_silver
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    print("📊 Silver sample (5 rows):")
    df_silver.show(5, truncate=False)

    # WRITE to Iceberg
    print(f"💾 Upserting into Iceberg table: {TARGET_TABLE}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            title        STRING,
            url          STRING,
            source       STRING,
            keyword      STRING,
            company      STRING,
            work_type    STRING,
            salary_raw   STRING,
            tags         STRING,
            posted       STRING,
            min_salary   FLOAT,
            max_salary   FLOAT,
            currency     STRING,
            location_std STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source)
    """)

    df_silver.createOrReplaceTempView("batch_updates")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING batch_updates s ON t.url = s.url
        WHEN MATCHED THEN UPDATE SET
            t.title        = s.title,
            t.keyword      = s.keyword,
            t.company      = s.company,
            t.work_type    = s.work_type,
            t.salary_raw   = s.salary_raw,
            t.tags         = s.tags,
            t.posted       = s.posted,
            t.min_salary   = s.min_salary,
            t.max_salary   = s.max_salary,
            t.currency     = s.currency,
            t.location_std = s.location_std,
            t.processed_at = s.processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("✅ SILVER ETL SUCCESS!")

if __name__ == "__main__":
    transform_data_silver()