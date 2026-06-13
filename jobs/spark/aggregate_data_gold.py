from datetime import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    current_date,
    current_timestamp,
    lit,
    lower,
    max as spark_max,
    regexp_replace,
    round as spark_round,
    sum as spark_sum,
    when,
)

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minio_password"),
}

# PostgreSQL serving layer for Metabase.
# The Iceberg Gold tables remain the analytical source of truth.
# PostgreSQL only stores BI-friendly copies of selected marts.
POSTGRES_CONF = {
    "host": os.getenv("SERVING_POSTGRES_HOST", os.getenv("MB_DB_HOST", "postgres")),
    "port": os.getenv("SERVING_POSTGRES_PORT", os.getenv("MB_DB_PORT", "5432")),
    "database": os.getenv("SERVING_POSTGRES_DB", os.getenv("MB_DB_DBNAME", "warehouse_db")),
    "user": os.getenv("SERVING_POSTGRES_USER", os.getenv("MB_DB_USER", os.getenv("POSTGRES_USER", "admin"))),
    "password": os.getenv("SERVING_POSTGRES_PASSWORD", os.getenv("MB_DB_PASS", os.getenv("POSTGRES_PASSWORD", "adminpassword"))),
    "schema": os.getenv("SERVING_POSTGRES_SCHEMA", "analytics"),
}

SOURCE_TABLE = "demo.silver.jobs"
GOLD_NAMESPACE = "demo.gold"

# Skill dictionary used to turn raw title/tags text into a skill-demand mart.
# You can add more skills later without changing the downstream schema.
SKILLS = [
    "python", "sql", "spark", "airflow", "docker", "kafka", "aws", "azure",
    "gcp", "java", "javascript", "typescript", "react", "node", "php",
    "c#", ".net", "golang", "go", "devops", "linux", "power bi", "tableau",
    "machine learning", "data engineer", "data analyst", "etl", "dbt",
]


def create_spark_session() -> SparkSession:
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.3",
    ]

    print("====== MinIO Config ======")
    print(f"MINIO_ENDPOINT={MINIO_CONF['endpoint']}")
    print(f"MINIO_ACCESS_KEY={MINIO_CONF['access_key']}")
    print(f"MINIO_SECRET_KEY is set: {bool(MINIO_CONF['secret_key'])}")
    print("==========================")
    print("====== PostgreSQL Serving Config ======")
    print(f"POSTGRES_HOST={POSTGRES_CONF['host']}")
    print(f"POSTGRES_PORT={POSTGRES_CONF['port']}")
    print(f"POSTGRES_DB={POSTGRES_CONF['database']}")
    print(f"POSTGRES_SCHEMA={POSTGRES_CONF['schema']}")
    print(f"POSTGRES_USER={POSTGRES_CONF['user']}")
    print(f"POSTGRES_PASSWORD is set: {bool(POSTGRES_CONF['password'])}")
    print("=======================================")

    return (
        SparkSession.builder
        .appName("DataLens_Gold_Data_Products")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hive")
        .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def _schema_ddl(df: DataFrame) -> str:
    return ",\n            ".join(
        f"{field.name} {field.dataType.simpleString().upper()}"
        for field in df.schema.fields
    )


def save_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    partition_col: str,
    merge_keys: list[str],
) -> None:
    """
    Create an Iceberg table if needed, then upsert records by merge_keys.

    Important:
    - This function assumes the target table schema is compatible with df.
    - For legacy tables, pass a DataFrame with the old schema only.
    """
    print(f"\n💾 Upserting data product: {table_name}")

    namespace = ".".join(table_name.split(".")[:2])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    temp_view = f"tmp_{table_name.replace('.', '_')}"
    df.createOrReplaceTempView(temp_view)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {_schema_ddl(df)}
        )
        USING iceberg
        PARTITIONED BY ({partition_col})
    """)

    merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
    update_cols = [c for c in df.columns if c not in merge_keys]

    if update_cols:
        update_set = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
        """
    else:
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON {merge_condition}
            WHEN NOT MATCHED THEN INSERT *
        """

    spark.sql(merge_sql)

    print(f"✅ {table_name}: {spark.table(table_name).count()} total row(s)")


def build_skill_mart(df_silver: DataFrame) -> DataFrame:
    """
    Build one row per detected skill per report_date.

    Robust handling:
    - title may be null
    - tags may be an array/string/null, so cast to string before text matching
    """
    df_text = df_silver.withColumn(
        "search_text",
        lower(
            regexp_replace(
                concat_ws(
                    " ",
                    coalesce(col("title").cast("string"), lit("")),
                    coalesce(col("tags").cast("string"), lit("")),
                ),
                r"[^a-zA-Z0-9+#. ]",
                " ",
            )
        ),
    )

    skill_dfs = []

    for skill in SKILLS:
        skill_df = (
            df_text
            .filter(col("search_text").contains(skill))
            .select(
                lit(skill.title()).alias("skill"),
                col("source"),
                col("location_std"),
                col("url"),
                col("report_date"),
            )
        )
        skill_dfs.append(skill_df)

    df_skills = skill_dfs[0]
    for item in skill_dfs[1:]:
        df_skills = df_skills.unionByName(item)

    return (
        df_skills
        .groupBy("skill", "source", "location_std", "report_date")
        .agg(countDistinct("url").alias("job_count"))
    )


def build_legacy_tables(df_silver: DataFrame, mart_high_salary_alerts: DataFrame) -> dict[str, DataFrame]:
    """
    Build backward-compatible Gold tables for old Discord bot / Metabase queries.

    These tables intentionally keep the old schema, while the new mart_* tables
    provide richer production-like analytics outputs.
    """

    # Legacy 1: ITviec jobs table for old Discord command / old dashboard
    legacy_itviec_jobs = (
        df_silver
        .filter(col("source") == "itviec")
        .select(
            col("keyword"),
            col("title"),
            col("url"),
            col("company"),
            col("location_std").alias("location"),
            col("work_type"),
            col("salary_raw").alias("salary"),
            col("tags"),
            col("posted"),
            col("report_date"),
        )
    )

    # Legacy 2: Market summary with old schema.
    # Old schema does NOT include source and uses total_jobs, not job_count.
    legacy_market_summary = (
        df_silver
        .filter(col("min_salary").isNotNull())
        .groupBy("location_std", "currency", "report_date")
        .agg(
            count("url").alias("total_jobs"),
            spark_round(avg("min_salary"), 2).alias("avg_min_salary"),
            spark_round(avg("max_salary"), 2).alias("avg_max_salary"),
            spark_max("max_salary").alias("highest_salary"),
        )
    )

    # Legacy 3: Source stats with old schema.
    legacy_source_stats = (
        df_silver
        .groupBy("source", "report_date")
        .agg(count("url").alias("jobs_count"))
    )

    # Legacy 4: Daily alerts with old schema.
    # Old schema does NOT include company or salary_raw.
    legacy_daily_alerts = (
        mart_high_salary_alerts
        .select(
            "title",
            "url",
            "min_salary",
            "max_salary",
            "currency",
            "source",
            "location_std",
            "report_date",
        )
    )

    return {
        "itviec_jobs": legacy_itviec_jobs,
        "market_summary": legacy_market_summary,
        "source_stats": legacy_source_stats,
        "daily_alerts": legacy_daily_alerts,
    }



def get_postgres_jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{POSTGRES_CONF['host']}:{POSTGRES_CONF['port']}/"
        f"{POSTGRES_CONF['database']}"
    )


def ensure_postgres_schema(spark: SparkSession) -> None:
    """Create the PostgreSQL serving schema if it does not exist."""
    schema_name = POSTGRES_CONF["schema"]
    jdbc_url = get_postgres_jdbc_url()

    print(f"\n🧱 Ensuring PostgreSQL schema exists: {schema_name}")

    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")

    conn = None
    stmt = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(
            jdbc_url,
            POSTGRES_CONF["user"],
            POSTGRES_CONF["password"],
        )
        stmt = conn.createStatement()
        stmt.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"✅ PostgreSQL schema ready: {schema_name}")
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def publish_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Publish a Gold mart to PostgreSQL serving layer for Metabase.

    Why overwrite?
    - These marts are daily analytical snapshots.
    - Metabase should read a clean BI-friendly table.
    - Iceberg Gold remains the source of truth and supports upsert/history.
    """
    schema_name = POSTGRES_CONF["schema"]
    full_table_name = f"{schema_name}.{table_name}"
    jdbc_url = get_postgres_jdbc_url()

    print(f"\n📤 Publishing to PostgreSQL serving layer: {full_table_name}")

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", full_table_name)
        .option("user", POSTGRES_CONF["user"])
        .option("password", POSTGRES_CONF["password"])
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )

    print(f"✅ Published PostgreSQL table: {full_table_name}")


def publish_marts_to_postgres(spark: SparkSession, marts: dict[str, DataFrame]) -> None:
    """Publish selected Gold marts to PostgreSQL for Metabase dashboards."""
    print("\n🚀 Publishing Gold marts to PostgreSQL serving layer for Metabase...")
    ensure_postgres_schema(spark)

    for table_name, df in marts.items():
        if df.count() == 0:
            print(f"⚠️ Skip PostgreSQL publish for {table_name}: empty DataFrame")
            continue
        publish_to_postgres(df, table_name)

    print("✅ PostgreSQL serving layer publish complete!")


def create_gold_data_products() -> None:
    spark = create_spark_session()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")

    print(f"📥 Loading Silver table: {SOURCE_TABLE}")

    try:
        df_silver = spark.table(SOURCE_TABLE)
    except Exception as exc:
        print(f"❌ Cannot load {SOURCE_TABLE}: {exc}")
        spark.stop()
        return

    records_in = df_silver.count()
    print(f"✅ Loaded {records_in} record(s) from Silver")

    if records_in == 0:
        print("⚠️ Silver table is empty. Gold aggregation skipped.")
        spark.stop()
        return

    df_silver = (
        df_silver
        .withColumn("report_date", current_date())
        .withColumn("has_salary", col("min_salary").isNotNull() | col("max_salary").isNotNull())
        .cache()
    )

    # ---------------------------------------------------------------------
    # New production-like Gold marts
    # ---------------------------------------------------------------------

    # 1. Job Market Overview: executive dashboard metrics.
    mart_job_market_overview = (
        df_silver
        .groupBy("report_date")
        .agg(
            countDistinct("url").alias("total_jobs"),
            countDistinct("company").alias("total_companies"),
            countDistinct("source").alias("total_sources"),
            countDistinct("location_std").alias("total_locations"),
            spark_sum(when(col("has_salary"), 1).otherwise(0)).alias("jobs_with_salary"),
        )
        .withColumn("created_at", current_timestamp())
    )

    save_to_iceberg(
        spark,
        mart_job_market_overview,
        f"{GOLD_NAMESPACE}.mart_job_market_overview",
        "report_date",
        ["report_date"],
    )

    # 2. Source Performance: where data/jobs come from.
    mart_source_performance = (
        df_silver
        .groupBy("source", "report_date")
        .agg(
            countDistinct("url").alias("job_count"),
            countDistinct("company").alias("company_count"),
            spark_sum(when(col("has_salary"), 1).otherwise(0)).alias("jobs_with_salary"),
        )
    )

    save_to_iceberg(
        spark,
        mart_source_performance,
        f"{GOLD_NAMESPACE}.mart_source_performance",
        "report_date",
        ["source", "report_date"],
    )

    # 3. Salary by Location: salary distribution by city/source/currency.
    mart_salary_by_location = (
        df_silver
        .filter(col("has_salary"))
        .groupBy("location_std", "source", "currency", "report_date")
        .agg(
            countDistinct("url").alias("job_count"),
            spark_round(avg("min_salary"), 2).alias("avg_min_salary"),
            spark_round(avg("max_salary"), 2).alias("avg_max_salary"),
            spark_max("max_salary").alias("highest_salary"),
        )
    )

    save_to_iceberg(
        spark,
        mart_salary_by_location,
        f"{GOLD_NAMESPACE}.mart_salary_by_location",
        "report_date",
        ["location_std", "source", "currency", "report_date"],
    )

    # 4. Company Hiring Trend: active employers by posting volume.
    mart_company_hiring_trend = (
        df_silver
        .filter(col("company").isNotNull())
        .groupBy("company", "source", "location_std", "report_date")
        .agg(
            countDistinct("url").alias("job_count"),
            spark_max("max_salary").alias("highest_salary"),
        )
    )

    save_to_iceberg(
        spark,
        mart_company_hiring_trend,
        f"{GOLD_NAMESPACE}.mart_company_hiring_trend",
        "report_date",
        ["company", "source", "location_std", "report_date"],
    )

    # 5. Skill Demand: most frequently requested skills.
    mart_skill_demand = build_skill_mart(df_silver)

    save_to_iceberg(
        spark,
        mart_skill_demand,
        f"{GOLD_NAMESPACE}.mart_skill_demand",
        "report_date",
        ["skill", "source", "location_std", "report_date"],
    )

    # 6. High Salary Alerts: rich job-level output for new dashboard.
    mart_high_salary_alerts = (
        df_silver
        .filter(
            ((col("currency") == "USD") & (col("min_salary") >= 1000)) |
            ((col("currency") == "VND") & (col("min_salary") >= 20_000_000))
        )
        .select(
            "title",
            "company",
            "url",
            "source",
            "location_std",
            "salary_raw",
            "min_salary",
            "max_salary",
            "currency",
            "report_date",
        )
    )

    save_to_iceberg(
        spark,
        mart_high_salary_alerts,
        f"{GOLD_NAMESPACE}.mart_high_salary_alerts",
        "report_date",
        ["url", "report_date"],
    )

    # ---------------------------------------------------------------------
    # Backward-compatible legacy Gold tables
    # ---------------------------------------------------------------------
    print("\n🔁 Building backward-compatible legacy Gold tables...")

    legacy_tables = build_legacy_tables(df_silver, mart_high_salary_alerts)

    if legacy_tables["itviec_jobs"].count() > 0:
        save_to_iceberg(
            spark,
            legacy_tables["itviec_jobs"],
            f"{GOLD_NAMESPACE}.itviec_jobs",
            "report_date",
            ["url", "report_date"],
        )
    else:
        print("⚠️ No ITviec jobs found. Skip legacy itviec_jobs table.")

    save_to_iceberg(
        spark,
        legacy_tables["market_summary"],
        f"{GOLD_NAMESPACE}.market_summary",
        "report_date",
        ["location_std", "currency", "report_date"],
    )

    save_to_iceberg(
        spark,
        legacy_tables["source_stats"],
        f"{GOLD_NAMESPACE}.source_stats",
        "report_date",
        ["source", "report_date"],
    )

    if legacy_tables["daily_alerts"].count() > 0:
        save_to_iceberg(
            spark,
            legacy_tables["daily_alerts"],
            f"{GOLD_NAMESPACE}.daily_alerts",
            "report_date",
            ["url", "report_date"],
        )
    else:
        print("⚠️ No high-salary jobs found. Skip legacy daily_alerts table.")

    # 7. Pipeline Health: simple production-like monitoring output.
    mart_pipeline_health = (
        spark.createDataFrame(
            [("gold_aggregation", "success", int(records_in), datetime.now())],
            "pipeline_name STRING, status STRING, records_in BIGINT, created_at TIMESTAMP",
        )
        .withColumn("report_date", current_date())
    )

    save_to_iceberg(
        spark,
        mart_pipeline_health,
        f"{GOLD_NAMESPACE}.mart_pipeline_health",
        "report_date",
        ["pipeline_name", "report_date"],
    )

    # ---------------------------------------------------------------------
    # PostgreSQL serving layer for Metabase
    # ---------------------------------------------------------------------
    marts_for_postgres = {
        "mart_job_market_overview": mart_job_market_overview,
        "mart_source_performance": mart_source_performance,
        "mart_salary_by_location": mart_salary_by_location,
        "mart_company_hiring_trend": mart_company_hiring_trend,
        "mart_skill_demand": mart_skill_demand,
        "mart_high_salary_alerts": mart_high_salary_alerts,
        "mart_pipeline_health": mart_pipeline_health,
    }
    publish_marts_to_postgres(spark, marts_for_postgres)

    print("\n✅ GOLD DATA PRODUCTS COMPLETE!")
    print("Created/updated new data marts:")
    for table in [
        "mart_job_market_overview",
        "mart_source_performance",
        "mart_salary_by_location",
        "mart_company_hiring_trend",
        "mart_skill_demand",
        "mart_high_salary_alerts",
        "mart_pipeline_health",
    ]:
        print(f"   - {GOLD_NAMESPACE}.{table}")

    print("Created/updated backward-compatible legacy tables:")
    for table in [
        "itviec_jobs",
        "market_summary",
        "source_stats",
        "daily_alerts",
    ]:
        print(f"   - {GOLD_NAMESPACE}.{table}")

    print("Published PostgreSQL serving tables for Metabase:")
    for table in [
        "mart_job_market_overview",
        "mart_source_performance",
        "mart_salary_by_location",
        "mart_company_hiring_trend",
        "mart_skill_demand",
        "mart_high_salary_alerts",
        "mart_pipeline_health",
    ]:
        print(f"   - {POSTGRES_CONF['schema']}.{table}")

    df_silver.unpersist()
    spark.stop()


if __name__ == "__main__":
    create_gold_data_products()
