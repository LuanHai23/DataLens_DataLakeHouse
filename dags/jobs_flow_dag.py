from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

SSH_CONN_ID = "spark_ssh"

SPARK_ENV = (
    "export JAVA_HOME=/opt/bitnami/java && "
    "export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:/opt/bitnami/java/bin:$PATH && "
    "export SPARK_HOME=/opt/bitnami/spark && "
    "export PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers && "
)

def _check_alerts(**context):
    from trino.dbapi import connect
    try:
        conn = connect(host="trino", port=8080, user="airflow",
                       catalog="demo", schema="gold", http_scheme="http")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM demo.gold.daily_alerts WHERE report_date = CURRENT_DATE")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"🔔 Số alert hôm nay: {count}")
        return "notify_discord" if count > 0 else "skip_notify"
    except Exception as e:
        print(f"⚠️  Không query được alerts: {e}")
        return "skip_notify"


def _notify_discord(**context):
    import urllib.request, json, os
    from trino.dbapi import connect

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️  Chưa set DISCORD_WEBHOOK_URL — bỏ qua")
        return

    conn = connect(host="trino", port=8080, user="airflow",
                   catalog="demo", schema="gold", http_scheme="http")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM demo.gold.daily_alerts WHERE report_date = CURRENT_DATE")
    alert_count = cur.fetchone()[0]
    cur.execute("SELECT SUM(total_jobs) FROM demo.gold.market_summary WHERE report_date = CURRENT_DATE")
    total_jobs = cur.fetchone()[0] or 0
    cur.close()
    conn.close()

    payload = {"content": (
        f"✅ **Pipeline chạy xong ngày {context['ds']}!**\n"
        f"📋 Tổng job hôm nay: **{total_jobs}**\n"
        f"🚨 Job lương khủng: **{alert_count}** → gõ `!alert` để xem"
    )}

    data = json.dumps(payload).encode("utf-8")
    
    req = urllib.request.Request(
        webhook_url,
        data = data,
        headers={
            "Content-Type": "application/json",
            # THÊM DÒNG NÀY ĐỂ NGỤY TRANG THÀNH TRÌNH DUYỆT CHUẨN:
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    )
    urllib.request.urlopen(req)
    print("✅ Đã gửi thông báo Discord!")


with DAG(
    dag_id="job_hunter_pipeline",
    description="Bronze → GE Validate → Silver → Gold ETL cho VNJobs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["lakehouse", "etl", "jobs"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_topcv = SSHOperator(
        task_id="bronze_crawl_topcv",
        ssh_conn_id=SSH_CONN_ID,
        command="export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && export PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers && python /jobs/crawlers/crawler_topcv.py",
        cmd_timeout=300,
    )

    bronze_itviec = SSHOperator(
        task_id="bronze_crawl_itviec",
        ssh_conn_id=SSH_CONN_ID,
        command="export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && export PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers && python /jobs/crawlers/crawler_itviec.py",
        cmd_timeout=300,
    )

    ingestion_bronze_done = SSHOperator(
        task_id="bronze_ingestion_done",
        ssh_conn_id=SSH_CONN_ID,
        command="export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && export PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers && python /jobs/spark/ingestion_data_bronze.py",
        cmd_timeout=300,
    )
    bronze_done = EmptyOperator(task_id="bronze_done")

    validate_bronze = SSHOperator(
        task_id="validate_bronze_ge",
        ssh_conn_id=SSH_CONN_ID,
        command="export JAVA_HOME=/opt/bitnami/java && export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /jobs/spark/Bronze_validate_to_silver.py",
        cmd_timeout=600,
    )

    silver_transform = SSHOperator(
        task_id="silver_transform",
        ssh_conn_id=SSH_CONN_ID,
        command="export JAVA_HOME=/opt/bitnami/java && export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /jobs/spark/transform_data_silver.py",
        cmd_timeout=600,
    )

    gold_aggregate = SSHOperator(
        task_id="gold_aggregate",
        ssh_conn_id=SSH_CONN_ID,
        command="export JAVA_HOME=/opt/bitnami/java && export PATH=/opt/bitnami/python/bin:/opt/bitnami/spark/bin:$PATH && spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /jobs/spark/aggregate_data_gold.py",
        cmd_timeout=600,
    )

    check_alerts = BranchPythonOperator(
        task_id="check_alerts",
        python_callable=_check_alerts,
        provide_context=True,
    )

    notify_discord = PythonOperator(
        task_id="notify_discord",
        python_callable=_notify_discord,
        provide_context=True,
    )

    skip_notify = EmptyOperator(task_id="skip_notify")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> [bronze_itviec, bronze_topcv] >> ingestion_bronze_done >> bronze_done
    bronze_done >> validate_bronze >> silver_transform >> gold_aggregate
    gold_aggregate >> check_alerts >> [notify_discord, skip_notify] >> end
