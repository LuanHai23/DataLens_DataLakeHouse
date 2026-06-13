# Backfill and Retry Strategy

This document describes how the DataLens pipeline handles crawler failures, source instability, and historical reprocessing.

Web data ingestion is unstable by nature because job websites may change HTML structure, block crawlers, return empty pages, or temporarily fail. Therefore, a production-like data pipeline needs retry and backfill strategies.

---

## 1. Why Retry and Backfill Are Needed

The pipeline ingests job postings from:

```text
ITviec
TopCV
```

Possible failure scenarios:

| Failure Scenario       | Example                                |
| ---------------------- | -------------------------------------- |
| Website blocks crawler | Cloudflare or bot detection            |
| HTML structure changes | CSS selector no longer works           |
| Network timeout        | Source website temporarily unavailable |
| Empty crawl result     | Search page returns no jobs            |
| MinIO upload failure   | Object storage unavailable             |
| Spark job failure      | Transformation or schema issue         |
| Dashboard data missing | Gold marts were not refreshed          |

---

## 2. Retry Strategy

Airflow is responsible for orchestration-level retry.

Recommended retry configuration:

```python
retries=2
retry_delay=timedelta(minutes=5)
```

This means if a crawler or processing task fails, Airflow will retry it automatically before marking the DAG run as failed.

---

## 3. Source-level Isolation

Each source should be crawled independently.

Recommended DAG design:

```text
crawl_itviec
crawl_topcv
     ↓
bronze_validation
     ↓
silver_transform
     ↓
gold_aggregate
     ↓
publish_to_postgres
     ↓
discord_alert
```

If one source fails, the other source can still produce data.

For example:

```text
ITviec failed
TopCV succeeded
```

The pipeline can still continue with TopCV data, while ITviec failure is logged for investigation.

---

## 4. Backfill Strategy

Backfill means rerunning the pipeline for a previous execution date.

The pipeline should support rerunning historical dates using Airflow execution dates.

Example use cases:

| Backfill Case                      | Reason                          |
| ---------------------------------- | ------------------------------- |
| Rerun yesterday's crawler          | Website was temporarily blocked |
| Reprocess Silver                   | Cleaning logic changed          |
| Rebuild Gold marts                 | Business metric logic changed   |
| Republish PostgreSQL serving layer | Dashboard tables need refresh   |

---

## 5. Idempotent Writes

The pipeline should be idempotent, meaning rerunning the same date should not create duplicated records.

Current strategy:

```text
Iceberg Gold tables use MERGE INTO with merge keys.
```

Examples:

```text
daily_alerts: url + report_date
market_summary: location_std + currency + report_date
mart_skill_demand: skill + source + location_std + report_date
mart_pipeline_health: pipeline_name + report_date
```

This allows the same task to be rerun safely.

---

## 6. Bronze Backfill

Bronze data should be stored with date-based paths.

Recommended path pattern:

```text
s3a://data-lake/bronze/source=<source>/year=<yyyy>/month=<mm>/day=<dd>/
```

Benefits:

```text
easy historical replay
source-level isolation
partition pruning
auditable raw data
```

---

## 7. Silver and Gold Backfill

Silver and Gold tables should support reruns by partition.

Recommended partition field:

```text
report_date
```

When a historical run is needed, the pipeline can reprocess only the affected date instead of rebuilding the entire lakehouse.

---

## 8. PostgreSQL Serving Layer Refresh

The PostgreSQL serving layer is used by Metabase.

Current strategy:

```text
Gold marts are published to PostgreSQL analytics schema after Gold aggregation.
```

Recommended refresh mode:

| Table Type                | Write Mode                   |
| ------------------------- | ---------------------------- |
| Small KPI marts           | overwrite                    |
| Dashboard aggregate marts | overwrite                    |
| Job-level alert tables    | overwrite or upsert          |
| Historical mart tables    | append/upsert by report_date |

For the current portfolio project, overwrite mode is acceptable because the tables are small and dashboard-ready.

---

## 9. Operational Monitoring

The pipeline records run-level health in:

```text
demo.gold.mart_pipeline_health
analytics.mart_pipeline_health
```

This table helps monitor:

```text
pipeline_name
status
records_in
report_date
created_at
```

Future improvement:

```text
Add failed run tracking, error messages, retry counts, and source-level status.
```

---

## 10. Recovery Playbook

If the pipeline fails:

### Case 1: Crawler returns empty data

Action:

```text
Check website availability
Check cookie/session validity
Check crawler selector
Rerun crawler task
```

### Case 2: MinIO upload fails

Action:

```text
Check MinIO container health
Check bucket existence
Check endpoint configuration
Rerun ingestion task
```

### Case 3: Silver transform fails

Action:

```text
Check Bronze schema
Check missing fields
Check parsing functions
Rerun Silver task
```

### Case 4: Gold aggregation fails

Action:

```text
Check Silver table availability
Check Iceberg catalog connection
Check merge keys and table schema
Rerun Gold task
```

### Case 5: PostgreSQL serving publish fails

Action:

```text
Check PostgreSQL container
Check warehouse_db database
Check analytics schema
Check PostgreSQL JDBC driver
Rerun Gold publish step
```

---

## 11. Current Project Status

Current implementation includes:

```text
Airflow orchestration
Task retry capability
Iceberg MERGE INTO for idempotent Gold writes
PostgreSQL serving layer refresh
Pipeline health mart
```

Future improvement:

```text
Add parameterized backfill by execution date and source-level failure isolation.
```
