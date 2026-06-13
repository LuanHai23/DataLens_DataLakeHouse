# Data Quality Rules

This document describes the data quality rules used in the DataLens lakehouse pipeline.

Data quality validation is applied to ensure that crawled job postings are reliable enough to be promoted from raw data into cleaned Silver tables and business-ready Gold marts.

---

## 1. Validation Scope

Data quality checks are applied mainly to:

```text
Bronze Layer → raw crawled job data
Silver Layer → cleaned and standardized job listings
Gold Layer → aggregated analytics marts
```

The main validation tool used in this project is:

```text
Great Expectations
```

---

## 2. Bronze Layer Quality Checks

The Bronze layer stores raw job postings collected from sources such as ITviec and TopCV.

Expected Bronze checks:

| Rule                                       | Description                                             | Severity |
| ------------------------------------------ | ------------------------------------------------------- | -------- |
| Raw dataset must not be empty              | Each crawler run should produce at least one record     | Critical |
| `url` should exist                         | Raw job listing should contain a job URL                | Critical |
| `title` should exist                       | Raw job listing should contain a job title              | Critical |
| `source` should exist                      | Each record must indicate its source                    | Critical |
| Raw record count should be above threshold | Helps detect crawler blocking or HTML structure changes | Warning  |

---

## 3. Silver Layer Quality Checks

The Silver layer contains cleaned, standardized, and deduplicated job listings.

Expected Silver checks:

| Rule                                          | Description                                   | Severity |
| --------------------------------------------- | --------------------------------------------- | -------- |
| `url` must not be null                        | URL is the main logical key                   | Critical |
| `title` must not be null                      | Job title is required for analytics           | Critical |
| `source` must be valid                        | Source must be `itviec` or `topcv`            | Critical |
| `location_std` must not be null               | Location is required for market analytics     | Critical |
| `url` should be unique                        | Prevent duplicated job postings               | Critical |
| `min_salary` should be numeric when present   | Required for salary analytics                 | Warning  |
| `max_salary` should be numeric when present   | Required for salary analytics                 | Warning  |
| `min_salary <= max_salary`                    | Salary range must be logically valid          | Warning  |
| `currency` should be valid when salary exists | Currency should be `USD`, `VND`, or `UNKNOWN` | Warning  |

---

## 4. Gold Layer Quality Checks

The Gold layer contains business-ready marts used by Trino, PostgreSQL serving tables, Metabase dashboards, and Discord alerts.

Expected Gold checks:

| Table                      | Rule                                 | Description                                         |
| -------------------------- | ------------------------------------ | --------------------------------------------------- |
| `mart_job_market_overview` | `total_jobs > 0`                     | Dashboard must have valid market overview data      |
| `mart_source_performance`  | `job_count > 0`                      | Each active source should produce records           |
| `mart_skill_demand`        | `job_count > 0`                      | Skill demand chart should contain meaningful values |
| `mart_high_salary_alerts`  | Salary threshold logic must be valid | Alerts should only include high-salary jobs         |
| `mart_pipeline_health`     | Status must be `success` or `failed` | Used for operational monitoring                     |

---

## 5. Example Great Expectations Rules

Example expectations that can be implemented:

```python
expect_column_values_to_not_be_null("url")
expect_column_values_to_not_be_null("title")
expect_column_values_to_not_be_null("source")
expect_column_values_to_be_in_set("source", ["itviec", "topcv"])
expect_column_values_to_not_be_null("location_std")
expect_column_values_to_be_unique("url")
expect_column_values_to_be_between("min_salary", min_value=0)
expect_column_values_to_be_between("max_salary", min_value=0)
expect_column_pair_values_A_to_be_greater_than_B(
    column_A="max_salary",
    column_B="min_salary",
    or_equal=True
)
```

---

## 6. Data Quality Failure Handling

When data quality validation fails:

| Failure Type             | Handling Strategy                                             |
| ------------------------ | ------------------------------------------------------------- |
| Empty crawler result     | Mark task as failed and retry through Airflow                 |
| Missing critical columns | Stop promotion to Silver                                      |
| Duplicate URLs           | Deduplicate before writing Silver                             |
| Invalid salary values    | Set parsed salary fields to null or exclude from salary marts |
| Invalid source value     | Quarantine or exclude invalid records                         |
| Dashboard mart is empty  | Keep previous successful output and investigate source issue  |

---

## 7. Quality Metrics to Monitor

The pipeline should monitor:

```text
total_records
valid_records
invalid_records
duplicate_url_count
missing_title_count
missing_company_count
missing_salary_count
quality_score
```

These metrics can later be stored in a Gold monitoring table such as:

```text
demo.gold.mart_data_quality_summary
analytics.mart_data_quality_summary
```

---

## 8. Current Project Status

Current implementation already includes Great Expectations as the validation component in the architecture.

Future improvement:

```text
Add a dedicated mart_data_quality_summary table and publish it to PostgreSQL for Metabase monitoring.
```
