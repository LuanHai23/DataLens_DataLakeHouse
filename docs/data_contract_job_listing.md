# Data Contract: Job Listing Schema

This document defines the expected schema for job listing data used in the DataLens lakehouse pipeline.

The purpose of this data contract is to ensure that crawled job data from multiple sources such as ITviec and TopCV can be standardized, validated, transformed, and consumed consistently across Bronze, Silver, Gold, PostgreSQL serving tables, Metabase dashboards, and Discord alerts.

---

## 1. Data Source

Current supported sources:

| Source   | Description                           |
| -------- | ------------------------------------- |
| `itviec` | IT job postings collected from ITviec |
| `topcv`  | IT job postings collected from TopCV  |

Each source may have different raw HTML structures and field formats, but the Silver layer must normalize them into a unified job listing schema.

---

## 2. Silver Layer Contract

The Silver layer represents cleaned and standardized job listing records.

Expected table:

```text
demo.silver.jobs
```

### Required Columns

| Column         | Type      | Required | Description                                                                  |
| -------------- | --------- | -------: | ---------------------------------------------------------------------------- |
| `source`       | string    |      Yes | Data source name, for example `itviec` or `topcv`                            |
| `keyword`      | string    |       No | Search keyword used during crawling                                          |
| `title`        | string    |      Yes | Job title                                                                    |
| `company`      | string    |       No | Hiring company name                                                          |
| `url`          | string    |      Yes | Unique job posting URL                                                       |
| `location_std` | string    |      Yes | Standardized location such as `Ho Chi Minh`, `Ha Noi`, `Da Nang`, or `Other` |
| `work_type`    | string    |       No | Work arrangement or job type when available                                  |
| `salary_raw`   | string    |       No | Original salary text extracted from the source                               |
| `min_salary`   | double    |       No | Parsed minimum salary value                                                  |
| `max_salary`   | double    |       No | Parsed maximum salary value                                                  |
| `currency`     | string    |       No | Salary currency such as `VND`, `USD`, or `UNKNOWN`                           |
| `tags`         | string    |       No | Raw or normalized skill tags                                                 |
| `posted`       | string    |       No | Original posted date text                                                    |
| `created_at`   | timestamp |       No | Processing timestamp when the record is created                              |

---

## 3. Primary Key Rule

The pipeline treats the following fields as the logical key for deduplication and upsert:

```text
url
```

For partitioned Gold tables, the merge key often includes:

```text
url, report_date
```

This prevents duplicate job postings from being inserted repeatedly during daily pipeline runs.

---

## 4. Required Validation Rules

The following rules must hold before data is promoted to analytics marts:

| Rule                                      | Description                                                  |
| ----------------------------------------- | ------------------------------------------------------------ |
| `url` must not be null                    | Each job posting must have a unique URL                      |
| `title` must not be null                  | A job record without title is not useful for analytics       |
| `source` must be valid                    | Source must belong to the supported source list              |
| `location_std` must not be null           | Location is required for market analytics                    |
| Salary fields must be numeric when parsed | `min_salary` and `max_salary` must be numeric when available |
| `min_salary <= max_salary`                | Salary range must be logically valid                         |
| Duplicate URLs should be removed          | The same job URL should not appear multiple times in Silver  |

---

## 5. Gold Layer Output Contract

Gold tables are designed as business-ready data products.

Expected Gold marts:

| Table                                 | Purpose                                            |
| ------------------------------------- | -------------------------------------------------- |
| `demo.gold.mart_job_market_overview`  | Executive overview KPIs                            |
| `demo.gold.mart_source_performance`   | Job volume and salary availability by source       |
| `demo.gold.mart_salary_by_location`   | Salary analytics by location, source, and currency |
| `demo.gold.mart_company_hiring_trend` | Top hiring companies                               |
| `demo.gold.mart_skill_demand`         | Skill demand analytics                             |
| `demo.gold.mart_high_salary_alerts`   | High-salary job opportunities                      |
| `demo.gold.mart_pipeline_health`      | Pipeline monitoring output                         |

---

## 6. Serving Layer Contract

Selected Gold marts are published to PostgreSQL for dashboard consumption.

Expected PostgreSQL schema:

```text
analytics
```

Expected serving tables:

```text
analytics.mart_job_market_overview
analytics.mart_source_performance
analytics.mart_salary_by_location
analytics.mart_company_hiring_trend
analytics.mart_skill_demand
analytics.mart_high_salary_alerts
analytics.mart_pipeline_health
```

The PostgreSQL serving layer exists to provide BI-friendly relational tables for Metabase, while Apache Iceberg remains the lakehouse source of truth.

---

## 7. Contract Ownership

This contract should be updated when:

* A new crawler source is added
* A new required field is introduced
* Salary parsing logic changes
* Location normalization logic changes
* Gold mart schemas change
* Dashboard requirements change
