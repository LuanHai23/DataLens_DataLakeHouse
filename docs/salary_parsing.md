# Salary Parsing Documentation

This document explains how salary text from job postings is converted into structured salary fields for analytics.

Salary parsing is important because raw job posting data often contains inconsistent formats such as monthly ranges, negotiable salaries, currency symbols, mixed languages, and missing salary values.

---

## 1. Input Field

The raw salary text is stored in:

```text
salary_raw
```

Examples of possible raw salary values:

```text
"Up to $2,000"
"$1,500 - $3,000"
"20,000,000 - 35,000,000 VND"
"Thỏa thuận"
"Negotiable"
"Không hiển thị"
```

---

## 2. Output Fields

The salary parser produces three normalized fields:

| Column       | Type   | Description                                               |
| ------------ | ------ | --------------------------------------------------------- |
| `min_salary` | double | Parsed minimum salary value                               |
| `max_salary` | double | Parsed maximum salary value                               |
| `currency`   | string | Parsed salary currency such as `USD`, `VND`, or `UNKNOWN` |

---

## 3. Parsing Rules

### Rule 1: Missing or Negotiable Salary

If the salary text indicates that salary is not available or negotiable, both salary values should be null.

Examples:

```text
"Thỏa thuận"
"Negotiable"
"Không hiển thị"
"Salary not shown"
```

Expected output:

| min_salary | max_salary | currency  |
| ---------: | ---------: | --------- |
|       null |       null | `UNKNOWN` |

---

### Rule 2: Salary Range

If the salary contains a range, extract both minimum and maximum values.

Example:

```text
"$1,500 - $3,000"
```

Expected output:

| min_salary | max_salary | currency |
| ---------: | ---------: | -------- |
|       1500 |       3000 | `USD`    |

Example:

```text
"20,000,000 - 35,000,000 VND"
```

Expected output:

| min_salary | max_salary | currency |
| ---------: | ---------: | -------- |
|   20000000 |   35000000 | `VND`    |

---

### Rule 3: Upper Bound Salary

If the salary text contains phrases such as `up to`, `tới`, or `lên đến`, the value should be treated as the maximum salary.

Example:

```text
"Up to $2,000"
```

Expected output:

| min_salary | max_salary | currency |
| ---------: | ---------: | -------- |
|       null |       2000 | `USD`    |

---

### Rule 4: Lower Bound Salary

If the salary text contains phrases such as `from`, `từ`, or `starting from`, the value should be treated as the minimum salary.

Example:

```text
"From $1,000"
```

Expected output:

| min_salary | max_salary | currency |
| ---------: | ---------: | -------- |
|       1000 |       null | `USD`    |

---

### Rule 5: Currency Detection

Currency is inferred from the salary text.

| Pattern                                   | Currency  |
| ----------------------------------------- | --------- |
| `$`, `USD`, `usd`                         | `USD`     |
| `VND`, `VNĐ`, `₫`, `triệu`, `million VND` | `VND`     |
| No recognizable currency                  | `UNKNOWN` |

---

## 4. High-Salary Alert Rule

The Gold layer generates high-salary alerts using the following business rule:

```text
USD salary: min_salary >= 1000
VND salary: min_salary >= 20,000,000
```

These records are stored in:

```text
demo.gold.mart_high_salary_alerts
analytics.mart_high_salary_alerts
```

They are used by:

* Metabase dashboard
* Discord high-salary job alerts

---

## 5. Known Limitations

Current salary parsing may not fully handle all edge cases, such as:

* Annual salary versus monthly salary
* Gross versus net salary
* Salary values written entirely in Vietnamese words
* Mixed currencies in the same text
* Benefits mentioned together with salary

These cases can be improved in future versions by adding more parsing rules and unit tests.

---

## 6. Testing Strategy

Salary parsing logic should be covered by unit tests for:

* USD salary ranges
* VND salary ranges
* Negotiable salary
* Missing salary
* Upper-bound salary
* Lower-bound salary
* Invalid salary text
* `min_salary <= max_salary` validation
