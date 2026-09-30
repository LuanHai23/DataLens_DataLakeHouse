# TopCV hybrid ingestion runbook

## Status

The VNJobs AWS MVP intentionally uses hybrid ingestion for TopCV:

- ITviec ingestion can run on AWS ECS Fargate.
- TopCV is crawled locally from an authorized user session and trusted network.
- The resulting `topcv_jobs.json` is validated and uploaded manually to the
  versioned Bronze S3 contract.
- Bronze validation, Silver transformation, Gold aggregation, Glue Catalog,
  Athena, Step Functions, and SNS remain AWS-managed.

The EventBridge daily schedule must remain `DISABLED` while this manual raw
input gate exists.

## Why TopCV is not crawled from AWS

The controlled ECS canary on 2026-09-30 established all of the following:

- ECS task definition revision `vnjobs-crawler-dev:4` ran the intended image.
- TopCV cookies were injected from AWS Secrets Manager.
- Cookie application did not fail.
- Camoufox started successfully.
- Cloudflare returned a blocked page.
- No TopCV job cards were visible and no raw S3 object was created.
- The same local cookie set could access visible TopCV job cards from the
  local network.

Authentication cookies alone do not make a browser session portable. A
Cloudflare clearance decision can also depend on the visitor, device, network,
browser/TLS fingerprint, and session behavior. Copying browser cookies from a
local residential session to an AWS datacenter environment therefore does not
guarantee that Cloudflare will accept the AWS request.

The project does not automate TopCV account login, store account passwords in
AWS, or repeatedly attempt to bypass the site's anti-bot controls.

## Security contract

The manual uploader:

- never reads cookie files;
- never prints job records;
- rejects empty or malformed JSON;
- requires a non-empty JSON array of objects;
- rejects cookie, password, authorization, and session-token keys;
- verifies the AWS account before upload;
- verifies that Bronze S3 versioning is enabled;
- uses the deterministic raw key expected by downstream jobs;
- writes AES256-encrypted objects;
- records a SHA-256 checksum and ingestion metadata;
- refuses to replace a different existing payload without an explicit flag.

## Prerequisites

1. Use the repository branch and virtual environment normally used for the
   project.
2. Authenticate the AWS CLI as account `490258149044` in
   `ap-southeast-1`.
3. Generate or export a local `topcv_jobs.json` using the existing local
   TopCV crawler flow.
4. Confirm that the JSON file contains jobs only, never cookies or account
   credentials.

## 1. Validate without calling AWS

PowerShell:

```powershell
python scripts/aws/upload_topcv_raw.py `
  --input "C:\path\to\topcv_jobs.json" `
  --batch-date "2026-09-30" `
  --dry-run
```

Expected markers include:

```text
TOPCV_MANUAL_INPUT_VALIDATION=PASS
AWS_MUTATION_PERFORMED=NO
TOPCV_MANUAL_UPLOAD_DRY_RUN=PASS
```

The command prints only record count, byte count, checksum, and target path.
It does not print job contents.

## 2. Upload the first payload for a batch date

```powershell
python scripts/aws/upload_topcv_raw.py `
  --input "C:\path\to\topcv_jobs.json" `
  --batch-date "2026-09-30"
```

The deterministic destination is:

```text
s3://vnjobs-data-pipeline-bronze-ap-southeast-1-dev/raw/source=topcv/batch_date=2026-09-30/topcv_jobs.json
```

Expected markers include:

```text
AWS_ACCOUNT_CHECK=PASS
S3_VERSIONING_CHECK=PASS
S3_OBJECT_VERIFICATION=PASS
TOPCV_MANUAL_UPLOAD=PASS
```

## 3. Upload a reviewed correction

If the same key already contains different data, the uploader stops by
default. After reviewing the corrected file, explicitly create a new S3
version:

```powershell
python scripts/aws/upload_topcv_raw.py `
  --input "C:\path\to\topcv_jobs.json" `
  --batch-date "2026-09-30" `
  --allow-new-version
```

Uploading an identical payload is idempotent and returns
`UPLOAD_STATUS=SKIPPED_IDENTICAL`.

## 4. Confirm both raw sources before orchestration

For the selected batch date, both objects must exist:

```text
raw/source=itviec/batch_date=YYYY-MM-DD/itviec_jobs.json
raw/source=topcv/batch_date=YYYY-MM-DD/topcv_jobs.json
```

Only after both objects are present should the Step Functions execution be
started with that `batch_date`.

## Accepted MVP limitation

TopCV ingestion is not unattended in this MVP because the source applies
anti-bot controls that reject AWS-originated browser traffic even when cookies
are delivered securely. This is an explicit operational boundary, not a
failure of the S3, Secrets Manager, ECS, Glue, Step Functions, or Athena
architecture.
