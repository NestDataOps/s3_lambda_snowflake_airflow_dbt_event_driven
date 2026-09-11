# Event-Driven S3 → Snowflake → dbt Pipeline

An Airflow-centric, event-triggered data pipeline. A file landing in S3
kicks off a Lambda-based cleaning step; Airflow senses the result, loads it
into Snowflake, and runs dbt to produce analytics-ready models.

## Architecture

```
File Uploaded
     │
     ▼
[ Amazon S3 raw/ ] ──(EventBridge: Object Created)──> [ Lambda (pandas/pyarrow) ]
                                                              │
                                              (flattens JSON, writes Parquet)
                                                              │
                                                              ▼
                                                [ Amazon S3 processed/ ]
                                                              │
                                          Airflow senses file (deferrable S3KeySensor)
                                                              │
                                                              ▼
                                             [ Snowflake RAW.RAW_EVENTS ]
                                                     (COPY INTO from
                                                   external stage/storage
                                                       integration)
                                                              │
                                                        dbt run + test
                                                   (staging → marts)
                                                              │
                                                              ▼
                                          [ Snowflake ANALYTICS.* (dbt models) ]
```

## Why these choices

- **Lambda instead of Glue** for the cleaning step: a single JSON file per
  event doesn't need Spark, and Glue's 5-10 min cold start is painful to
  iterate against. Swap the Lambda for Glue if files grow into the GB+
  range or you need distributed joins.
- **Airflow senses the processed file** rather than a second EventBridge →
  Lambda hop, so the whole downstream lineage (load → transform → test)
  lives in one DAG with retries, backfills, and observability in one place
  — the point of using an orchestrator instead of a pure event chain.
- **`COPY INTO` via Airflow, not Snowpipe**, for the same reason: keeping
  ingestion inside the DAG (rather than an out-of-band Snowpipe) makes the
  whole raw → analytics lineage visible and testable in Airflow/dbt.
- **dbt Core via `BashOperator`**, not dbt Cloud: no extra paid service,
  and it mirrors how most teams without a dbt Cloud license actually run
  dbt in production.

## Repo layout

```
terraform/          # AWS + Snowflake infra (reuses patterns from the
                     # actions/terraform/ansible project)
  modules/
    s3/              raw + processed buckets, EventBridge notifications enabled
    iam/              lambda exec role, snowflake storage-integration role
    lambda/           packages + deploys the cleaning function + layer
    eventbridge/      rule matching S3 Object Created on raw/, DLQ on failure
    snowflake/        warehouse/db/schema/stage/raw table
lambda/
  src/handler.py       flattens JSON -> Parquet
  build_layer.sh       builds the pandas/pyarrow layer via Docker
airflow/
  dags/s3_to_snowflake_dbt_dag.py
dbt/
  models/staging/      typed view over the raw VARIANT payload
  models/marts/         daily aggregate example
scripts/
  generate_sample_events.py   test-data generator, can upload straight to S3
terraform-bootstrap/
  main.tf                     one-time S3+lock remote state backend,
                               shared with your other Terraform project
```

## Setup

## Snowflake credentials (never in `main.tf`)
The `provider "snowflake" {}` block only sets `role` -- everything
sensitive comes from environment variables Terraform reads at apply time.
Key-pair auth is recommended over a password since it's what you'd use in
CI anyway:
```bash
# generate a key pair (once)
cd ~/.ssh
openssl genrsa -out snowflake_key.p8 4096
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```
Register the public key on the Snowflake user (`ALTER USER <user> SET
RSA_PUBLIC_KEY='...'`), then export before running Terraform:
```bash
export SNOWFLAKE_ORGANIZATION_NAME="your-org"
export SNOWFLAKE_ACCOUNT_NAME="your-account"
export SNOWFLAKE_USER="terraform_user"
export SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT"
export SNOWFLAKE_PRIVATE_KEY="$(cat ~/.ssh/snowflake_key.p8)"
```
`SNOWFLAKE_ORGANIZATION_NAME` and `SNOWFLAKE_ACCOUNT_NAME` are two
*separate* values, not the combined `org-account` string you may be used
to typing into the classic Snowflake login URL. Find both with:
```sql
SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();
```
If you'd rather use a password for a quick local test, `SNOWFLAKE_USER` +
`SNOWFLAKE_PASSWORD` also work, but don't use that in anything you'd call
CI-ready. Either way, these are exported in your shell/CI secrets --
never written into any `.tf` file, and `profiles.yml` (dbt's separate
credential file) should stay out of git too (already gitignored below).

### 1. Build the Lambda layer
Pandas/pyarrow need to be compiled for the Lambda runtime, not your local
OS:
```bash
cd lambda
./build_layer.sh
```

### 2. First Terraform apply (two-step for the Snowflake storage integration)
Snowflake storage integrations have a chicken-and-egg dependency with the
IAM role's trust policy. On the first apply, the role trusts *your own*
AWS account (a placeholder that's still valid, unlike a fake account ID)
so `CreateRole` doesn't get rejected outright:
```bash
cd terraform
terraform init
terraform apply   # role self-trusts your account; everything else builds fully
```
Then in Snowflake:
```sql
DESC STORAGE INTEGRATION EVENTDRIVEN_PIPELINE_S3_INTEGRATION;
-- copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
```
Pass those back in as vars (or set them in
`terraform/modules/iam/variables.tf` defaults) and apply again -- this
swaps the trust policy's principal from "yourself" to the real Snowflake
IAM user:
```bash
terraform apply \
  -var="snowflake_storage_aws_iam_user_arn=<STORAGE_AWS_IAM_USER_ARN>" \
  -var="snowflake_external_id=<STORAGE_AWS_EXTERNAL_ID>"
```

### 3. Airflow
- Add an `aws_default` connection and a `snowflake_default` connection
  (account, user, password/key-pair, role, warehouse, database) in the
  Airflow UI or via `airflow connections add`.
- Mount/copy the `dbt/` directory to `/opt/airflow/dbt` on your workers
  (matches `DBT_PROJECT_DIR` in the DAG).
- Copy `dbt/profiles.yml.example` → `dbt/profiles.yml`, or set the
  `SNOWFLAKE_*` env vars it references.
- Trigger the DAG manually to test, then wire it to fire automatically:
  either add a short polling `schedule` (the sensor no-ops if nothing's
  new) or have the Lambda call the Airflow REST API's
  `dagRuns` endpoint after it writes the Parquet file, passing the S3 key
  as a `conf` param.

### 4. Test the pipeline
Drop a JSON file into `s3://<raw-bucket>/raw/`:
```bash
aws s3 cp sample-event.json s3://eventdriven-pipeline-raw/raw/sample-event.json
```
Watch CloudWatch Logs for the Lambda, then trigger (or wait on) the
Airflow DAG and check `ANALYTICS.ANALYTICS_EVENTS_DAILY` in Snowflake.

## Sample event shape
The Lambda expects JSON like:
```json
{
  "event_id": "evt_001",
  "event_type": "purchase",
  "user_id": "u_123",
  "amount": 42.50,
  "currency": "USD",
  "created_at": "2026-09-01T10:00:00Z"
}
```
Nested objects/arrays are flattened automatically via
`pandas.json_normalize`.

## Generating test data
Instead of hand-writing sample events:
```bash
cd scripts
pip install boto3
python generate_sample_events.py --count 5
# or, to upload straight to S3 and kick off the real pipeline:
python generate_sample_events.py --count 5 --upload --bucket eventdriven-pipeline-raw
```
Each generated file is a JSON array of 1-20 records with nested
`metadata`/`address` objects, so it also exercises the `pandas.json_normalize`
flattening in the Lambda rather than just flat records.

## Shared remote state backend (links this repo with your CI/CD project)
`terraform-bootstrap/` is a standalone module — apply it once, separately
from everything else:
```bash
cd terraform-bootstrap
terraform init
terraform apply -var="state_bucket_name=<something-globally-unique>"
```
Take the `state_bucket_name` and `lock_table_name` outputs and:
1. Update the `backend "s3"` block in `terraform/main.tf` (this repo) with
   the real bucket name, then `cd terraform && terraform init` to migrate
   from local state to S3.
2. Do the same in your actions/terraform/ansible/airflow/dbt project's
   backend block, using a **different** `key` (e.g.
   `key = "ci-cd-infra/terraform.tfstate"`) so the two projects' state
   files don't collide in the same bucket.

Both repos then share one governed state backend with locking — worth
mentioning explicitly if you write these up together, since it's the kind
of platform-level decision a real infra team makes once and reuses.
# s3_lambda_snowflake_airflow_dbt_event_driven
