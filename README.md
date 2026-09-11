# Event-Driven S3 → Snowflake → dbt Pipeline

A genuinely event-driven data pipeline: a file landing in S3 triggers a
Lambda cleaning step, which writes Parquet and calls Airflow's REST API
directly to kick off a DAG run — no polling. Airflow (running on a plain
EC2 box, no Docker) loads the file into Snowflake and runs dbt to produce
analytics-ready models.

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
                                            Lambda calls Airflow's REST API
                                              directly (POST .../dagRuns,
                                              conf = exact S3 key just written)
                                                              │
                                                              ▼
                                    [ Airflow on EC2 (systemd, no Docker) ]
                                       S3KeySensor confirms the file, then:
                                                              │
                                                              ▼
                                             [ Snowflake RAW.RAW_EVENTS ]
                                                     (COPY INTO from
                                                   external stage/storage
                                                       integration)
                                                              │
                                                        dbt run + test
                                                   (staging → marts,
                                                    dedup'd on event_id)
                                                              │
                                                              ▼
                                          [ Snowflake ANALYTICS.* (dbt models) ]
```

## Why these choices

- **Lambda instead of Glue** for the cleaning step: a single JSON file per
  event doesn't need Spark, and Glue's 5-10 min cold start is painful to
  iterate against. Swap the Lambda for Glue if files grow into the GB+
  range or you need distributed joins.
- **Lambda triggers Airflow directly via its REST API**, rather than
  Airflow polling S3 on a schedule. Genuinely event-driven: a file landing
  produces a DAG run within seconds, not "sometime in the next 5 minutes."
  The callback is non-fatal on failure (network blip, EC2 down for
  maintenance) — the file's already safely written either way, so a
  missed callback just means "needs a manual trigger," not lost data. See
  "How the event trigger actually works" below for the security tradeoff
  this involves.
- **Airflow on EC2, no Docker** — a venv + three systemd services
  (webserver, scheduler, triggerer). More setup than Docker Compose would
  have been, but nothing hidden behind an image someone else built; every
  dependency and config choice is visible in `ansible/playbook.yml`.
- **`COPY INTO` via Airflow, not Snowpipe**: keeping ingestion inside the
  DAG (rather than an out-of-band Snowpipe) makes the whole raw → analytics
  lineage visible and testable in Airflow/dbt, in one place, with retries
  and observability that a pure event chain wouldn't give you.
- **dbt Core via a separate venv, called by full path from `BashOperator`**
  — not dbt Cloud (no extra paid service), and not sharing Airflow's own
  venv (Airflow's dependency constraints and dbt-snowflake's conflict
  directly if forced to coexist — see comments in `playbook.yml`).
- **Dedup happens in dbt, not at the load layer.** Snowflake's `COPY INTO`
  dedup is per-*file* (path + checksum), not per-*event* — the same event
  content re-staged under a different filename (a retry, a re-upload)
  loads again as a "new" file. `stg_events.sql` guards against this with
  `qualify row_number() ... partition by event_id`, so the analytics layer
  stays correct regardless of how raw-layer duplicates occur.
- **S3 native locking (`use_lockfile`), not DynamoDB**, for Terraform state
  — see "Shared remote state backend" below.

## Repo layout

```
terraform/          # AWS + Snowflake infra (reuses patterns from the
                     # actions/terraform/ansible project)
  modules/
    s3/              raw + processed buckets, EventBridge notifications enabled
    iam/              lambda exec role, snowflake storage-integration role
    lambda/           packages + deploys the cleaning function + layer
    eventbridge/      rule matching S3 Object Created on raw/, DLQ on failure
    ec2_airflow/       t3a.small host: security group, IAM instance role, key pair
    snowflake/        warehouse/db/schema/stage/raw table, TRANSFORMER role + grants
lambda/
  src/handler.py       flattens JSON -> Parquet, then triggers the Airflow DAG
  build_layer.sh       fallback: builds a custom layer (not used by default --
                       see pandas_layer_arn, AWS's managed layer is used instead)
airflow/
  dags/s3_to_snowflake_dbt_dag.py
  dbt/
    models/staging/    typed + deduplicated view over the raw VARIANT payload
    models/marts/       daily aggregate example
ansible/
  playbook.yml         installs Airflow (webserver/scheduler/triggerer as
                       systemd services, no Docker) + dbt on the EC2 host
  templates/            systemd units, dbt profiles.yml, env file
  group_vars/airflow.yml.example   credentials template (vault this)
scripts/
  generate_sample_events.py   test-data generator, can upload straight to S3
terraform-bootstrap/
  main.tf                     one-time S3 remote state backend (native
                               locking, no DynamoDB), shared with your
                               other Terraform project
```

## Setup

### Snowflake credentials (never in `main.tf`)
Nothing sensitive is hardcoded. Two different mechanisms are in play
though, so it's worth being precise:

- **`organization_name` / `account_name`**: passed explicitly via
  Terraform variables (`var.snowflake_organization_name` /
  `var.snowflake_account_name`), not auto-discovered from env vars —
  relying on the provider to find `SNOWFLAKE_*`-named env vars silently
  breaks if you're in a fresh shell where they were never re-exported.
  ```bash
  export TF_VAR_snowflake_organization_name="your-org"
  export TF_VAR_snowflake_account_name="your-account"
  export TF_VAR_snowflake_transformer_user="your-airflow-dbt-username"
  ```
  Find the first two with:
  ```sql
  SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();
  ```
  These are two *separate* values, not the combined `org-account` string
  from the classic Snowflake login URL.

- **Key-pair auth for the Terraform provider itself** — generate once:
  ```bash
  cd ~/.ssh
  openssl genrsa -out snowflake_key.p8 4096
  openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
  ```
  Register the public key (base64 body only, no header/footer/line
  breaks — the single most common cause of "Invalid Public key" errors):
  ```bash
  grep -v "PUBLIC KEY" ~/.ssh/snowflake_key.pub | tr -d '\n'
  ```
  ```sql
  ALTER USER <your_username> SET RSA_PUBLIC_KEY='<that output>';
  ```
  Then export:
  ```bash
  export SNOWFLAKE_USER="terraform_user"
  export SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT"
  export SNOWFLAKE_PRIVATE_KEY="$(cat ~/.ssh/snowflake_key.p8)"
  ```

- **Lambda's Airflow API credentials** — must match `airflow_admin_user` /
  `airflow_admin_password` in `ansible/group_vars/airflow.yml` *exactly*,
  since that's what actually creates the login on the Airflow side.
  Terraform and Ansible don't share state, so these two have to be kept
  in sync by hand:
  ```bash
  export TF_VAR_airflow_api_user="admin"
  export TF_VAR_airflow_api_password="<same value as ansible's airflow_admin_password>"
  ```

- **EC2 access**:
  ```bash
  export TF_VAR_ssh_public_key="$(cat ~/.ssh/id_ed25519.pub)"
  export TF_VAR_allowed_cidr="$(curl -s ifconfig.me)/32"
  ```

Put all of these in one gitignored `env.sh` you `source` at the start of
each session — session-scoped exports getting lost between terminal
restarts is the single most common cause of confusing errors in this
setup, and it's not worth re-typing them by hand every time.

### 1. Terraform apply (also two-step, for the Snowflake storage integration)
This creates AWS infra (S3, Lambda, EventBridge, the EC2 host), *and* the
Snowflake side: warehouse/database/schemas/stage/raw table, plus the
`TRANSFORMER` role with every grant Airflow's `COPY INTO` and dbt need
(warehouse/database/schema USAGE, stage + file format USAGE, the raw
table's SELECT/INSERT, CREATE SCHEMA/TABLE/VIEW) — granted directly to
whichever user you set as `snowflake_transformer_user`.
```bash
cd terraform
terraform init
terraform apply
```
Storage integrations have a genuine chicken-and-egg dependency with the
IAM role's trust policy: on this first apply, the role self-trusts *your
own* AWS account (a real, valid placeholder) so `CreateRole` doesn't get
rejected outright. Then in Snowflake:
```sql
DESC STORAGE INTEGRATION EVENTDRIVEN_PIPELINE_S3_INTEGRATION;
-- copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
```
Apply again with those, which swaps the trust policy's principal from
"yourself" to the real Snowflake IAM user:
```bash
terraform apply \
  -var="snowflake_storage_aws_iam_user_arn=<STORAGE_AWS_IAM_USER_ARN>" \
  -var="snowflake_external_id=<STORAGE_AWS_EXTERNAL_ID>"
```

**Two Snowflake account-level privileges** `SYSADMIN` doesn't have by
default — grant these once, as `ACCOUNTADMIN`, before the first apply:
```sql
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;
GRANT CREATE ROLE ON ACCOUNT TO ROLE SYSADMIN;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SYSADMIN;
```

### 2. Lambda dependencies: use AWS's managed layer, don't build your own
A hand-built pandas+pyarrow layer reliably exceeds Lambda's 250MB
*unzipped* hard limit. This repo uses AWS's managed
`AWSSDKPandas-Python312` layer instead (pre-optimized to fit). Default in
`terraform/variables.tf` targets `ap-southeast-2`; for another region,
look up the right ARN at
https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html and pass it
via `-var="pandas_layer_arn=..."`.

`lambda/build_layer.sh` and `lambda/requirements.txt` are left in the repo
as a documented fallback in case you ever need a dependency the managed
layer doesn't include — not part of the default setup path.

### 3. Airflow on EC2 (Ansible, no Docker)
```bash
cd ansible
cp group_vars/airflow.yml.example group_vars/airflow.yml
# fill in: airflow_admin_password, snowflake_organization_name,
# snowflake_account_name, snowflake_user, snowflake_private_key_local_path
ansible-vault encrypt group_vars/airflow.yml
```
`snowflake_private_key_local_path` just points at your key FILE (e.g.
`~/.ssh/snowflake_key.p8`) — Ansible copies the actual file directly
rather than embedding its content as a YAML string, which sidesteps a
real gotcha: a PEM's literal newlines are very easy to mangle via
copy-paste into a YAML block scalar, and doing so produces confusing
downstream errors (`Unable to load PEM file`, `JWT token is invalid`)
rather than an obvious YAML syntax error.

`inventory.ini` is generated automatically by Terraform (the
`local_file` resource in `modules/ec2_airflow/outputs.tf`), pointing at
the instance's current public IP. Re-run `terraform apply` if you ever
stop/start the instance without an Elastic IP attached — the IP changes,
and both this inventory file and Lambda's `AIRFLOW_BASE_URL` go stale
until Terraform picks up the new value.

Run the playbook:
```bash
ansible-playbook playbook.yml --ask-vault-pass
```
This installs Airflow 2.9.3 + the Snowflake provider into one venv, and
`dbt-snowflake` into a **separate** venv (their dependency requirements
conflict if forced to share one), syncs `airflow/dags/` and `airflow/dbt/`
from this repo, renders `dbt/profiles.yml` + both Airflow connections from
your vaulted credentials (always deleted and recreated on every run, so
edits to `group_vars` actually take effect on re-apply), and starts three
systemd services: `airflow-webserver`, `airflow-scheduler`,
`airflow-triggerer`. The triggerer specifically is required because the
DAG's `S3KeySensor` runs `deferrable=True`.

```bash
terraform output airflow_public_ip
```
Open `http://<that-ip>:8080`, log in with `airflow_admin_user` /
`airflow_admin_password`.

**Metadata DB note**: SQLite + `SequentialExecutor` — fine for this DAG
(a single linear chain, event-triggered rather than highly concurrent) but
not something you'd want for many concurrent DAGs. Upgrade path: Postgres
on the same box (or RDS) + `LocalExecutor`.

### How the event trigger actually works
Lambda calls Airflow's REST API directly after writing each Parquet file
(`trigger_airflow_dag()` in `lambda/src/handler.py`), POSTing to
`/api/v1/dags/s3_to_snowflake_dbt/dagRuns` with the exact S3 key in
`conf`. The DAG's `schedule=None` — this callback is the only thing that
triggers it (besides a manual `airflow dags trigger`).

Two things worth understanding here, not just accepting on faith:

- **Port 8080 is open to the internet** (`0.0.0.0/0`), not restricted to
  your IP the way SSH is. Lambda (outside any VPC) has no stable,
  allowlist-able egress IP — AWS routes its outbound traffic through a
  shared, unpredictable pool, so there's no CIDR to restrict to without a
  NAT Gateway + Elastic IP (real ongoing cost/complexity) or a
  signed-request pattern via API Gateway. The actual protection is Basic
  Auth on every request (`AIRFLOW__API__AUTH_BACKENDS` in
  `airflow.env.j2`) — the port's open, but nothing is served without valid
  credentials. Fine for a portfolio/testing project; revisit if this ever
  needs to be genuinely production-grade.
- **The trigger call is non-fatal by design.** If Lambda can't reach
  Airflow, it logs a warning and moves on. If you want a periodic safety
  net in case a callback ever fails silently, add a sparse fallback
  `schedule` back to the DAG (e.g. `"0 * * * *"`) — safe to layer on top,
  since the wildcard sensor match + idempotent `COPY INTO` won't
  double-process anything.

### 4. Test the pipeline
```bash
cd scripts
pip install boto3
python generate_sample_events.py --count 1 --upload --bucket eventdriven-pipeline-raw
```
Watch CloudWatch Logs for the Lambda — look for `Triggered Airflow DAG
run: HTTP 200`. A new DAG run should appear in the Airflow UI within
seconds, not after a polling delay. Once it completes, check:
```sql
SELECT COUNT(*) FROM EVENTDRIVEN_PIPELINE_DB.RAW.RAW_EVENTS;
SELECT * FROM EVENTDRIVEN_PIPELINE_DB.ANALYTICS.ANALYTICS_EVENTS_DAILY;
```

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
```bash
cd scripts
python generate_sample_events.py --count 5
# or, to upload straight to S3 and kick off the real pipeline:
python generate_sample_events.py --count 5 --upload --bucket eventdriven-pipeline-raw
```
Each generated file is a JSON array of 1-20 records with nested
`metadata`/`address` objects, exercising the `pandas.json_normalize`
flattening rather than just flat records.

## Shared remote state backend
`terraform-bootstrap/` is a standalone module — apply it once, separately
from everything else:
```bash
cd terraform-bootstrap
terraform init
terraform apply -var="state_bucket_name=<something-globally-unique>"
```
This uses **S3 native locking** (`use_lockfile = true`, Terraform 1.10+ —
no DynamoDB table, no separate service to provision or pay for). Take the
`state_bucket_name` output and:
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

## Destroying / tearing down
S3 buckets have `force_destroy = true`, so `terraform destroy` empties and
removes them automatically — versioned objects and delete markers
included, which otherwise block deletion with a `BucketNotEmpty` error.
