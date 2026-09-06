terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

locals {
  # Snowflake unquoted identifiers can't contain hyphens -- upper() alone
  # doesn't strip them, so a hyphenated project_name (e.g. the default
  # "eventdriven-pipeline") silently produces a *different*, hyphenated
  # identifier than what any hand-typed SQL elsewhere (Airflow DAG, this
  # README, ad-hoc worksheet queries) assumes. Sanitize once, here, and
  # use this for every Snowflake object name in this module.
  sql_safe_name = upper(replace(var.project_name, "-", "_"))
}

resource "snowflake_warehouse" "wh" {
  name           = "${local.sql_safe_name}_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_database" "db" {
  name = "${local.sql_safe_name}_DB"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.db.name
  name     = "RAW"
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.db.name
  name     = "ANALYTICS"
}

# Storage integration: lets Snowflake assume the IAM role created in the
# iam module to read the processed bucket directly (no static AWS keys).
# NOTE: first `terraform apply` will fail to fully connect until you run
# `DESC STORAGE INTEGRATION` in Snowflake and feed the returned
# STORAGE_AWS_IAM_USER_ARN/EXTERNAL_ID back into the iam module variables,
# then `terraform apply` again. This chicken-and-egg step is normal for
# Snowflake storage integrations -- documented in the README.
resource "snowflake_storage_integration" "s3_integration" {
  name             = "${local.sql_safe_name}_S3_INTEGRATION"
  storage_provider = "S3"
  enabled          = true

  storage_aws_role_arn      = var.storage_aws_role_arn
  storage_allowed_locations = ["s3://${var.processed_bucket_name}/"]
}

resource "snowflake_file_format" "parquet" {
  name        = "PARQUET_FORMAT"
  database    = snowflake_database.db.name
  schema      = snowflake_schema.raw.name
  format_type = "PARQUET"
}

resource "snowflake_stage" "processed_stage" {
  name                = "PROCESSED_STAGE"
  database            = snowflake_database.db.name
  schema              = snowflake_schema.raw.name
  url                 = "s3://${var.processed_bucket_name}/"
  storage_integration = snowflake_storage_integration.s3_integration.name
  file_format         = "FORMAT_NAME = ${snowflake_database.db.name}.${snowflake_schema.raw.name}.${snowflake_file_format.parquet.name}"
}

# Raw landing table that Airflow's COPY INTO writes to. Kept schema-light
# (variant-ish columns) since dbt staging models do the real typing/cleanup.
resource "snowflake_table" "raw_events" {
  database = snowflake_database.db.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_EVENTS"

  column {
    name = "EVENT_ID"
    type = "STRING"
  }
  column {
    name = "EVENT_TYPE"
    type = "STRING"
  }
  column {
    name = "EVENT_PAYLOAD"
    type = "VARIANT"
  }
  column {
    name = "SOURCE_FILE_NAME"
    type = "STRING"
  }
  column {
    name = "INGESTED_AT"
    type = "TIMESTAMP_NTZ"
  }
}
