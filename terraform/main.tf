terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake" # renamed from Snowflake-Labs/snowflake
      version = "~> 2.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }

  # Points at the bucket/table created by terraform-bootstrap/. Run that
  # module once first, then fill in the real bucket name below (backend
  # blocks can't use variables, so it has to be hardcoded here) and run
  # `terraform init` to migrate state from local to S3.
  backend "s3" {
    bucket         = "portfolio-terraform-state-changeme" # match terraform-bootstrap output
    key            = "event-driven-pipeline/terraform.tfstate"
    region         = "ap-southeast-2"
    use_lockfile   = true
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}

provider "snowflake" {
  # Explicit rather than relying on the provider to discover SNOWFLAKE_*
  # env vars itself -- that discovery only works if those exact vars are
  # exported in the shell running `terraform apply`, which is easy to lose
  # across terminal sessions. These two still come from env, just via
  # Terraform's own (more reliable) TF_VAR_ mechanism -- see README.
  organization_name = var.snowflake_organization_name
  account_name       = var.snowflake_account_name
  role               = "SYSADMIN"

  # snowflake_file_format, snowflake_table, snowflake_storage_integration,
  # and snowflake_stage resources are still gated behind preview flags in
  # provider v2.x.
  preview_features_enabled = [
    "snowflake_file_format_resource",
    "snowflake_table_resource",
    "snowflake_storage_integration_resource",
    "snowflake_stage_resource",
  ]
}

# ---------------------------------------------------------------------------
# IAM: roles/policies shared across Lambda + EventBridge + Airflow
# ---------------------------------------------------------------------------
module "iam" {
  source       = "./modules/iam"
  project_name = var.project_name
  raw_bucket_arn       = module.s3.raw_bucket_arn
  processed_bucket_arn = module.s3.processed_bucket_arn
  snowflake_storage_aws_iam_user_arn = var.snowflake_storage_aws_iam_user_arn
  snowflake_external_id              = var.snowflake_external_id
}

# ---------------------------------------------------------------------------
# S3: raw landing zone + processed (Parquet) zone
# ---------------------------------------------------------------------------
module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
}

# ---------------------------------------------------------------------------
# Lambda: cleans/flattens JSON -> Parquet
# ---------------------------------------------------------------------------
module "lambda" {
  source            = "./modules/lambda"
  project_name      = var.project_name
  lambda_role_arn   = module.iam.lambda_role_arn
  processed_bucket  = module.s3.processed_bucket_name
  lambda_source_dir = "${path.module}/../lambda"
  pandas_layer_arn  = var.pandas_layer_arn
  # Derived directly from the actual EC2 instance's public IP -- always
  # current, no separate variable to keep in sync by hand. Must match the
  # ansible_admin_user/password set in ansible/group_vars/airflow.yml,
  # since that's what actually creates this login on the Airflow side.
  airflow_base_url     = "http://${module.ec2_airflow.public_ip}:8080"
  airflow_api_user     = var.airflow_api_user
  airflow_api_password = var.airflow_api_password
}

# ---------------------------------------------------------------------------
# EventBridge: S3 ObjectCreated (raw/) -> Lambda
# ---------------------------------------------------------------------------
module "eventbridge" {
  source          = "./modules/eventbridge"
  project_name    = var.project_name
  raw_bucket_name = module.s3.raw_bucket_name
  lambda_arn      = module.lambda.lambda_arn
  lambda_name     = module.lambda.lambda_name
}

# ---------------------------------------------------------------------------
# EC2: Airflow host (webserver + scheduler + triggerer via systemd, no Docker)
# ---------------------------------------------------------------------------
module "ec2_airflow" {
  source                = "./modules/ec2_airflow"
  project_name          = var.project_name
  ssh_public_key        = var.ssh_public_key
  allowed_cidr          = var.allowed_cidr
  raw_bucket_arn        = module.s3.raw_bucket_arn
  processed_bucket_arn  = module.s3.processed_bucket_arn
}

# ---------------------------------------------------------------------------
# Snowflake: warehouse/db/schema/stage/raw table for Airflow to COPY INTO
# ---------------------------------------------------------------------------
module "snowflake" {
  source                = "./modules/snowflake"
  project_name          = var.project_name
  processed_bucket_name = module.s3.processed_bucket_name
  storage_aws_role_arn  = module.iam.snowflake_storage_role_arn
  transformer_user      = var.snowflake_transformer_user
}
