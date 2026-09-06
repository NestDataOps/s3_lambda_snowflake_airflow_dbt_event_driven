/*
  Shared remote state backend for your Terraform projects.

  Run this ONCE, standalone (it has no backend block itself — bootstrapping
  the backend can't depend on the backend). After it's applied, both this
  event-driven-pipeline repo AND your actions/terraform/ansible/airflow/dbt
  repo can point their `backend "s3" {}` blocks at the same bucket, using
  different `key` values so their state files don't collide:

    # event-driven-pipeline/terraform/main.tf
    backend "s3" {
      bucket         = "<output: state_bucket_name>"
      key            = "event-driven-pipeline/terraform.tfstate"
      region         = "ap-southeast-2"
      dynamodb_table = "<output: lock_table_name>"
      encrypt        = true
    }

    # your other repo's backend block
    backend "s3" {
      bucket         = "<output: state_bucket_name>"
      key            = "ci-cd-infra/terraform.tfstate"     # different key
      region         = "ap-southeast-2"
      dynamodb_table = "<output: lock_table_name>"
      encrypt        = true
    }

  This is also a nice thing to point out on a resume/portfolio: two
  separate projects sharing one governed state backend, the way a real
  platform team would manage multiple product teams' Terraform.
*/

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "ap-southeast-2"
}

variable "state_bucket_name" {
  description = "Must be globally unique across all of AWS"
  type        = string
  default     = "portfolio-terraform-state-changeme"
}

resource "aws_s3_bucket" "tf_state" {
  bucket = var.state_bucket_name

  # Prevents `terraform destroy` from ever nuking your state history by accident
  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tf_lock" {
  name         = "terraform-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

output "state_bucket_name" {
  value = aws_s3_bucket.tf_state.bucket
}

output "lock_table_name" {
  value = aws_dynamodb_table.tf_lock.name
}
