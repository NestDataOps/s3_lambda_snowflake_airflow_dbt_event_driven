variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Short name used as a prefix for all resources"
  type        = string
  default     = "eventdriven-pipeline"
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name. Set via TF_VAR_snowflake_organization_name env var, not a default here (org-specific). Find with: SELECT CURRENT_ORGANIZATION_NAME();"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name. Set via TF_VAR_snowflake_account_name env var, not a default here (account-specific). Find with: SELECT CURRENT_ACCOUNT_NAME();"
  type        = string
}

variable "snowflake_storage_aws_iam_user_arn" {
  description = "Real STORAGE_AWS_IAM_USER_ARN from `DESC STORAGE INTEGRATION`. Leave empty on first apply."
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "Real STORAGE_AWS_EXTERNAL_ID from `DESC STORAGE INTEGRATION`. Leave as placeholder on first apply."
  type        = string
  default     = "placeholder_external_id"
}

variable "pandas_layer_arn" {
  description = "ARN of AWS's managed AWSSDKPandas-Python312 layer for your AWS region. Full table (per region/arch): https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html"
  type        = string
  default     = "arn:aws:lambda:ap-southeast-2:336392948345:layer:AWSSDKPandas-Python312:31"
}

variable "ssh_public_key" {
  description = "Contents of your SSH public key file (e.g. ~/.ssh/id_ed25519.pub), for EC2 access -- not the private key"
  type        = string
}

variable "allowed_cidr" {
  description = "Your public IP in CIDR form (e.g. 1.2.3.4/32). Find yours with: curl -s ifconfig.me"
  type        = string
}
