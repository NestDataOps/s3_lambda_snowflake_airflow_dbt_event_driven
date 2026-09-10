variable "project_name" {
  type = string
}

variable "lambda_role_arn" {
  type = string
}

variable "processed_bucket" {
  type = string
}

variable "lambda_source_dir" {
  description = "Path to the ../lambda directory (contains src/)"
  type        = string
}

variable "pandas_layer_arn" {
  description = "ARN of AWS's managed AWSSDKPandas-Python312 layer for your region. Look up at https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html"
  type        = string
}

variable "airflow_base_url" {
  description = "e.g. http://<ec2-public-ip>:8080 -- used by Lambda to trigger the DAG directly after writing each Parquet file. Leave empty to disable (falls back to the DAG's own poll schedule)."
  type        = string
  default     = ""
}

variable "airflow_api_user" {
  description = "Airflow admin username Lambda authenticates as when calling the REST API"
  type        = string
  default     = ""
}

variable "airflow_api_password" {
  description = "Airflow admin password Lambda authenticates with. NOTE: Lambda env vars are encrypted at rest but readable in plaintext by anyone with lambda:GetFunctionConfiguration -- fine for a portfolio project, swap for AWS Secrets Manager if this needs to be genuinely production-grade."
  type        = string
  default     = ""
  sensitive   = true
}
