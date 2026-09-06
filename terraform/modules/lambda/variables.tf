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
