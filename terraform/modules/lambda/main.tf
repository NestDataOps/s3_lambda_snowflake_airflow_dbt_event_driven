data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${var.lambda_source_dir}/src"
  output_path = "${path.module}/build/lambda.zip"
}

# A hand-built pandas+pyarrow layer reliably exceeds Lambda's 250MB
# UNZIPPED hard limit (not just the 50MB direct-upload limit -- routing
# through S3 doesn't help here). AWS publishes a managed, pre-optimized
# layer with pandas/pyarrow/numpy that's built specifically to fit this
# ceiling -- use that instead of building your own. Find the correct ARN
# for your region/architecture at:
#   https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
# and pass it as var.pandas_layer_arn (see README).
resource "aws_lambda_function" "clean_flatten" {
  function_name    = "${var.project_name}-clean-flatten"
  role             = var.lambda_role_arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 512
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  layers           = [var.pandas_layer_arn]

  environment {
    variables = {
      PROCESSED_BUCKET     = var.processed_bucket
      AIRFLOW_BASE_URL     = var.airflow_base_url
      AIRFLOW_API_USER     = var.airflow_api_user
      AIRFLOW_API_PASSWORD = var.airflow_api_password
    }
  }
}
