output "lambda_arn" {
  value = aws_lambda_function.clean_flatten.arn
}

output "lambda_name" {
  value = aws_lambda_function.clean_flatten.function_name
}
