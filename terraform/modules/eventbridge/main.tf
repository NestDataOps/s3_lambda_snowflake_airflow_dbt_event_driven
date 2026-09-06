resource "aws_cloudwatch_event_rule" "raw_object_created" {
  name        = "${var.project_name}-raw-object-created"
  description = "Fires when a new file lands in the raw/ prefix of the raw bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.raw_bucket_name] }
      object = { key = [{ prefix = "raw/" }] }
    }
  })
}

# This has to live here, not in the lambda module: it needs the rule's
# *real* ARN (with actual account ID/region filled in by AWS), and
# aws_lambda_permission rejects wildcards ("*") in the account/region
# segments of source_arn -- only the trailing resource name may vary.
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.raw_object_created.arn
}

resource "aws_cloudwatch_event_target" "invoke_lambda" {
  rule      = aws_cloudwatch_event_rule.raw_object_created.name
  target_id = "${var.project_name}-clean-flatten"
  arn       = var.lambda_arn

  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }

  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

resource "aws_sqs_queue" "dlq" {
  name = "${var.project_name}-eventbridge-dlq"
}

resource "aws_sqs_queue_policy" "dlq_policy" {
  queue_url = aws_sqs_queue.dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.dlq.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.raw_object_created.arn }
      }
    }]
  })
}
