# Package each handler dir to a zip at plan time (stdlib + boto3 only, so no deps
# to vendor). Mirrors lambda/build.sh, but keeps the artifact in Terraform state.
data "archive_file" "imdb_mirror" {
  type        = "zip"
  source_dir  = "${local.lambda_dir}/imdb_mirror"
  output_path = "${path.module}/.build/streaming_imdb_mirror.zip"
}

data "archive_file" "html_render" {
  type        = "zip"
  source_dir  = "${local.lambda_dir}/html_render"
  output_path = "${path.module}/.build/streaming_html_render.zip"
}

resource "aws_lambda_function" "imdb_mirror" {
  function_name    = local.lambda_imdb_mirror
  role             = aws_iam_role.lambda_imdb_mirror.arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.imdb_mirror.output_path
  source_code_hash = data.archive_file.imdb_mirror.output_base64sha256
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      STREAMING_S3_BUCKET = var.data_bucket
      IMDB_S3_PREFIX    = "imdb_base"
    }
  }
}

resource "aws_lambda_function" "html_render" {
  function_name    = local.lambda_html_render
  role             = aws_iam_role.lambda_html_render.arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.html_render.output_path
  source_code_hash = data.archive_file.html_render.output_base64sha256
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      STREAMING_S3_BUCKET    = var.data_bucket
      STREAMING_REPORTING_DB = local.glue_databases.reporting
      # The handler reads AWS_REGION, which Lambda sets automatically to the
      # function's region (us-east-1) — it's reserved, so we don't set it here.
      # Athena results MUST be in the same region as the workgroup (us-east-1).
      ATHENA_OUTPUT    = "s3://${aws_s3_bucket.ops.id}/athena-results/html_render/"
      ATHENA_WORKGROUP = aws_athena_workgroup.streaming.name
      REPORTS_PREFIX   = "reports"
    }
  }
}
