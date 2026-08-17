# Athena workgroup for ad-hoc queries, validate_etl reconciliation, and the
# html_render Lambda. Results land in the us-east-1 ops bucket (same region as
# the workgroup — required).
resource "aws_athena_workgroup" "streaming" {
  name = "${local.name}-wg"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.ops.id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
