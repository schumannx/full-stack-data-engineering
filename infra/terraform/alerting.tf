# =============================================================================
# SNS alerting — task-failure callbacks (common/notify.py) + the daily
# dq_scorecard publish here. Subscription created only when alert_email is set;
# confirm the link AWS emails you after apply. Wire the topic ARN into Airflow as
# env STREAMING_ALERT_TOPIC_ARN (see output).
# =============================================================================

resource "aws_sns_topic" "alerts" {
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Let the Airflow execution role publish to the project's alert topic(s).
# Pattern-scoped so it doesn't depend on the (optional) topic resource existing.
resource "aws_iam_role_policy" "airflow_sns_publish" {
  name = "sns-publish"
  role = aws_iam_role.airflow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sns:Publish"]
      Resource = "arn:${local.partition}:sns:${var.aws_region}:${local.account_id}:${local.name}-*"
    }]
  })
}

output "alert_topic_arn" {
  description = "Set as STREAMING_ALERT_TOPIC_ARN in the Airflow env to enable alerts."
  value       = var.alert_email != "" ? aws_sns_topic.alerts[0].arn : null
}
