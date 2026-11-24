# ------------------------------------------------------------------ alerting
resource "aws_sns_topic" "alerts" {
  name              = "${local.name}-pipeline-alerts"
  kms_master_key_id = aws_kms_key.lake.id
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Any wm-<env>-* Glue job that fails, times out or stops -> SNS (independent of Airflow)
resource "aws_cloudwatch_event_rule" "glue_failures" {
  name        = "${local.name}-glue-job-failures"
  description = "WM Glue job failed / timed out"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [{ prefix = "${local.name}-" }]
      state   = ["FAILED", "TIMEOUT", "STOPPED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failures" {
  rule      = aws_cloudwatch_event_rule.glue_failures.name
  target_id = "sns"
  arn       = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "sns_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_events.json
}

# ------------------------------------------------------------------ Power BI service principal secret
resource "aws_secretsmanager_secret" "powerbi" {
  name        = "${local.name}/powerbi-refresh"
  description = "Power BI service principal for dataset refresh: {tenant_id, client_id, client_secret}. Value set out of band."
  kms_key_id  = aws_kms_key.lake.arn
}
