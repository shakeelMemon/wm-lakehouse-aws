# Amazon MWAA (optional). Set create_mwaa = true with two private subnets.
# Alternatives: an existing MWAA / self-managed Airflow, or SageMaker Unified Studio workflows.

data "aws_iam_policy_document" "mwaa_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa" {
  count              = var.create_mwaa ? 1 : 0
  name               = "${local.name}-mwaa"
  assume_role_policy = data.aws_iam_policy_document.mwaa_trust.json
}

data "aws_iam_policy_document" "mwaa_access" {
  statement {
    sid       = "RunGlueJobs"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:GetJob", "glue:BatchStopJobRun"]
    resources = [for j in aws_glue_job.this : j.arn]
  }
  statement {
    sid       = "GlueLogsForVerboseOperator"
    actions   = ["logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogStreams"]
    resources = ["arn:${data.aws_partition.current.partition}:logs:${var.region}:${local.account_id}:log-group:/aws-glue/*"]
  }
  statement {
    sid       = "DagsBucket"
    actions   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
    resources = [aws_s3_bucket.this["artifacts"].arn, "${aws_s3_bucket.this["artifacts"].arn}/*"]
  }
  statement {
    sid       = "AlertsAndSecrets"
    actions   = ["sns:Publish", "secretsmanager:GetSecretValue"]
    resources = [aws_sns_topic.alerts.arn, aws_secretsmanager_secret.powerbi.arn]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey", "kms:Encrypt"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid       = "AirflowMetricsAndLogs"
    actions   = ["airflow:PublishMetrics", "cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:CreateLogGroup", "logs:PutLogEvents", "logs:GetLogRecord", "logs:GetLogGroupFields", "logs:GetQueryResults", "logs:DescribeLogGroups"]
    resources = ["*"]
  }
  statement {
    sid       = "CeleryQueues"
    actions   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
    resources = ["arn:${data.aws_partition.current.partition}:sqs:${var.region}:*:airflow-celery-*"]
  }
}

resource "aws_iam_role_policy" "mwaa" {
  count  = var.create_mwaa ? 1 : 0
  name   = "wm-orchestration"
  role   = aws_iam_role.mwaa[0].id
  policy = data.aws_iam_policy_document.mwaa_access.json
}

resource "aws_mwaa_environment" "this" {
  count              = var.create_mwaa ? 1 : 0
  name               = "${local.name}-airflow"
  airflow_version    = "2.10.3"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa[0].arn
  kms_key            = aws_kms_key.lake.arn

  source_bucket_arn    = aws_s3_bucket.this["artifacts"].arn
  dag_s3_path          = "mwaa/dags"
  requirements_s3_path = "mwaa/requirements.txt"

  webserver_access_mode = "PRIVATE_ONLY"
  max_workers           = 3

  airflow_configuration_options = {
    "core.default_timezone"         = "America/Toronto"
    "webserver.default_ui_timezone" = "America/Toronto"
    "core.load_examples"            = "false"
  }

  network_configuration {
    subnet_ids         = var.mwaa_subnet_ids
    security_group_ids = var.mwaa_security_group_ids
  }

  logging_configuration {
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  depends_on = [aws_s3_object.airflow]
}
