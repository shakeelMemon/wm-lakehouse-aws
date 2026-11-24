output "raw_bucket" {
  value = aws_s3_bucket.this["raw"].id
}

output "lake_bucket" {
  value = aws_s3_bucket.this["lake"].id
}

output "artifacts_bucket" {
  value = aws_s3_bucket.this["artifacts"].id
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "glue_jobs" {
  value = { for k, j in aws_glue_job.this : k => j.name }
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "powerbi_secret_id" {
  value = aws_secretsmanager_secret.powerbi.id
}

output "powerbi_reader_role_arn" {
  value = try(aws_iam_role.bi_reader[0].arn, null)
}

output "mwaa_webserver_url" {
  value = try(aws_mwaa_environment.this[0].webserver_url, null)
}
