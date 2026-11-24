# ------------------------------------------------------------------ encryption
resource "aws_kms_key" "lake" {
  description             = "${local.name} lakehouse data, logs and query results"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${local.name}-lake"
  target_key_id = aws_kms_key.lake.key_id
}

# ------------------------------------------------------------------ buckets
locals {
  buckets = {
    raw       = "${local.name}-raw-${local.account_id}"       # landing zone: files as delivered
    lake      = "${local.name}-lake-${local.account_id}"      # Iceberg warehouse (bronze/silver/gold)
    artifacts = "${local.name}-artifacts-${local.account_id}" # Glue scripts, wm_common.zip, Airflow DAGs
    athena    = "${local.name}-athena-${local.account_id}"    # Athena query results
  }
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.env != "prod"
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = { for k, v in aws_s3_bucket.this : k => v if k != "athena" }
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.this["raw"].id
  rule {
    id     = "archive-landing-files"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena" {
  bucket = aws_s3_bucket.this["athena"].id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

# Deny any non-TLS access to lakehouse buckets
data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.this
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value.arn, "${each.value.arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json
}

# ------------------------------------------------------------------ code artifacts
data "archive_file" "wm_common" {
  type        = "zip"
  source_dir  = "${local.repo_root}/src"
  output_path = "${path.module}/.build/wm_common.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc", "*.egg-info/**"]
}

resource "aws_s3_object" "wm_common" {
  bucket = aws_s3_bucket.this["artifacts"].id
  key    = "glue/lib/wm_common.zip"
  source = data.archive_file.wm_common.output_path
  etag   = data.archive_file.wm_common.output_md5
}

resource "aws_s3_object" "glue_scripts" {
  for_each = fileset("${local.repo_root}/glue_jobs", "*.py")
  bucket   = aws_s3_bucket.this["artifacts"].id
  key      = "glue/jobs/${each.value}"
  source   = "${local.repo_root}/glue_jobs/${each.value}"
  etag     = filemd5("${local.repo_root}/glue_jobs/${each.value}")
}

resource "aws_s3_object" "airflow" {
  for_each = setunion(fileset("${local.repo_root}/airflow", "dags/*.py"), fileset("${local.repo_root}/airflow", "requirements.txt"))
  bucket   = aws_s3_bucket.this["artifacts"].id
  key      = "mwaa/${each.value}"
  source   = "${local.repo_root}/airflow/${each.value}"
  etag     = filemd5("${local.repo_root}/airflow/${each.value}")
}

