# ------------------------------------------------------------------ Glue job role
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com", "lakeformation.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue-jobs"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid       = "LakehouseBuckets"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = flatten([for b in aws_s3_bucket.this : [b.arn, "${b.arn}/*"]])
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions", "glue:BatchCreatePartition",
      "glue:GetUserDefinedFunctions"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:database/wm_*",
      "arn:${data.aws_partition.current.partition}:glue:${var.region}:${local.account_id}:table/wm_*/*",
    ]
  }
  statement {
    sid       = "LakeFormationCredentialVending"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
  statement {
    sid = "AthenaForSemanticPublish"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"
    ]
    resources = [aws_athena_workgroup.analytics.arn]
  }
}

resource "aws_iam_role_policy" "glue_access" {
  name   = "lakehouse-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}

# ------------------------------------------------------------------ Power BI reader (optional)
data "aws_iam_policy_document" "bi_trust" {
  count = var.bi_gateway_principal_arn == "" ? 0 : 1
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.bi_gateway_principal_arn]
    }
  }
}

resource "aws_iam_role" "bi_reader" {
  count              = var.bi_gateway_principal_arn == "" ? 0 : 1
  name               = "${local.name}-powerbi-reader"
  assume_role_policy = data.aws_iam_policy_document.bi_trust[0].json
}

data "aws_iam_policy_document" "bi_access" {
  statement {
    actions   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup", "athena:ListDatabases", "athena:ListTableMetadata", "athena:GetTableMetadata",
    "athena:GetQueryResultsStream", "athena:StopQueryExecution", "athena:ListDataCatalogs", "athena:GetDataCatalog", "athena:ListWorkGroups"]
    resources = ["*"]
  }
  statement {
    actions   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = ["*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.this["athena"].arn, "${aws_s3_bucket.this["athena"].arn}/*"]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "bi_access" {
  count  = var.bi_gateway_principal_arn == "" ? 0 : 1
  name   = "athena-semantic-read"
  role   = aws_iam_role.bi_reader[0].id
  policy = data.aws_iam_policy_document.bi_access.json
}

