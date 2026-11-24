# ------------------------------------------------------------------ Glue Data Catalog databases
locals {
  databases = {
    wm_bronze   = "Bronze: raw feeds as delivered (STRING) + lineage, Iceberg, append-only"
    wm_silver   = "Silver: validated, de-duplicated, SCD2 history, quarantine tables (Iceberg)"
    wm_gold     = "Gold: standardized WM reporting datasets - conformed dims and facts (Iceberg)"
    wm_semantic = "Semantic: Athena views for Power BI and analysts, metric catalog, entitlements"
    wm_ops      = "Ops: data-quality results, watermarks, job run log"
  }
}

resource "aws_glue_catalog_database" "this" {
  for_each     = local.databases
  name         = each.key
  description  = each.value
  location_uri = "s3://${aws_s3_bucket.this["lake"].id}/warehouse/${each.key}/"
  depends_on   = [aws_lakeformation_permissions.admin_location, aws_lakeformation_permissions.glue_location]
}

# ------------------------------------------------------------------ encryption for Glue logs / bookmarks
resource "aws_glue_security_configuration" "this" {
  name = "${local.name}-glue-kms"
  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED"
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.lake.arn
    }
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.lake.arn
    }
  }
}

# ------------------------------------------------------------------ jobs
locals {
  lake_bucket = aws_s3_bucket.this["lake"].id
  artifacts   = aws_s3_bucket.this["artifacts"].id

  # Iceberg + Glue Data Catalog as the Spark catalog "glue_catalog"
  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.warehouse=s3://${local.lake_bucket}/warehouse/",
    "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.iceberg.handle-timestamp-without-timezone=true",
    "spark.sql.session.timeZone=UTC",
  ])

  glue_common_args = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--extra-py-files"                   = "s3://${local.artifacts}/${aws_s3_object.wm_common.key}"
    "--additional-python-modules"        = "PyYAML==6.0.2"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--enable-auto-scaling"              = "true"
    "--TempDir"                          = "s3://${local.artifacts}/glue/tmp/"
  }

  glue_jobs = {
    generate-landing = {
      script      = "wm_generate_landing.py", workers = 2, timeout = 60, bookmark = false,
      description = "Simulate upstream systems and land files in S3 (history or daily increment)",
      args = { "--raw_bucket" = aws_s3_bucket.this["raw"].id, "--mode" = "incremental",
      "--scale" = var.data_scale, "--as_of_date" = "auto" }
    }
    bronze-ingest = {
      script      = "wm_bronze_ingest.py", workers = 4, timeout = 60, bookmark = true,
      description = "Landing files -> bronze Iceberg (job bookmarks, lineage, schema evolution)",
      args        = { "--raw_bucket" = aws_s3_bucket.this["raw"].id, "--landing_prefix" = "landing" }
    }
    silver-build = {
      script      = "wm_silver_build.py", workers = 4, timeout = 90, bookmark = false,
      description = "Bronze -> silver: DQ rules, quarantine, MERGE upserts, SCD2",
      args        = { "--tables" = "all" }
    }
    gold-build = {
      script      = "wm_gold_build.py", workers = 4, timeout = 90, bookmark = false,
      description = "Silver -> gold reporting datasets (Iceberg CTAS from SQL files)",
      args        = { "--tables" = "all" }
    }
    data-health = {
      script      = "wm_data_health.py", workers = 2, timeout = 30, bookmark = false,
      description = "Freshness, volume, reconciliation, integrity and expectation checks",
      args        = { "--expected_date" = "auto", "--fail_on_critical" = "true" }
    }
    publish-semantic = {
      script      = "wm_publish_semantic.py", workers = 2, timeout = 30, bookmark = false,
      description = "Metric catalog + Athena semantic views for Power BI",
      args = { "--athena_workgroup" = aws_athena_workgroup.analytics.name,
      "--athena_output" = "s3://${aws_s3_bucket.this["athena"].id}/glue-publish/" }
    }
    incremental-impact = {
      script      = "wm_incremental_impact.py", workers = 2, timeout = 30, bookmark = false,
      description = "Campaign difference-in-differences with bootstrap CIs",
      args        = { "--n_boot" = "1000" }
    }
  }
}

resource "aws_glue_job" "this" {
  for_each               = local.glue_jobs
  name                   = "${local.name}-${each.key}"
  description            = each.value.description
  role_arn               = aws_iam_role.glue.arn
  glue_version           = var.glue_version
  worker_type            = "G.1X"
  number_of_workers      = each.value.workers
  timeout                = each.value.timeout
  max_retries            = 0 # retries are owned by Airflow
  security_configuration = aws_glue_security_configuration.this.name

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.artifacts}/${aws_s3_object.glue_scripts[each.value.script].key}"
  }

  default_arguments = merge(local.glue_common_args, each.value.args, {
    "--job-bookmark-option" = each.value.bookmark ? "job-bookmark-enable" : "job-bookmark-disable"
  })

  execution_property {
    max_concurrent_runs = 1
  }
}


