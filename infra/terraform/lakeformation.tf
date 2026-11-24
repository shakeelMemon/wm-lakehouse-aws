# =====================================================================================
# Lake Formation: persona access with LF-tags, row-level and column-level security
#   Glue jobs   -> ALL on every wm_* database (they build the layers)
#   Analysts    -> SELECT where layer in (gold, semantic, ops) AND pii = false (PII columns excluded)
#   Power BI    -> SELECT on layer = semantic only
#   Advisors    -> row filter on advisor_id (data cells filter) for Athena access
# =====================================================================================

resource "aws_lakeformation_data_lake_settings" "this" {
  allow_full_table_external_data_access = true # required by SageMaker Unified Studio compute
  admins = distinct(concat(var.lf_admin_arns, [aws_iam_role.glue.arn]))
}

resource "aws_lakeformation_resource" "lake" {
  arn      = aws_s3_bucket.this["lake"].arn
  role_arn = aws_iam_role.glue.arn
}

resource "aws_lakeformation_permissions" "glue_location" {
  principal   = aws_iam_role.glue.arn
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location {
    arn = aws_lakeformation_resource.lake.arn
  }
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lakeformation_permissions" "glue_databases" {
  for_each    = aws_glue_catalog_database.this
  principal   = aws_iam_role.glue.arn
  permissions = ["ALL"]
  database {
    name = each.value.name
  }
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

# ------------------------------------------------------------------ LF-tags
resource "aws_lakeformation_lf_tag" "layer" {
  key        = "layer"
  values     = ["bronze", "silver", "gold", "semantic", "ops"]
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lakeformation_lf_tag" "classification" {
  key        = "classification"
  values     = ["confidential", "internal"]
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lakeformation_lf_tag" "domain" {
  key        = "domain"
  values     = ["wealth_management"]
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

resource "aws_lakeformation_lf_tag" "pii" {
  key        = "pii"
  values     = ["true", "false"]
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

locals {
  db_tags = {
    wm_bronze   = { layer = "bronze", classification = "confidential" }
    wm_silver   = { layer = "silver", classification = "confidential" }
    wm_gold     = { layer = "gold", classification = "confidential" }
    wm_semantic = { layer = "semantic", classification = "internal" }
    wm_ops      = { layer = "ops", classification = "internal" }
  }
}

resource "aws_lakeformation_resource_lf_tags" "databases" {
  for_each = local.db_tags
  database {
    name = aws_glue_catalog_database.this[each.key].name
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.layer.key
    value = each.value.layer
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = each.value.classification
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.domain.key
    value = "wealth_management"
  }
  lf_tag { # every column is non-PII unless overridden at column level (dim_client below)
    key   = aws_lakeformation_lf_tag.pii.key
    value = "false"
  }
}

# ------------------------------------------------------------------ tag-based grants
resource "aws_lakeformation_permissions" "analysts" {
  for_each    = toset(var.analyst_principal_arns)
  principal   = each.value
  permissions = ["SELECT", "DESCRIBE"]
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = "layer"
      values = ["gold", "semantic", "ops"]
    }
    expression {
      key    = "pii"
      values = ["false"]
    }
  }
  depends_on = [aws_lakeformation_resource_lf_tags.databases]
}

resource "aws_lakeformation_permissions" "bi_reader" {
  count       = var.bi_gateway_principal_arn == "" ? 0 : 1
  principal   = aws_iam_role.bi_reader[0].arn
  permissions = ["SELECT", "DESCRIBE"]
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = "layer"
      values = ["semantic", "gold", "ops"] # Athena views need access to the tables they read
    }
    expression {
      key    = "pii"
      values = ["false"]
    }
  }
  depends_on = [aws_lakeformation_resource_lf_tags.databases]
}

# Power BI browses databases before tables: DESCRIBE on wm_semantic only, so its navigator shows
# just the semantic layer (the gold/ops table grants above are only for Athena view resolution).
resource "aws_lakeformation_permissions" "bi_reader_db" {
  count       = var.bi_gateway_principal_arn == "" ? 0 : 1
  principal   = aws_iam_role.bi_reader[0].arn
  permissions = ["DESCRIBE"]
  database {
    name = aws_glue_catalog_database.this["wm_semantic"].name
  }
}

# ------------------------------------------------------------------ PII column exclusion (after bootstrap)
# Client name / email columns are re-tagged pii=true, so the pii=false grants above exclude them.
resource "aws_lakeformation_resource_lf_tags" "client_pii" {
  count = var.enable_table_level_grants ? 1 : 0
  table_with_columns {
    database_name = "wm_gold"
    name          = "dim_client"
    column_names  = ["first_name", "last_name", "email", "birth_year"]
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.pii.key
    value = "true"
  }
}

# ------------------------------------------------------------------ row-level security (after bootstrap)
resource "aws_lakeformation_data_cells_filter" "advisor_book" {
  for_each = var.enable_table_level_grants ? var.advisor_row_filters : {}
  table_data {
    database_name    = "wm_gold"
    table_name       = "fct_household_monthly"
    table_catalog_id = local.account_id
    name             = "advisor-book-${substr(md5(each.key), 0, 8)}"
    row_filter {
      filter_expression = "advisor_id IN (${join(", ", [for a in each.value : "'${a}'"])})"
    }
    column_wildcard {
      excluded_column_names = []
    }
  }
}

resource "aws_lakeformation_permissions" "advisor_book" {
  for_each    = var.enable_table_level_grants ? var.advisor_row_filters : {}
  principal   = each.key
  permissions = ["SELECT"]
  data_cells_filter {
    database_name    = "wm_gold"
    table_name       = "fct_household_monthly"
    table_catalog_id = local.account_id
    name             = aws_lakeformation_data_cells_filter.advisor_book[each.key].table_data[0].name
  }
}

resource "aws_lakeformation_permissions" "admin_location" {
  for_each    = toset(var.lf_admin_arns)
  principal   = each.value
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location {
    arn = aws_lakeformation_resource.lake.arn
  }
  depends_on = [aws_lakeformation_data_lake_settings.this]
}

moved {
  from = aws_lakeformation_resource_lf_tags.pii_columns["wm_gold.dim_client"]
  to   = aws_lakeformation_resource_lf_tags.client_pii[0]
}
# Support maintains the Power BI RLS entitlements (runbook 9.6). Table is created by the
# publish-semantic job, so this grant waits for the bootstrap like the other table-level grants.
resource "aws_lakeformation_permissions" "support_access_map" {
  for_each    = var.enable_table_level_grants ? toset(var.lf_admin_arns) : toset([])
  principal   = each.value
  permissions = ["SELECT", "INSERT", "DELETE", "ALTER", "DESCRIBE"]
  table {
    database_name = aws_glue_catalog_database.this["wm_semantic"].name
    name          = "advisor_access_map"
  }
}
