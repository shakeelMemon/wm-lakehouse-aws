variable "env" {
  description = "Environment name: dev, test or prod"
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be dev, test or prod."
  }
}

variable "region" {
  description = "AWS region (Canadian data residency by default)"
  type        = string
  default     = "ca-central-1"
}

variable "owner" {
  description = "Owning team, used in tags"
  type        = string
  default     = "wm-data-engineering"
}

variable "data_scale" {
  description = "Synthetic data scale for the generator job: tiny | small | medium | large"
  type        = string
  default     = "small"
}

variable "alert_email" {
  description = "Email subscribed to the failure-alert SNS topic (confirm the subscription once)"
  type        = string
}

variable "glue_version" {
  description = "AWS Glue version (5.0 = Spark 3.5, Python 3.11, native Iceberg)"
  type        = string
  default     = "5.0"
}

variable "lf_admin_arns" {
  description = "IAM principals that administer Lake Formation (data platform team, deployment role)"
  type        = list(string)
}

variable "analyst_principal_arns" {
  description = "IAM roles for analysts / SageMaker Unified Studio projects (read gold, semantic, ops)"
  type        = list(string)
  default     = []
}

variable "bi_gateway_principal_arn" {
  description = "Principal allowed to assume the Power BI reader role (gateway host role or user). Empty = no role."
  type        = string
  default     = ""
}

variable "enable_table_level_grants" {
  description = "Create table-level Lake Formation grants and row/column filters. Enable after the first bootstrap run has created the gold tables."
  type        = bool
  default     = false
}

variable "advisor_row_filters" {
  description = "Row-level security for Athena users: principal ARN -> list of advisor_ids that principal may see"
  type        = map(list(string))
  default     = {}
}

variable "create_mwaa" {
  description = "Create an Amazon MWAA environment (needs private subnets with NAT). Set false to use an existing Airflow or SageMaker Unified Studio workflows."
  type        = bool
  default     = false
}

variable "mwaa_subnet_ids" {
  description = "Two private subnet IDs for MWAA"
  type        = list(string)
  default     = []
}

variable "mwaa_security_group_ids" {
  description = "Security groups for MWAA"
  type        = list(string)
  default     = []
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the analytics workgroup (cost guardrail)"
  type        = number
  default     = 10737418240 # 10 GiB
}
