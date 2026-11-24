# Runbook (AWS)

## Prerequisites

* An AWS account and region (default `ca-central-1`). You deploy with an admin or deployment role that is also a Lake Formation admin.
* Terraform ≥ 1.6, AWS CLI v2, Python 3.10+.
* An S3 bucket for Terraform state (see `infra/terraform/envs/*.backend.hcl`).
* Optional: two private subnets with NAT for MWAA (`create_mwaa = true`). Otherwise run the DAGs in an existing Airflow or in SageMaker Unified Studio workflows.

## First deployment (dev)

```bash
cd infra/terraform
# edit envs/dev.tfvars: alert_email, lf_admin_arns (your role ARN)
terraform init -backend-config=envs/dev.backend.hcl
terraform apply -var-file=envs/dev.tfvars
```

This creates the KMS key, 4 buckets, 5 Glue databases, 7 Glue jobs (scripts and `wm_common.zip` uploaded), the Athena workgroup, Lake Formation settings and tags, the SNS alerts topic plus the EventBridge rule, and the Power BI secret shell. Confirm the SNS email subscription.

**Bootstrap without Airflow** (quickest first run):

```bash
aws glue start-job-run --job-name wm-dev-generate-landing --arguments '{"--mode":"history","--scale":"small"}'
# when it succeeds, run in order:
for j in bronze-ingest silver-build gold-build; do aws glue start-job-run --job-name wm-dev-$j; done  # wait for each
aws glue start-job-run --job-name wm-dev-data-health --arguments '{"--fail_on_critical":"false"}'
aws glue start-job-run --job-name wm-dev-incremental-impact
aws glue start-job-run --job-name wm-dev-publish-semantic
```

**With Airflow:** set the Airflow Variables `wm_env`, `wm_sns_alerts_topic_arn`, `wm_powerbi_secret_id`, `wm_powerbi_workspace_id` and `wm_powerbi_dataset_id`. Trigger `wm_bootstrap` with `{"scale": "small"}`, then unpause `wm_daily_reporting`.

**After the bootstrap:** set `enable_table_level_grants = true` (and any `advisor_row_filters`) and apply again, which adds the PII column tags and row filters. Query in Athena (workgroup `wm-dev-analytics`): `SELECT * FROM wm_semantic.v_kpi_monthly ORDER BY month DESC`.

**Power BI:** follow `powerbi/README.md`. Store the service-principal JSON in the Secrets Manager secret `wm-dev/powerbi-refresh`.

## Daily operation

| Time (ET) | What happens | Check |
|---|---|---|
| 06:30 | `wm_daily_reporting` starts | Airflow UI: the run is queued |
| ~07:00 | Glue jobs: landing → bronze → silver → gold → semantic and health | All tasks green. `SELECT * FROM wm_semantic.v_platform_health_latest` shows no CRITICAL FAIL |
| ~07:15 | Power BI refresh triggered | Data As Of in the report = previous business day |
| Monday | `incremental_impact` runs | `wm_semantic.v_campaign_impact.run_at` = today |

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bronze_ingest` succeeds but writes 0 rows | No new files, or bookmark already advanced | Check `s3://…-raw-…/landing/<feed>/`. To reprocess: `aws glue reset-job-bookmark --job-name wm-dev-bronze-ingest` (silver MERGE dedupes) |
| `silver_build`: MERGE fails with a schema mismatch | New source column not yet in silver | Add it to the standardizer in `silver.py`. Bronze has already captured it |
| `data_health` FAIL `freshness:*` | Late file, or a market holiday | Rerun after the file lands (Airflow: Clear task). On holidays, acknowledge |
| `recon:holdings_vs_valuations` FAIL | Partial month-end holdings file | Ask the custodian to restate, then rerun from `bronze_ingest` |
| Power BI not refreshed | `data_health` failed (by design), or the SP secret or permission is wrong | Fix the data first. For auth, check the secret and the workspace member role |
| Athena `AccessDeniedException` on a gold table | Lake Formation grant missing | Grant through the LF-tag policy (analysts). BI must use `wm_semantic` only |
| Analyst can't see client names | Working as designed (`pii=true` columns) | Needs the PII reader grant, approved by Privacy |

## Standard procedures

* **Rerun a failed step:** in Airflow, Clear the failed task and its downstream tasks. Glue jobs are idempotent: silver uses MERGE and watermarks, and gold uses CREATE OR REPLACE.
* **Rebuild one gold table:** `aws glue start-job-run --job-name wm-prod-gold-build --arguments '{"--tables":"fct_account_daily,fct_account_monthly_performance"}'`
* **Roll back a gold table:** `CALL glue_catalog.system.rollback_to_snapshot('wm_gold.fct_aum_daily', <snapshot_id>)` (Spark), or query an earlier snapshot in Athena with `FOR TIMESTAMP AS OF`.
* **Restated custodian file:** deliver it as a new file name in the same feed prefix. The next run's MERGE keeps the latest `file_ts`.
* **Grant advisor access (Power BI):** `INSERT INTO wm_semantic.advisor_access_map VALUES ('ADV0042','jane.doe@firm.com','support',current_timestamp)` (Athena, Iceberg table).
* **Weekly maintenance (recommended):** Iceberg `rewrite_data_files` and `expire_snapshots` on the silver and gold facts.
