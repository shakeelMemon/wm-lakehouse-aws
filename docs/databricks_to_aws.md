# Databricks → AWS mapping

The business logic is identical in both versions: the same generator, DQ rules, gold SQL,
metrics and impact estimator. Only the platform services change.

| Concern | Databricks version | AWS version | Why it's equivalent |
|---|---|---|---|
| Landing | UC Volume | S3 `raw` bucket, `landing/<feed>/period=…/` | KMS, versioning and lifecycle to Glacier IR after 90 days |
| Incremental file pickup | Auto Loader | **Glue job bookmarks** (`transformation_ctx` per feed + `job.commit()`) | Bookmarks advance only after every feed is written. A failed run re-reads, and silver MERGE makes that idempotent |
| Table format | Delta Lake | **Apache Iceberg** on the Glue Data Catalog | ACID, time travel, schema evolution, MERGE. Readable by Athena, EMR, Redshift Spectrum and Snowflake |
| Pipeline framework | Lakeflow Declarative Pipelines | Glue PySpark jobs orchestrated by Airflow | Declarative dependencies → explicit DAG. Logic kept in pure functions so it is unit-testable |
| Expectations | `@dlt.expect_all_or_drop` | `silver.split_valid()` using the same `quality.py` rules → valid rows, quarantine table with `_failed_rules`, pass counts in `wm_ops.dq_rule_results` | Same rules file, same outcome, plus a history table instead of the event log |
| SCD1 dedupe | AUTO CDC SCD1 | `MERGE INTO … WHEN MATCHED AND (s.file_ts > t.file_ts …) THEN UPDATE` + watermark | Latest delivery wins. Re-runs are idempotent |
| SCD2 | AUTO CDC SCD2 | Rebuild from the full bronze event history with window `lead()` (`__START_AT/__END_AT`) | Dimensions are small, so a full rebuild is simpler and self-healing. For large dims, switch to MERGE-based SCD2 |
| Gold | Materialized views | Iceberg `CREATE OR REPLACE TABLE … AS` (atomic snapshot swap) | Readers see the old snapshot until commit. Roll back with time travel |
| Semantic layer | UC metric views | **Athena views** (`wm_semantic`) + **Power BI DAX measures** + metric catalog table | Measures live in the BI model, inputs and certified KPIs in views, definitions in one YAML |
| Dashboards | AI/BI dashboard | **Power BI** (Athena connector, Import mode) | Refresh triggered by Airflow only after health checks pass |
| Natural-language Q&A / notebooks | Genie, Databricks notebooks | **SageMaker Unified Studio** (query editor, notebooks, managed MLflow) | Same Glue catalog. Lake Formation applies |
| Self-serve app | Databricks App | Power BI RLS + Unified Studio (optionally a Streamlit app on ECS/App Runner) | RLS by advisor book in both Power BI and Lake Formation |
| Orchestration | Lakeflow Jobs | **Airflow on MWAA** (`GlueJobOperator`, deferrable) | ShortCircuit for the Monday impact run. SNS callback on failure |
| Governance | Unity Catalog grants, row filters, masks | **Lake Formation**: LF-tags (layer, classification, pii), tag-based grants, data cells filters, PII columns tagged out | Least privilege by persona. BI reads `wm_semantic` only |
| Alerting | Job email notifications | EventBridge (Glue Job State Change) → SNS, plus the Airflow `on_failure_callback` | Alerts even if Airflow is down |
| IaC / CI | Asset Bundles | **Terraform** + GitHub Actions with OIDC | dev/test/prod tfvars, smoke run after deploy |

## Design questions and answers

**How do you make Glue ingestion idempotent?** Bookmarks give at-most-once pickup per successful run. If a run fails after writing some feeds, bookmarks aren't committed, so the next run re-reads those files. Bronze may then hold duplicates, but silver MERGEs on the business key with a latest-wins condition, so reporting never double counts.

**Why keep bronze as STRING?** An upstream type change (int → decimal, or a date format) can never fail ingestion. Silver owns typing, and bad values land in quarantine with the rule that failed.

**Why Iceberg over plain Parquet or Glue tables?** MERGE and upserts, atomic CTAS swaps for gold, time travel for restatements and audits, and schema evolution. Athena and Power BI read it natively.

**How do you control Glue cost?** Right-sized G.1X workers per job, a 30–90 minute timeout per job, partition pruning (`months(as_of_date)`), incremental silver via watermarks, gold full rebuilds only for small tables, auto-scaling, and an Athena per-query scan limit.

**What happens when a check fails?** `wm_data_health` raises, the Glue job fails, the Airflow task fails, `refresh_powerbi` never runs, and SNS emails support. Power BI keeps yesterday's certified numbers.

**How is PII protected?** KMS everywhere, and TLS-only bucket policies. `dim_client` name and email columns are tagged `pii=true`, and every grant requires `pii=false`. Power BI only sees masked initials through `v_dim_client_masked`.

**How would you scale this?** Switch the large gold facts (account daily, AUM daily) to incremental MERGE by date partition. Use Glue Flex for non-urgent jobs. Run Iceberg compaction (`rewrite_data_files`) weekly. Move Power BI's largest table to DirectQuery or aggregations.
