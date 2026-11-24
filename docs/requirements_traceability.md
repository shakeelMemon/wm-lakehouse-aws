# What the project covers

| # | Capability | What this project delivers (AWS) | Where |
|---|---|---|---|
| 1 | **Standardized reporting datasets, semantic layers, dashboards and performance reporting** | 18 gold Iceberg datasets at documented grains. 14 Athena semantic views. A certified metric catalog (17 metrics). A Power BI star schema with certified DAX measures and 3 report pages | `src/wm_common/sql/gold/`, `src/wm_common/sql/athena/`, `src/wm_common/sql/metric_catalog.yml`, `powerbi/` |
| 2 | **Pipelines and reporting processes: reliability, performance, automation, ease of consumption** | Glue 5.0 PySpark jobs with bookmarks, DQ quarantine, Iceberg MERGE and SCD2, and atomic gold CTAS. Airflow DAGs on MWAA with a deferrable GlueJobOperator. Terraform with dev/test/prod. CI that runs the real logic locally | `glue_jobs/`, `src/wm_common/silver.py`, `airflow/dags/`, `infra/terraform/`, `.github/workflows/` |
| 3 | **Performance measurement: business outcomes, operational health, incremental impact across quantity and quality** | AUM bridge, TWR, NNA, organic growth, ROA. An advisor scorecard with separate quantity and quality scores. Health checks that gate the BI refresh. Difference-in-differences with guardrails | `gold_performance.sql`, `gold_advisor_client.sql`, `src/wm_common/health.py`, `src/wm_common/impact.py` |
| 4 | **Solution design with WM stakeholders, data teams and technology partners** | Architecture decisions, the Databricks→AWS mapping, a Lake Formation persona model, and a runbook | `docs/` |
| 5 | **Present complex data clearly** | Plain-language metric definitions, Power BI page designs, impact verdicts, and Unified Studio notebook storytelling | `docs/metric_definitions.md`, `powerbi/README.md`, `sagemaker/impact_explorer.ipynb` |
| 6 | **New analyses, data products, self-serve tools** | Power BI with RLS by advisor book. SageMaker Unified Studio project (query editor, notebooks, MLflow). The campaign impact data product | `powerbi/`, `sagemaker/` |

**Tools covered:** AWS Glue · Airflow · SageMaker Unified Studio · Power BI · wealth management domain (registered plans, AUM, NNA, ROA, TWR, NPS, fiscal year Nov–Oct)
