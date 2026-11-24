# SageMaker Unified Studio

Unified Studio is where analysts and data scientists work with the lakehouse interactively.
The production pipeline stays in Glue and Airflow.

## Project setup (one-off)

1. In the SageMaker Unified Studio domain, create a project `wm-analytics-<env>` from the SQL analytics + data science project profile.
2. **Data access:** grant the project role Lake Formation `SELECT` on `wm_gold` and `wm_semantic`, plus `DESCRIBE` on the databases (see `infra/terraform/lakeformation.tf`, variable `analyst_principal_arns`). The Glue Data Catalog then appears under **Data → Lakehouse**.
3. **Code:** connect the project to this Git repository so the notebooks can import `src/wm_common`.
4. **Query editor:** set the Athena workgroup to `wm-<env>-analytics` (results are encrypted with the project KMS key).

## What lives here

| Asset | Purpose |
|---|---|
| `impact_explorer.ipynb` | Re-run and stress-test the campaign difference-in-differences (balance check, trend plots, sensitivity to the post window, optional MLflow logging) |
| Query editor | Ad-hoc SQL on `wm_semantic` views (same certified numbers as Power BI) |
| Workflows (optional) | Unified Studio workflows run on managed Airflow, so the DAGs in `airflow/dags` can be deployed there instead of a stand-alone MWAA environment |

## Typical analyst questions

* "Is the consolidation campaign effect still significant with only 2 months of post data?" (notebook section 5)
* "Which advisors grow fast but have low meeting coverage?" (`SELECT * FROM wm_semantic.v_advisor_scorecard WHERE organic_growth_annualized > 0.10 AND meeting_coverage_pct < 0.30`)
* "Explain last month's AUM change for one region" (`wm_semantic.v_aum_flows_revenue_monthly`: begin + NNA + market = end)
