# Power BI: WM Performance report

Power BI is the business-facing layer. It reads **only** the `wm_semantic` Athena views, so
business users never touch bronze, silver or gold, and every number uses the certified
formulas.

## 1. Connect to Athena

| Option | When to use |
|---|---|
| **Amazon Athena connector** (Get Data → Amazon Athena), with the Simba Athena ODBC driver and a DSN pointing at workgroup `wm-<env>-analytics` | Standard setup, via the on-premises data gateway for scheduled refresh |
| Import mode (recommended) | Small, fast model. Refreshed by the Airflow `refresh_powerbi` task after the health checks pass |
| DirectQuery | Only for `AUM Daily` if you need intraday re-query. Keep the other tables in Import |

Authenticate the gateway with an IAM role or user that has Lake Formation `SELECT` on
`wm_semantic` only (the `bi_reader` role created by Terraform).

## 2. Model (star schema)

| Power BI table | Athena view | Grain |
|---|---|---|
| Date | `wm_semantic.v_dim_date` | day. Mark as date table; fiscal year ends 31 Oct |
| Advisor | `wm_semantic.v_dim_advisor` | advisor |
| Household | `wm_semantic.v_dim_household` | household |
| AUM Monthly | `wm_semantic.v_aum_flows_revenue_monthly` | month × advisor × segment × program × account type |
| AUM Daily | `wm_semantic.v_aum_daily` | day × advisor × segment × program × account type |
| Client Experience | `wm_semantic.v_client_experience_monthly` | household × month |
| Service Quality | `wm_semantic.v_service_quality_monthly` | month × region × category × channel |
| Scorecard | `wm_semantic.v_advisor_scorecard` | advisor × month |
| Asset Mix | `wm_semantic.v_asset_mix` | month-end × advisor × segment × asset class |
| Campaign Impact | `wm_semantic.v_campaign_impact` | campaign × metric |
| Campaign Trend | `wm_semantic.v_campaign_trend` | campaign × group × relative month |
| Platform Health | `wm_semantic.v_platform_health_latest` | check |
| Access Map (hidden) | `wm_semantic.advisor_access_map` | user × advisor |
| KPI Monthly (optional) | `wm_semantic.v_kpi_monthly` | month. Pre-computed certified KPIs for simple tiles |

**Relationships** (single direction, many-to-one):
`AUM Monthly[month]`, `Client Experience[month]`, `Scorecard[month]` and `Service Quality[month]` → `Date[date_key]` ·
`AUM Daily[as_of_date]` → `Date[date_key]` ·
`AUM Monthly[advisor_id]`, `AUM Daily[advisor_id]`, `Client Experience[advisor_id]`, `Scorecard[advisor_id]` and `Asset Mix[advisor_id]` → `Advisor[advisor_id]` ·
`Client Experience[household_id]` → `Household[household_id]`

Measures: [`measures.dax`](measures.dax) · Row-level security: [`rls_roles.dax`](rls_roles.dax)

## 3. Report pages

| Page | Visuals |
|---|---|
| **Executive overview** | Cards: Month-End AUM, Net New Assets, Organic Growth, ROA (bps), Total Revenue, Data As Of · Line: AUM Daily by client segment · Columns: NNA by month and segment · Waterfall: AUM bridge (Beginning AUM → NNA → Market Movement → Month-End AUM) · Bar: asset mix · Slicers: region, segment, fiscal year |
| **Advisor scorecard** | Scatter: quantity_score vs quality_score (colour = region, details = advisor) · Bar: net flows by advisor · Table: full scorecard with conditional formatting |
| **Client experience & service** | Lines: NPS with the NPS Reliability tooltip, Digital Adoption % · Columns: Complaints per 100 Households by region · Line: SLA Breach Rate by case category |
| **Initiative impact** | Lines: Campaign Trend (treatment vs control by relative month) · Table: Campaign Impact with Incremental per Household, Impact 95% CI and Impact Verdict (icons: lift = green, no effect = grey, guardrail breach = red) |
| **Platform health** | Cards: Failed Checks, Critical Failures · Table: latest checks |

## 4. Refresh and deployment

* **Scheduled refresh** is triggered by Airflow (`refresh_powerbi` task, Power BI REST API with a service principal) only after `data_health` passes. Don't also schedule refresh in the service.
* **Deployment pipeline:** Dev → Test → Prod workspaces. Parameterize the Athena DSN or workgroup per stage.
* **RLS testing:** use "View as role" with a test user mapped in `advisor_access_map` before each release.
