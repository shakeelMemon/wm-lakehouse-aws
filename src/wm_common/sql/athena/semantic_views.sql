-- =====================================================================================
-- SEMANTIC LAYER (Amazon Athena, engine v3 / Trino SQL)
-- Business-friendly views over the gold Iceberg tables. Power BI, SageMaker Unified
-- Studio and analysts read only from wm_semantic; certified formulas live here and in
-- the Power BI model (powerbi/measures.dax), both generated from metric_catalog.yml terms.
-- Published by the wm_publish_semantic Glue job. Row-level security: Lake Formation data
-- filters on the gold tables (Athena users) and Power BI RLS roles (report users).
-- =====================================================================================

-- AUM bridge + revenue at month x advisor x segment x program x account type.
-- AUM is semi-additive: sum month_end_aum_cad within one month only.
CREATE OR REPLACE VIEW wm_semantic.v_aum_flows_revenue_monthly AS
SELECT
  r.month_start                    AS month,
  d.fiscal_year,
  d.fiscal_period_label            AS fiscal_quarter,
  r.region,
  r.branch_name                    AS branch,
  r.advisor_id,
  a.advisor_name                   AS advisor,
  r.segment                        AS client_segment,
  r.program,
  r.account_type,
  r.revenue_model,
  CAST(r.begin_aum_cad AS double)          AS begin_aum_cad,
  CAST(r.end_aum_cad AS double)            AS month_end_aum_cad,
  CAST(r.gross_inflows_cad AS double)      AS gross_inflows_cad,
  CAST(r.gross_outflows_cad AS double)     AS gross_outflows_cad,
  CAST(r.net_flows_cad AS double)          AS net_new_assets_cad,
  CAST(r.market_movement_cad AS double)    AS market_movement_cad,
  CAST(r.fee_revenue_cad AS double)        AS fee_revenue_cad,
  CAST(r.commission_revenue_cad AS double) AS commission_revenue_cad,
  CAST(r.total_revenue_cad AS double)      AS total_revenue_cad,
  r.funded_accounts,
  r.new_accounts,
  r.closed_accounts
FROM wm_gold.fct_aum_rollforward_monthly r
LEFT JOIN wm_gold.dim_advisor a ON r.advisor_id = a.advisor_id
LEFT JOIN wm_gold.dim_date d    ON r.month_start = d.date_key;

-- Certified headline KPIs by month (ratio of sums, so they stay correct for any window).
CREATE OR REPLACE VIEW wm_semantic.v_kpi_monthly AS
SELECT
  month_start                                                            AS month,
  CAST(sum(end_aum_cad) AS double)                                       AS month_end_aum_cad,
  CAST(sum(net_flows_cad) AS double)                                     AS net_new_assets_cad,
  CAST(sum(market_movement_cad) AS double)                               AS market_movement_cad,
  CAST(sum(total_revenue_cad) AS double)                                 AS total_revenue_cad,
  CAST(sum(net_flows_cad) AS double) * 12
    / nullif(CAST(sum(begin_aum_cad) AS double), 0)                      AS organic_growth_annualized,
  CAST(sum(total_revenue_cad) AS double) * 12 * 10000
    / nullif(CAST(sum((begin_aum_cad + end_aum_cad) / 2) AS double), 0)  AS roa_bps,
  sum(new_accounts)                                                      AS new_accounts,
  sum(closed_accounts)                                                   AS closed_accounts
FROM wm_gold.fct_aum_rollforward_monthly
GROUP BY month_start;

-- Daily AUM trend.
CREATE OR REPLACE VIEW wm_semantic.v_aum_daily AS
SELECT as_of_date, advisor_id, region, branch_name AS branch, segment AS client_segment, program, account_type,
       CAST(aum_cad AS double) AS aum_cad, CAST(net_flows_cad AS double) AS net_flows_cad,
       funded_accounts, funded_households
FROM wm_gold.fct_aum_daily;

-- Household x month client-experience facts (quantity and quality signals).
CREATE OR REPLACE VIEW wm_semantic.v_client_experience_monthly AS
SELECT
  h.month_start AS month, h.household_id, h.advisor_id, a.advisor_name AS advisor, a.branch_name AS branch,
  a.region, h.segment AS client_segment, h.tenure_band, h.province,
  CAST(h.end_aum_cad AS double) AS month_end_aum_cad, CAST(h.net_flows_cad AS double) AS net_flows_cad,
  CAST(h.revenue_cad AS double) AS revenue_cad,
  h.interactions, h.meetings, h.financial_plans, h.digital_sessions, h.digital_active_flag,
  h.service_cases, h.complaints, h.sla_breaches, h.avg_resolution_hours,
  h.nps_responses, h.promoters, h.detractors, h.new_household_flag, h.attrition_flag
FROM wm_gold.fct_household_monthly h
LEFT JOIN wm_gold.dim_advisor a ON h.advisor_id = a.advisor_id;

-- Advisor balanced scorecard (quantity + quality, ranked within region).
CREATE OR REPLACE VIEW wm_semantic.v_advisor_scorecard AS
SELECT s.month_start AS month, s.advisor_id, a.advisor_name AS advisor, s.branch_name AS branch, s.region,
       s.households, CAST(s.end_aum_cad AS double) AS month_end_aum_cad, CAST(s.net_flows_cad AS double) AS net_flows_cad,
       CAST(s.revenue_cad AS double) AS revenue_cad, s.organic_growth_annualized, s.roa_bps,
       s.interactions_per_household, s.meeting_coverage_pct, s.digital_adoption_pct,
       s.complaints_per_100_households, s.sla_breach_rate, s.nps, s.nps_responses,
       s.quantity_score, s.quality_score
FROM wm_gold.fct_advisor_scorecard_monthly s
LEFT JOIN wm_gold.dim_advisor a ON s.advisor_id = a.advisor_id;

CREATE OR REPLACE VIEW wm_semantic.v_service_quality_monthly AS
SELECT month_start AS month, region, category AS case_category, channel, cases_opened, cases_closed, cases_open,
       avg_resolution_hours, median_resolution_hours, p90_resolution_hours, sla_breaches, sla_breach_rate
FROM wm_gold.fct_service_quality_monthly;

CREATE OR REPLACE VIEW wm_semantic.v_asset_mix AS
SELECT month_start AS month, as_of_date, advisor_id, segment AS client_segment, asset_class, asset_class_group,
       CAST(market_value_cad AS double) AS market_value_cad, accounts_holding
FROM wm_gold.fct_asset_mix_monthly;

CREATE OR REPLACE VIEW wm_semantic.v_campaign_trend AS
SELECT campaign_id, campaign_name, assignment_group, rel_month, month_start AS month, period, households,
       avg_net_flows_cad, avg_deposits, avg_interactions, avg_digital_sessions, digital_active_rate, avg_complaints
FROM wm_gold.fct_campaign_trend;

CREATE OR REPLACE VIEW wm_semantic.v_campaign_impact AS
SELECT campaign_id, campaign_name, metric, dimension, verdict, did_estimate, ci_low, ci_high, p_value,
       relative_lift, total_incremental, n_treatment, n_control, pre_trend_ok, is_primary_metric,
       post_months_observed, measurement_complete, run_at
FROM wm_gold.campaign_impact_results;

CREATE OR REPLACE VIEW wm_semantic.v_platform_health_latest AS
SELECT check_name, category, severity, status, observed_value, threshold, details, checked_at
FROM wm_ops.dq_check_results
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM wm_ops.dq_check_results);

-- Conformed dimensions for the Power BI star schema (client PII masked).
CREATE OR REPLACE VIEW wm_semantic.v_dim_date AS
SELECT date_key, calendar_year, calendar_quarter, calendar_month, month_start, month_end, month_label,
       is_business_day, is_last_business_day_of_month, fiscal_year, fiscal_quarter, fiscal_period_label
FROM wm_gold.dim_date;

CREATE OR REPLACE VIEW wm_semantic.v_dim_advisor AS
SELECT advisor_id, advisor_name AS advisor, designation, team_type, tenure_years, branch_id,
       branch_name AS branch, region, province, status
FROM wm_gold.dim_advisor;

CREATE OR REPLACE VIEW wm_semantic.v_dim_household AS
SELECT household_id, household_name, advisor_id, segment AS client_segment, province, status,
       onboard_date, closed_date, tenure_band
FROM wm_gold.dim_household;

CREATE OR REPLACE VIEW wm_semantic.v_dim_client_masked AS
SELECT client_id, household_id,
       substr(first_name, 1, 1) || '.' AS first_initial,
       substr(last_name, 1, 1) || '.'  AS last_initial,
       regexp_replace(email, '^[^@]+', '*****') AS email_masked,
       age_band, risk_profile, email_opt_in, status
FROM wm_gold.dim_client;
