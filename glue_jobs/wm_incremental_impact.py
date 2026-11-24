"""Glue job: incremental impact of business initiatives (quantity + quality).

Reads the gold campaign panel (household x month, treatment vs control, pre vs post),
estimates household-level difference-in-differences with a 95% bootstrap CI for every
metric, classifies quantity lifts and quality guardrails, and writes
wm_gold.campaign_impact_results for Power BI and SageMaker Unified Studio.

Job parameters: --n_boot (default 1000)
"""
from datetime import datetime, timezone

import pandas as pd

from wm_common.glue_runtime import init, log_run, replace, t
from wm_common.impact import METRIC_REGISTRY, evaluate_campaign

spark, glue, job, args, run_id = init(["n_boot"])
n_boot = int(args["n_boot"])

panel_all = spark.table(t("gold", "fct_campaign_household_monthly")).toPandas()
campaigns = spark.table(t("silver", "campaigns")).toPandas()
frames = []
for c in campaigns.itertuples():
    p = panel_all[panel_all.campaign_id == c.campaign_id]
    if p.empty or p.period.nunique() < 2:
        print(f"skip {c.campaign_id}: no post-period data yet")
        continue
    res = evaluate_campaign(p, c.campaign_id, [m for m in METRIC_REGISTRY if m in p.columns], n_boot=n_boot)
    res["campaign_name"] = c.campaign_name
    res["primary_metric"] = c.primary_metric
    res["is_primary_metric"] = res.metric == c.primary_metric
    res["post_months_observed"] = int(p.loc[p.period == "post", "rel_month"].nunique())
    res["measurement_complete"] = pd.Timestamp(c.measurement_end_date) <= pd.Timestamp.today()
    frames.append(res)

if frames:
    results = pd.concat(frames, ignore_index=True)
    results["run_at"] = datetime.now(timezone.utc)
    results["run_id"] = run_id
    replace(spark.createDataFrame(results), t("gold", "campaign_impact_results"))
    for r in results[results.is_primary_metric].itertuples():
        print(f"{r.campaign_id} {r.metric}: {r.did_estimate:,.2f} per household per month "
              f"(95% CI {r.ci_low:,.2f} to {r.ci_high:,.2f}) -> {r.verdict}")
    log_run(spark, "wm_incremental_impact", run_id, "SUCCEEDED", {"rows": len(results)})
else:
    log_run(spark, "wm_incremental_impact", run_id, "SKIPPED", {"reason": "no campaign with post-period data"})
job.commit()
