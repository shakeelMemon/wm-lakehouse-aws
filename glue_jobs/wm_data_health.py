"""Glue job: data and pipeline health checks after each build.

Writes every check to wm_ops.dq_check_results. A CRITICAL failure raises, which fails the
Glue job, which fails the Airflow task: Power BI is not refreshed and on-call is alerted,
so business users keep yesterday's correct numbers instead of today's wrong ones.

Job parameters: --expected_date (YYYY-MM-DD or "auto"), --fail_on_critical (true|false)
"""
import pandas as pd

from wm_common.glue_runtime import append, init, log_run, t
from wm_common.health import critical_failures, run_checks

spark, glue, job, args, run_id = init(["expected_date", "fail_on_critical"])
exp = args["expected_date"]
if exp in ("", "auto"):
    exp = str((pd.Timestamp.now(tz="America/Toronto").normalize().tz_localize(None) - pd.offsets.BDay(1)).date())
expected = pd.Timestamp(exp).date()

results = run_checks(spark, t, expected, run_id)
df = spark.createDataFrame(pd.DataFrame(results))
append(spark, df, t("ops", "dq_check_results"))
df.orderBy("status", "severity").show(100, truncate=False)

crit = critical_failures(results)
log_run(spark, "wm_data_health", run_id, "FAILED" if crit else "SUCCEEDED",
        {"checks": len(results), "failed": sum(r["status"] == "FAIL" for r in results), "critical": len(crit)})
if crit and args["fail_on_critical"].lower() == "true":
    raise RuntimeError("Critical data-health checks failed: " + ", ".join(c["check_name"] for c in crit))
job.commit()
