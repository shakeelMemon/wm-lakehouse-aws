"""Glue job: bronze -> silver (validated, de-duplicated, history-tracked Iceberg tables).

For every entry in ``wm_common.silver.SILVER_SPECS``:

  upsert     read bronze rows newer than the watermark -> type -> DQ rules
             (bad rows -> <table>_quarantine with rule names) -> dedupe by key ->
             Iceberg MERGE (latest delivery wins) -> advance watermark
  scd2       rebuild SCD Type 2 history from all bronze change events
  current    rebuild latest state per key (accounts)
  reference  rebuild in full (branches, securities, campaigns, assignments)

DQ rule pass/fail counts go to wm_ops.dq_rule_results for the health job and dashboards.
Job parameters: --tables (comma list or "all")
"""
from datetime import datetime, timezone

from pyspark.sql import functions as F

from wm_common.glue_runtime import append, exists, get_watermark, init, log_run, replace, set_watermark, t
from wm_common.silver import SILVER_SPECS, build, merge_sql

spark, glue, job, args, run_id = init(["tables"])
wanted = None if args["tables"] in ("", "all") else set(args["tables"].split(","))
PARTITION = {"transactions": F.months("trade_date"), "valuations": F.months("as_of_date"),
             "holdings": F.months("as_of_date"), "digital_sessions": F.months("session_date")}
summary, rule_rows = {}, []

for spec in SILVER_SPECS:
    if wanted and spec.table not in wanted:
        continue
    bronze_name = t("bronze", spec.bronze)
    if not exists(spark, bronze_name):
        summary[spec.table] = "no bronze yet"
        continue
    bronze = spark.table(bronze_name)
    target = t("silver", spec.table)

    if spec.pattern == "upsert":
        wm_key = f"silver.{spec.table}"
        wm = get_watermark(spark, wm_key)
        if wm is not None:
            bronze = bronze.where(F.col("_ingested_at") > F.lit(wm))
        new_wm = bronze.agg(F.max("_ingested_at")).collect()[0][0]
        if new_wm is None:
            summary[spec.table] = 0
            continue
        batch, quarantine, results = build(spec, bronze)
        batch = batch.cache()
        n = batch.count()
        if not exists(spark, target):
            replace(batch, target, PARTITION.get(spec.table))
        else:
            batch.createOrReplaceTempView("_silver_batch")
            spark.sql(merge_sql(target, "_silver_batch", spec.keys,
                                tuple(c for c in spec.order_by if c in batch.columns)))
        if results:
            if quarantine.limit(1).count():
                append(spark, quarantine, t("silver", f"{spec.table}_quarantine"))
        set_watermark(spark, wm_key, new_wm, run_id)
        summary[spec.table] = n
    else:
        full, quarantine, results = build(spec, bronze)
        replace(full, target)
        summary[spec.table] = full.count()

    rule_rows += [(run_id, spec.table, rule, level, passed, failed, datetime.now(timezone.utc))
                  for rule, level, passed, failed in results]
    print(f"silver.{spec.table}: {summary[spec.table]}")

if rule_rows:
    append(spark, spark.createDataFrame(rule_rows, "run_id string, entity string, rule string, level string, "
                                                   "passed_rows long, failed_rows long, checked_at timestamp"),
           t("ops", "dq_rule_results"))
log_run(spark, "wm_silver_build", run_id, "SUCCEEDED", summary)
job.commit()
