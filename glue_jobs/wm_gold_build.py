"""Glue job: silver -> gold standardized reporting datasets (Iceberg CTAS).

Runs every statement in wm_common/sql/gold/*.sql in dependency order:
    CREATE OR REPLACE TABLE glue_catalog.wm_gold.<table> USING iceberg ... AS SELECT ...
CREATE OR REPLACE is atomic in Iceberg: readers (Athena, Power BI) see the previous
snapshot until the new one commits, and time travel keeps prior versions for rollback.

Job parameters: --tables (comma list or "all")
"""
import time

from wm_common.glue_runtime import init, log_run
from wm_common.sqlrunner import gold_statements, render

spark, glue, job, args, run_id = init(["tables"])
wanted = None if args["tables"] in ("", "all") else set(args["tables"].split(","))
summary = {}

for name, ctas, _ in gold_statements():
    if wanted and name not in wanted:
        continue
    start = time.time()
    spark.sql(render(ctas))
    rows = spark.table(f"glue_catalog.wm_gold.{name}").count()
    summary[name] = {"rows": rows, "seconds": round(time.time() - start, 1)}
    print(f"gold.{name}: {rows:,} rows in {summary[name]['seconds']}s")

log_run(spark, "wm_gold_build", run_id, "SUCCEEDED", summary)
job.commit()
