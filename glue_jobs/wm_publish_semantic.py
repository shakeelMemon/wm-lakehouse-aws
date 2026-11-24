"""Glue job: publish the semantic layer.

1. Writes the certified metric catalog (metric_catalog.yml) to wm_semantic.metric_catalog.
2. Creates the advisor_access_map entitlement table if missing (drives row-level security).
3. Creates / replaces the Athena views in wm_semantic (friendly names, certified formulas)
   that Power BI, SageMaker Unified Studio and analysts query.
4. Smoke-tests each view with a SELECT ... LIMIT 1 so a broken definition fails the run.

Job parameters: --athena_workgroup, --athena_output (s3://.../)
"""
import time
from datetime import datetime, timezone
from importlib import resources

import boto3
import yaml

from wm_common.glue_runtime import init, log_run, replace, t
from wm_common.sqlrunner import athena_statements

spark, glue, job, args, run_id = init(["athena_workgroup", "athena_output"])
athena = boto3.client("athena")


def run_athena(sql: str) -> str:
    qid = athena.start_query_execution(
        QueryString=sql, WorkGroup=args["athena_workgroup"],
        ResultConfiguration={"OutputLocation": args["athena_output"]})["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st["State"] != "SUCCEEDED":
                raise RuntimeError(f"Athena failed: {st.get('StateChangeReason')}\n{sql[:400]}")
            return qid
        time.sleep(1.5)


# 1. metric catalog ---------------------------------------------------------------------
catalog = yaml.safe_load(resources.files("wm_common").joinpath("sql", "metric_catalog.yml").read_text())["metrics"]
rows = [{**{k: str(v) for k, v in m.items()}, "published_at": datetime.now(timezone.utc)} for m in catalog]
replace(spark.createDataFrame(rows), t("semantic", "metric_catalog"))

# Entitlements used by Power BI RLS and Lake Formation data filters (maintained by support, SOP-07)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('semantic', 'advisor_access_map')}
              (advisor_id string, user_email string, granted_by string, granted_at timestamp) USING iceberg""")

# 2 + 3. Athena views ----------------------------------------------------------------------
published = []
for stmt in athena_statements():
    run_athena(stmt)
    view = stmt.split("VIEW", 1)[1].split()[0]
    run_athena(f"SELECT * FROM {view} LIMIT 1")
    published.append(view)
    print(f"published {view}")

log_run(spark, "wm_publish_semantic", run_id, "SUCCEEDED", {"views": published, "metrics": len(rows)})
job.commit()
