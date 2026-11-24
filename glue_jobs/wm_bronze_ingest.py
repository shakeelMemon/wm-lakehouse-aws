"""Glue job: S3 landing zone -> bronze Iceberg tables (one per feed).

* **Glue job bookmarks** make ingestion incremental: each run reads only files that
  arrived since the last successful run (``transformation_ctx`` per feed + ``job.commit()``).
* Bronze stores every value as delivered (STRING) plus lineage columns
  (_source_file, _ingested_at, _source_system, _run_id), appended to Iceberg with schema
  evolution enabled, so a new upstream column never breaks the load.
* Metadata-driven: the feed list is ``wm_common.config.FEEDS``.

Job parameters: --raw_bucket, --landing_prefix (default "landing")
Bookmarks: --job-bookmark-option job-bookmark-enable (set in Terraform)
"""
from pyspark.sql import functions as F

from wm_common.config import FEEDS
from wm_common.glue_runtime import append, init, log_run, t
from wm_common.silver import bronze_frame

spark, glue, job, args, run_id = init(["raw_bucket", "landing_prefix"])
root = f"s3://{args['raw_bucket']}/{args['landing_prefix']}"
summary = {}

for feed in FEEDS:
    fmt_opts = {"attachFilename": "_source_file"}
    if feed.fmt == "csv":
        fmt_opts.update({"withHeader": True, "separator": ",", "quoteChar": '"'})
    dyf = glue.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{root}/{feed.name}/"], "recurse": True},
        format=feed.fmt,
        format_options=fmt_opts,
        transformation_ctx=f"bronze_{feed.name}",      # bookmark state key
    )
    df = dyf.toDF()
    if not df.columns or df.limit(1).count() == 0:
        summary[feed.name] = 0
        continue
    bronze = bronze_frame(df, feed.source_system, run_id)
    append(spark, bronze, t("bronze", feed.name), partition_by=F.days("_ingested_at"))
    summary[feed.name] = bronze.count()
    print(f"bronze.{feed.name}: +{summary[feed.name]:,} rows")

log_run(spark, "wm_bronze_ingest", run_id, "SUCCEEDED", summary)
job.commit()   # advances bookmarks only after every feed was written
