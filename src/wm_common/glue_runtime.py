"""Glue job plumbing shared by every job: context, Iceberg table helpers, watermarks, run log.

Iceberg + Glue Data Catalog are enabled by job parameters set in Terraform:
    --datalake-formats iceberg
    --conf spark.sql.extensions=...IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=...
so jobs address tables as glue_catalog.<database>.<table>.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import ICEBERG_CATALOG, SCHEMAS

TABLE_PROPS = {"format-version": "2", "write.parquet.compression-codec": "zstd",
               "write.metadata.delete-after-commit.enabled": "true",
               "write.metadata.previous-versions-max": "50"}


def init(arg_names: list[str]):
    """Return (spark, glue_context, job, args, run_id). Imports awsglue lazily (Glue only)."""
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", *arg_names])
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    job = Job(glue)
    job.init(args["JOB_NAME"], args)
    run_id = args.get("JOB_RUN_ID") or str(uuid.uuid4())
    return glue.spark_session, glue, job, args, run_id


def t(layer: str, table: str) -> str:
    return f"{ICEBERG_CATALOG}.{SCHEMAS[layer]}.{table}"


def exists(spark: SparkSession, name: str) -> bool:
    try:
        spark.table(name).limit(0).collect()
        return True
    except Exception:  # AnalysisException: table or view not found
        return False


def replace(df: DataFrame, name: str, partition_by=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    for k, v in TABLE_PROPS.items():
        w = w.tableProperty(k, v)
    if partition_by is not None:
        w = w.partitionedBy(partition_by)
    w.createOrReplace()


def append(spark: SparkSession, df: DataFrame, name: str, partition_by=None) -> None:
    """Append, creating the table on first write; new source columns evolve the schema."""
    if exists(spark, name):
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
        df.writeTo(name).option("mergeSchema", "true").append()
    else:
        w = df.writeTo(name).using("iceberg")
        for k, v in {**TABLE_PROPS, "write.spark.accept-any-schema": "true"}.items():
            w = w.tableProperty(k, v)
        if partition_by is not None:
            w = w.partitionedBy(partition_by)
        w.create()


WATERMARKS = t("ops", "etl_watermarks")


def get_watermark(spark: SparkSession, key: str):
    if not exists(spark, WATERMARKS):
        return None
    row = spark.table(WATERMARKS).where(F.col("wm_key") == key).select("watermark").collect()
    return row[0][0] if row else None


def set_watermark(spark: SparkSession, key: str, value, run_id: str) -> None:
    if value is None:
        return
    df = spark.createDataFrame([(key, value, run_id, datetime.now(timezone.utc))],
                               "wm_key string, watermark timestamp, run_id string, updated_at timestamp")
    if not exists(spark, WATERMARKS):
        replace(df, WATERMARKS)
        return
    df.createOrReplaceTempView("_wm_new")
    spark.sql(f"""MERGE INTO {WATERMARKS} t USING _wm_new s ON t.wm_key = s.wm_key
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def log_run(spark: SparkSession, job: str, run_id: str, status: str, details: dict) -> None:
    rows = [(job, run_id, status, str(details), datetime.now(timezone.utc))]
    df = spark.createDataFrame(rows, "job string, run_id string, status string, details string, logged_at timestamp")
    append(spark, df, t("ops", "job_runs"))
