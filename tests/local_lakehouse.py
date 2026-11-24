"""Local, laptop-sized replica of the Glue pipeline for tests.

Runs the *same* generator, bronze/silver functions, DQ rules and gold SQL files as the Glue
jobs; only the I/O differs:

* S3 + Glue DynamicFrame bookmarks -> spark.read on the local landing folder
* Iceberg tables + MERGE           -> temp views (bronze__/silver__/gold__<table>)
* Iceberg CTAS                     -> the unmodified SELECT body of each gold statement
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from wm_common.config import FEEDS
from wm_common.generator import write_landing
from wm_common.silver import SILVER_SPECS, bronze_frame, build
from wm_common.sqlrunner import gold_statements, localize


def load_bronze(spark: SparkSession, landing: str, run_id: str = "local-test") -> dict[str, DataFrame]:
    out = {}
    for f in FEEDS:
        path = f"{landing}/{f.name}"
        if not Path(path).exists():
            continue
        r = spark.read.format(f.fmt)
        if f.fmt == "csv":
            r = r.option("header", "true")           # all strings, like the Glue CSV reader
        df = bronze_frame(r.load(path), f.source_system, run_id)
        df.cache().createOrReplaceTempView(f"bronze__{f.name}")
        out[f.name] = df
    return out


def build_silver(spark: SparkSession, bronze: dict[str, DataFrame]) -> dict[str, DataFrame]:
    silver = {}
    for spec in SILVER_SPECS:
        table, quarantine, _ = build(spec, bronze[spec.bronze])
        table.cache().createOrReplaceTempView(f"silver__{spec.table}")
        silver[spec.table] = table
        if spec.pattern == "upsert":
            quarantine.cache().createOrReplaceTempView(f"silver__{spec.table}_quarantine")
            silver[f"{spec.table}_quarantine"] = quarantine
    return silver


def build_gold(spark: SparkSession) -> list[str]:
    built = []
    for name, _, body in gold_statements():
        spark.sql(localize(body)).cache().createOrReplaceTempView(f"gold__{name}")
        built.append(name)
    return built


def build_all(spark: SparkSession, frames: dict, landing_dir: str) -> dict:
    write_landing(frames, landing_dir)
    bronze = load_bronze(spark, landing_dir)
    silver = build_silver(spark, bronze)
    gold = build_gold(spark)
    return {"bronze": bronze, "silver": silver, "gold": gold}


def resolver(layer: str, table: str) -> str:
    """Name resolver for wm_common.health.run_checks against the local temp views."""
    return f"{layer}__{table}"
