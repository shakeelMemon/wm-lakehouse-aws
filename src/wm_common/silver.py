"""Bronze -> silver logic as pure PySpark functions.

The Glue jobs do the I/O (S3, Glue Data Catalog, Iceberg MERGE); everything that decides
*what* the data should look like lives here so it runs identically in Glue and in local
tests.

Silver load patterns (table-driven, see ``SILVER_SPECS``):

* ``upsert``   – incremental facts. New bronze rows since the last watermark are typed,
                 validated (bad rows -> quarantine), de-duplicated by business key within
                 the batch, then MERGEd into the Iceberg table: re-sent files and
                 custodian restatements resolve to the latest delivery (SCD Type 1).
* ``scd2``     – master data (advisors, households, clients). Rebuilt from the full event
                 history in bronze with __START_AT / __END_AT validity columns (SCD Type 2).
* ``current``  – latest state per key rebuilt from bronze events (accounts).
* ``reference``– small reference sets rebuilt in full (branches, securities, campaigns).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .quality import drop_rules, warn_rules
from .transforms import (
    failed_rules_col,
    latest_by_key,
    scd2_from_events,
    standardize_clients,
    standardize_holdings,
    standardize_service_cases,
    standardize_transactions,
    standardize_valuations,
)

LINEAGE = ["_source_file", "_ingested_at", "_source_system", "_run_id"]


# ----------------------------------------------------------------------------- bronze
def bronze_frame(df: DataFrame, source_system: str, run_id: str) -> DataFrame:
    """Raw file rows as strings + lineage columns.

    Bronze keeps every value exactly as delivered (all STRING), so a type change upstream
    can never break ingestion; silver owns typing.
    """
    raw_cols = [c for c in df.columns if c not in LINEAGE and c != "period"]
    src = F.col("_source_file") if "_source_file" in df.columns else F.input_file_name()  # Glue attachFilename
    return df.select(
        *[F.col(f"`{c}`").cast("string").alias(c) for c in raw_cols],
        src.cast("string").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(source_system).alias("_source_system"),
        F.lit(run_id).alias("_run_id"),
    )


# ----------------------------------------------------------------------------- standardizers
def _lin(df: DataFrame) -> list:
    """Carry the bronze ingestion time through (ordering for MERGE, freshness)."""
    return [F.col("_ingested_at").cast("timestamp").alias("_ingested_at")] if "_ingested_at" in df.columns else []


def std_prices(df):
    return df.select("security_id", F.col("price_date").cast("date"), F.col("close_price").cast("double"), "currency", *_lin(df))


def std_fx(df):
    return df.select(F.col("rate_date").cast("date"), "currency", F.col("rate_to_cad").cast("double"), *_lin(df))


def std_crm(df):
    return df.select("interaction_id", "household_id", "advisor_id", F.col("interaction_date").cast("date"),
                     "interaction_type", F.col("duration_min").cast("int"), "outcome", *_lin(df))


def std_digital(df):
    return (df.select("session_id", "client_id", "household_id", F.col("session_ts").cast("timestamp"),
                      "channel", "primary_feature", F.col("duration_sec").cast("int"), *_lin(df))
              .withColumn("session_date", F.to_date("session_ts")))


def std_nps(df):
    return df.select("response_id", "household_id", "advisor_id", F.col("response_date").cast("date"),
                     F.col("score").cast("int"), "survey_type", *_lin(df))


def std_branches(df):
    return df.select("branch_id", "branch_name", "province", "region", F.col("opened_date").cast("date"), *_lin(df))


def std_securities(df):
    return df.select("security_id", "ticker", "security_name", "asset_class", "currency", "sector", *_lin(df))


def std_campaigns(df):
    return df.select("campaign_id", "campaign_name", "objective", "channel", F.col("start_date").cast("date"),
                     F.col("end_date").cast("date"), F.col("measurement_end_date").cast("date"),
                     "eligible_segments", "primary_metric", *_lin(df))


def std_assignments(df):
    return df.select("campaign_id", "household_id", "assignment_group", F.col("assigned_date").cast("date"), "stratum", *_lin(df))


def std_advisor_events(df):
    return df.select("advisor_id", "advisor_name", "branch_id", "designation", F.col("hire_date").cast("date"),
                     "team_type", "status", F.upper("operation").alias("operation"),
                     F.col("event_ts").cast("timestamp").alias("event_ts"), *_lin(df))


def std_household_events(df):
    return df.select("household_id", "household_name", "advisor_id", "segment",
                     F.col("onboard_date").cast("date"), "province", "status",
                     F.upper("operation").alias("operation"), F.col("event_ts").cast("timestamp").alias("event_ts"),
                     *_lin(df))


def std_account_events(df):
    return df.select("account_id", "client_id", "household_id", "account_type", "program",
                     F.col("fee_bps").cast("double"), "currency", F.col("open_date").cast("date"),
                     F.col("close_date").cast("date"), "status", F.upper("operation").alias("operation"),
                     F.col("event_ts").cast("timestamp").alias("event_ts"), *_lin(df))


# ----------------------------------------------------------------------------- specs
@dataclass(frozen=True)
class SilverSpec:
    table: str                     # silver table name
    bronze: str                    # bronze table / feed name
    pattern: str                   # upsert | scd2 | current | reference
    standardize: Callable[[DataFrame], DataFrame]
    keys: tuple[str, ...]
    order_by: tuple[str, ...] = ("_ingested_at",)
    rules_entity: str | None = None
    extra_rules: dict = field(default_factory=dict)


SILVER_SPECS: list[SilverSpec] = [
    SilverSpec("transactions", "transactions", "upsert", standardize_transactions, ("txn_id",),
               ("file_ts", "_ingested_at"), "transactions"),
    SilverSpec("valuations", "valuations", "upsert", standardize_valuations, ("account_id", "as_of_date"),
               ("file_ts", "_ingested_at"), "valuations"),
    SilverSpec("holdings", "holdings", "upsert", standardize_holdings, ("account_id", "security_id", "as_of_date"),
               ("_ingested_at",), "holdings"),
    SilverSpec("service_cases", "service_cases", "upsert", standardize_service_cases, ("case_id",),
               ("_ingested_at",), "service_cases"),
    SilverSpec("prices", "prices", "upsert", std_prices, ("security_id", "price_date"), ("_ingested_at",), "prices"),
    SilverSpec("fx_rates", "fx_rates", "upsert", std_fx, ("rate_date", "currency"), ("_ingested_at",), None,
               {"positive_rate": "rate_to_cad > 0"}),
    SilverSpec("crm_interactions", "crm_interactions", "upsert", std_crm, ("interaction_id",), ("_ingested_at",), None,
               {"valid_interaction": "interaction_id IS NOT NULL AND household_id IS NOT NULL"}),
    SilverSpec("digital_sessions", "digital_sessions", "upsert", std_digital, ("session_id",), ("_ingested_at",), None,
               {"valid_session": "session_id IS NOT NULL AND client_id IS NOT NULL"}),
    SilverSpec("nps_responses", "nps_responses", "upsert", std_nps, ("response_id",), ("_ingested_at",),
               "nps_responses"),
    SilverSpec("advisors", "advisor_events", "scd2", std_advisor_events, ("advisor_id",), ("event_ts",)),
    SilverSpec("households", "household_events", "scd2", std_household_events, ("household_id",), ("event_ts",)),
    SilverSpec("clients", "client_events", "scd2", standardize_clients, ("client_id",), ("event_ts",), "clients"),
    SilverSpec("accounts", "account_events", "current", std_account_events, ("account_id",), ("event_ts",)),
    SilverSpec("branches", "branches", "reference", std_branches, ("branch_id",)),
    SilverSpec("securities", "securities", "reference", std_securities, ("security_id",)),
    SilverSpec("campaigns", "campaigns", "reference", std_campaigns, ("campaign_id",)),
    SilverSpec("campaign_assignments", "campaign_assignments", "reference", std_assignments,
               ("campaign_id", "household_id")),
]
SPEC_BY_TABLE = {s.table: s for s in SILVER_SPECS}


def rules_for(spec: SilverSpec) -> tuple[dict, dict]:
    drop = dict(drop_rules(spec.rules_entity)) if spec.rules_entity else {}
    drop.update(spec.extra_rules)
    warn = dict(warn_rules(spec.rules_entity)) if spec.rules_entity else {}
    return drop, warn


def split_valid(spec: SilverSpec, df: DataFrame) -> tuple[DataFrame, DataFrame, list[tuple[str, str, int, int]]]:
    """Apply DROP rules (valid vs quarantine) and measure WARN rules.

    Returns (valid, quarantine, rule_results) with rule_results as
    [(rule_name, level, passed_rows, failed_rows)].
    """
    drop, warn = rules_for(spec)
    checks = {**{f"DROP::{k}": v for k, v in drop.items()}, **{f"WARN::{k}": v for k, v in warn.items()}}
    if not checks:
        return df, df.limit(0), []
    flagged = df.withColumns({f"__ok_{i}": F.coalesce(F.expr(expr), F.lit(False))
                              for i, expr in enumerate(checks.values())})
    agg = flagged.agg(*[F.sum(F.col(f"__ok_{i}").cast("int")).alias(f"p{i}") for i in range(len(checks))],
                      F.count(F.lit(1)).alias("n")).collect()[0]
    results = [(name.split("::")[1], name.split("::")[0], int(agg[f"p{i}"] or 0), int(agg["n"]) - int(agg[f"p{i}"] or 0))
               for i, name in enumerate(checks)]
    drop_idx = [i for i, name in enumerate(checks) if name.startswith("DROP::")]
    ok = F.lit(True)
    for i in drop_idx:
        ok = ok & F.col(f"__ok_{i}")
    tmp = [f"__ok_{i}" for i in range(len(checks))]
    valid = flagged.where(ok).drop(*tmp)
    quarantine = (flagged.where(~ok).drop(*tmp)
                  .withColumn("_failed_rules", failed_rules_col(drop))
                  .withColumn("_quarantined_at", F.current_timestamp()))
    return valid, quarantine, results


def build(spec: SilverSpec, bronze_df: DataFrame):
    """Pure silver build for one spec.

    upsert    -> (batch_to_merge, quarantine, rule_results)   caller MERGEs the batch
    scd2      -> (full_scd2_table, quarantine, rule_results)   caller replaces the table
    current   -> (latest_state_table, quarantine, rules)
    reference -> (full_table, quarantine, rules)
    """
    df = spec.standardize(bronze_df)
    valid, quarantine, results = split_valid(spec, df)
    if spec.pattern == "upsert":
        out = latest_by_key(valid, list(spec.keys), [c for c in spec.order_by if c in valid.columns])
    elif spec.pattern == "scd2":
        out = scd2_from_events(valid.dropDuplicates([*spec.keys, "event_ts"]), list(spec.keys), "event_ts")
    elif spec.pattern == "current":
        out = latest_by_key(valid, list(spec.keys), ["event_ts"]).drop("operation")
    else:
        out = valid.dropDuplicates(list(spec.keys))
    return out, quarantine, results


def merge_sql(target: str, source_view: str, keys: tuple[str, ...], order_by: tuple[str, ...]) -> str:
    """Iceberg MERGE that keeps the latest delivery per business key (idempotent re-runs)."""
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    newer = []
    for i, c in enumerate(order_by):
        eq = " AND ".join(f"s.{p} = t.{p}" for p in order_by[:i])
        newer.append(f"({eq + ' AND ' if eq else ''}s.{c} > t.{c})")
    cond = " OR ".join(newer)
    return (f"MERGE INTO {target} t USING {source_view} s ON {on} "
            f"WHEN MATCHED AND ({cond}) THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *")
