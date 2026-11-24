"""Data and pipeline health checks (run by the wm_data_health Glue job after every build).

``run_checks`` takes a name resolver so the identical checks run against Iceberg tables in
Glue (glue_catalog.wm_silver.valuations) and against temp views in tests (silver__valuations).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Callable

from pyspark.sql import SparkSession

from .quality import GOLD_EXPECTATIONS

Resolver = Callable[[str, str], str]   # (layer, table) -> SQL name


def run_checks(spark: SparkSession, T: Resolver, expected_date: date, run_id: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    out: list[dict] = []

    def record(check, category, severity, passed, observed, threshold, details=""):
        out.append(dict(run_id=run_id, checked_at=now, check_name=check, category=category, severity=severity,
                        status="PASS" if passed else "FAIL",
                        observed_value=None if observed is None else float(observed),
                        threshold=str(threshold), details=details))

    def scalar(sql):
        return spark.sql(sql).collect()[0][0]

    # Freshness --------------------------------------------------------------------
    for layer, table, col in [("silver", "valuations", "as_of_date"), ("silver", "transactions", "trade_date"),
                              ("silver", "prices", "price_date"), ("gold", "fct_account_daily", "as_of_date"),
                              ("gold", "fct_aum_daily", "as_of_date")]:
        latest = scalar(f"SELECT max({col}) FROM {T(layer, table)}")
        lag = (expected_date - latest).days if latest else 999
        record(f"freshness:{table}", "Freshness", "CRITICAL", latest is not None and latest >= expected_date,
               max(lag, 0), "current to expected business date", f"latest={latest} expected={expected_date}")

    # Volume -----------------------------------------------------------------------
    vol = spark.sql(f"""SELECT as_of_date, count(*) n FROM {T('silver', 'valuations')}
                        GROUP BY 1 ORDER BY 1 DESC LIMIT 21""").toPandas()
    if len(vol) >= 5:
        today, hist = vol.n.iloc[0], vol.n.iloc[1:]
        z = (today - hist.mean()) / (hist.std() or 1)
        drop = 1 - today / hist.mean()
        record("volume:valuations_zscore", "Volume", "WARNING", abs(z) < 3, round(float(z), 2), "|z| < 3")
        record("volume:valuations_drop", "Volume", "CRITICAL", drop < 0.2, round(float(drop), 4), "< 20% drop")

    # Reconciliation ---------------------------------------------------------------
    g = spark.sql(f"""
        WITH me AS (SELECT max(as_of_date) d FROM {T('silver', 'holdings')}),
        h AS (SELECT account_id, sum(market_value_cad) hv FROM {T('silver', 'holdings')}
              WHERE as_of_date = (SELECT d FROM me) GROUP BY 1),
        v AS (SELECT account_id, market_value_cad mv FROM {T('silver', 'valuations')}
              WHERE as_of_date = (SELECT d FROM me))
        SELECT count(*) accounts, count_if(abs(coalesce(h.hv, 0) - coalesce(v.mv, 0)) > 1) breaks
        FROM h FULL OUTER JOIN v ON h.account_id = v.account_id""").collect()[0]
    record("recon:holdings_vs_valuations", "Reconciliation", "CRITICAL",
           g.breaks <= 0.001 * max(g.accounts, 1), g.breaks, "<= 0.1% of accounts off by > $1",
           f"accounts={g.accounts}")
    gap = scalar(f"""SELECT abs(
        (SELECT sum(aum_cad) FROM {T('gold', 'fct_aum_daily')}
          WHERE as_of_date = (SELECT max(as_of_date) FROM {T('gold', 'fct_aum_daily')}))
      - (SELECT sum(market_value_cad) FROM {T('silver', 'valuations')}
          WHERE as_of_date = (SELECT max(as_of_date) FROM {T('gold', 'fct_aum_daily')})))""")
    record("recon:gold_aum_vs_silver", "Reconciliation", "CRITICAL", gap is not None and float(gap) < 1,
           gap, "< $1", "gold AUM must equal custodian valuations")

    # Integrity --------------------------------------------------------------------
    orph = scalar(f"""SELECT count(DISTINCT v.account_id) FROM {T('silver', 'valuations')} v
                      LEFT ANTI JOIN {T('gold', 'dim_account')} a ON v.account_id = a.account_id""")
    record("integrity:orphan_valuation_accounts", "Integrity", "CRITICAL", orph == 0, orph, "0")
    dups = scalar(f"SELECT count(*) - count(DISTINCT txn_id) FROM {T('silver', 'transactions')}")
    record("integrity:duplicate_txn_ids", "Integrity", "CRITICAL", dups == 0, dups, "0")

    # Gold row-level expectations ---------------------------------------------------
    for table, name, predicate, severity in GOLD_EXPECTATIONS:
        bad = scalar(f"SELECT count_if(NOT coalesce(({predicate}), false)) FROM {T('gold', table)}")
        record(f"expectation:{table}.{name}", "Expectations", severity, bad == 0, bad, "0 violating rows")

    # Quarantine rates ---------------------------------------------------------------
    for ent in ("transactions", "valuations"):
        try:
            q = scalar(f"SELECT count(*) FROM {T('silver', ent + '_quarantine')}")
        except Exception:
            q = 0
        n = scalar(f"SELECT count(*) FROM {T('silver', ent)}")
        rate = q / max(n, 1)
        record(f"quarantine:{ent}_rate", "Quarantine", "WARNING", rate < 0.01, round(rate, 5), "< 1%",
               f"quarantined={q} accepted={n}")
    return out


def critical_failures(results: list[dict]) -> list[dict]:
    return [r for r in results if r["status"] == "FAIL" and r["severity"] == "CRITICAL"]
