"""The data-health job must pass on clean data and catch a stale feed."""
import pandas as pd
import pytest

pytest.importorskip("pyspark")

from local_lakehouse import resolver  # noqa: E402

from wm_common.health import critical_failures, run_checks  # noqa: E402


def _latest(spark):
    return spark.sql("SELECT max(as_of_date) d FROM silver__valuations").collect()[0].d


def test_all_critical_checks_pass_on_clean_build(spark, lakehouse):
    res = run_checks(spark, resolver, _latest(spark), "t1")
    names = {r["check_name"] for r in res}
    assert {"recon:holdings_vs_valuations", "recon:gold_aum_vs_silver", "integrity:duplicate_txn_ids",
            "expectation:fct_aum_rollforward_monthly.bridge_balances"} <= names
    assert critical_failures(res) == [], critical_failures(res)


def test_freshness_fails_when_data_is_behind(spark, lakehouse):
    tomorrow = (pd.Timestamp(_latest(spark)) + pd.offsets.BDay(1)).date()
    res = run_checks(spark, resolver, tomorrow, "t2")
    failed = {r["check_name"] for r in critical_failures(res)}
    assert "freshness:valuations" in failed and "freshness:fct_account_daily" in failed


def test_quarantine_rate_is_reported(spark, lakehouse):
    res = run_checks(spark, resolver, _latest(spark), "t3")
    q = next(r for r in res if r["check_name"] == "quarantine:transactions_rate")
    assert 0 < q["observed_value"] < 0.01
