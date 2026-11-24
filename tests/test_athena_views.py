"""Athena semantic views: valid Trino SQL, and every column they reference exists in gold.

Each view is parsed with sqlglot's Athena dialect, transpiled to Spark SQL and executed
against the local gold build, so a renamed gold column breaks this test, not Power BI.
"""
import pandas as pd
import pytest

sqlglot = pytest.importorskip("sqlglot")
pytest.importorskip("pyspark")

from wm_common.sqlrunner import athena_statements  # noqa: E402

LOCAL = {"wm_ops.dq_check_results": "local_dq_check_results",
         "wm_gold.campaign_impact_results": "local_campaign_impact"}


def _select_part(stmt: str) -> tuple[str, str]:
    head, body = stmt.split(" AS\n", 1)
    return head.split("VIEW", 1)[1].strip(), body


def test_views_parse_as_athena_sql():
    stmts = athena_statements()
    assert len(stmts) >= 14
    for s in stmts:
        assert s.startswith("CREATE OR REPLACE VIEW wm_semantic.")
        sqlglot.parse_one(s, read="athena")


def test_views_execute_against_local_gold(spark, lakehouse):
    spark.createDataFrame(pd.DataFrame([dict(
        campaign_id="C", campaign_name="c", metric="m", dimension="Quantity", verdict="v", did_estimate=1.0,
        ci_low=0.0, ci_high=2.0, p_value=0.01, relative_lift=0.1, total_incremental=10.0, n_treatment=40,
        n_control=40, pre_trend_ok=True, is_primary_metric=True, post_months_observed=3,
        measurement_complete=True, run_at=pd.Timestamp("2026-09-01"))])).createOrReplaceTempView("local_campaign_impact")
    spark.sql("""SELECT 'r1' run_id, current_timestamp() checked_at, 'x' check_name, 'Freshness' category,
                 'CRITICAL' severity, 'PASS' status, 0.0 observed_value, '0' threshold, '' details""") \
        .createOrReplaceTempView("local_dq_check_results")
    for stmt in athena_statements():
        view, body = _select_part(stmt)
        for k, v in LOCAL.items():
            body = body.replace(k, v)
        spark_sql = sqlglot.transpile(body, read="athena", write="spark")[0]
        spark_sql = spark_sql.replace("wm_gold.", "gold__")
        df = spark.sql(spark_sql)
        assert df.columns, view
        df.limit(5).collect()


def test_kpi_view_matches_certified_roa(spark, lakehouse):
    stmt = next(s for s in athena_statements() if "v_kpi_monthly" in s)
    body = sqlglot.transpile(_select_part(stmt)[1], read="athena", write="spark")[0].replace("wm_gold.", "gold__")
    kpi = spark.sql(body).toPandas().sort_values("month")
    raw = spark.sql("""SELECT month_start, sum(total_revenue_cad) * 12 * 10000 / sum((begin_aum_cad + end_aum_cad) / 2) r
                       FROM gold__fct_aum_rollforward_monthly GROUP BY 1 ORDER BY 1""").toPandas()
    assert kpi.roa_bps.round(6).tolist() == raw.r.astype(float).round(6).tolist()
