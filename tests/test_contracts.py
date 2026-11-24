"""Static contracts between the pieces: Glue scripts, Terraform, Airflow DAGs, specs, catalog."""
import ast
import py_compile
import re
from pathlib import Path

import yaml

from wm_common.config import FEEDS
from wm_common.silver import SILVER_SPECS, merge_sql
from wm_common.sqlrunner import gold_statements

ROOT = Path(__file__).resolve().parents[1]
TF = (ROOT / "infra" / "terraform" / "glue.tf").read_text()


def _tf_jobs() -> dict[str, str]:
    """job key -> script file, parsed from the glue_jobs map in glue.tf"""
    return dict(re.findall(r'\n\s+([a-z-]+) = \{\s*\n\s+script\s*=\s*"([\w.]+)"', TF))


def _dag_jobs(dag_file: str) -> set[str]:
    tree = ast.parse((ROOT / "airflow" / "dags" / dag_file).read_text())
    return {n.args[1].value for n in ast.walk(tree)
            if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "glue" and len(n.args) >= 2}


def test_all_python_compiles():
    for p in [*ROOT.glob("glue_jobs/*.py"), *ROOT.glob("airflow/dags/*.py"), *ROOT.glob("src/wm_common/*.py"),
              *ROOT.glob("tools/*.py")]:
        py_compile.compile(str(p), doraise=True)


def test_every_terraform_job_has_a_script():
    jobs = _tf_jobs()
    assert len(jobs) == 7, jobs
    for key, script in jobs.items():
        assert (ROOT / "glue_jobs" / script).exists(), f"{key} -> {script}"


def test_dags_only_call_jobs_that_terraform_creates():
    jobs = set(_tf_jobs())
    for dag in ("wm_daily_reporting.py", "wm_bootstrap.py"):
        used = _dag_jobs(dag)
        assert used and used <= jobs, (dag, used - jobs)


def test_every_feed_lands_in_silver():
    assert {s.bronze for s in SILVER_SPECS} == {f.name for f in FEEDS}


def test_merge_statement_is_valid_spark_sql():
    import sqlglot
    for s in SILVER_SPECS:
        if s.pattern == "upsert":
            sqlglot.parse_one(merge_sql("glue_catalog.wm_silver.x", "batch", s.keys, s.order_by), read="spark")


def test_gold_ctas_are_iceberg_and_complete():
    names = [n for n, _, _ in gold_statements()]
    assert len(names) == 18 and len(set(names)) == 18
    for _, sql, _ in gold_statements():
        assert "USING iceberg" in sql


def test_metric_catalog_sources_exist():
    cat = yaml.safe_load((ROOT / "src" / "wm_common" / "sql" / "metric_catalog.yml").read_text())["metrics"]
    views = (ROOT / "src" / "wm_common" / "sql" / "athena" / "semantic_views.sql").read_text()
    dax = (ROOT / "powerbi" / "measures.dax").read_text()
    for m in cat:
        src = m["source_view"]
        if src.startswith("wm_semantic."):
            assert f"VIEW {src} AS" in views, src
        if not m["powerbi_measure"].startswith("n/a"):
            assert re.search(rf"^{re.escape(m['powerbi_measure'])} =", dax, re.M), m["powerbi_measure"]
