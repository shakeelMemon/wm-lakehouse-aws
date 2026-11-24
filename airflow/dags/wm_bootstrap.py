"""One-off (re-runnable) bootstrap: land two years of history and build every layer.

Trigger manually. Uses the same Glue jobs as the daily DAG, with history mode and
fail_on_critical=false so the first build always completes and reports its findings.
Pass {"scale": "tiny"} in the trigger config for a fast smoke test.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from airflow import DAG

# Resolved at run time (Jinja), not at DAG parse time, per MWAA best practice
ENV = "{{ var.value.get('wm_env', 'dev') }}"


def glue(task_id: str, job: str, args: dict | None = None) -> GlueJobOperator:
    return GlueJobOperator(task_id=task_id, job_name=f"wm-{ENV}-{job}", script_args=args or {},
                           wait_for_completion=True, verbose=True, deferrable=True)


with DAG(
    dag_id="wm_bootstrap",
    description="Backfill history and build bronze, silver, gold, impact and semantic layers",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Toronto"),
    catchup=False,
    max_active_runs=1,
    params={"scale": "small"},
    default_args={"owner": "wm-data-engineering", "retries": 0, "execution_timeout": timedelta(hours=2)},
    tags=["wealth-management", "bootstrap"],
) as dag:
    land = glue("land_history", "generate-landing",
                {"--mode": "history", "--scale": "{{ params.scale }}", "--as_of_date": "auto"})
    bronze = glue("bronze_ingest", "bronze-ingest")
    silver = glue("silver_build", "silver-build", {"--tables": "all"})
    gold = glue("gold_build", "gold-build", {"--tables": "all"})
    health = glue("data_health", "data-health", {"--expected_date": "auto", "--fail_on_critical": "false"})
    impact = glue("incremental_impact", "incremental-impact", {"--n_boot": "1000"})
    semantic = glue("publish_semantic", "publish-semantic")

    land >> bronze >> silver >> gold >> health >> impact >> semantic
