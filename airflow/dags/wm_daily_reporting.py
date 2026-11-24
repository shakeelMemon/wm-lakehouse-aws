"""Daily WM performance reporting (Amazon MWAA / Apache Airflow 2.9+).

06:30 America/Toronto, Monday-Friday:

    land_daily_files -> bronze_ingest -> silver_build -> gold_build
        -> [publish_semantic, data_health]
        -> (Monday) incremental_impact
        -> refresh_powerbi (only if data_health passed)

A CRITICAL data-health failure fails the Glue job, so refresh_powerbi never runs and
Power BI keeps yesterday's correct numbers. Failures page support via the on_failure
callback (SNS topic).

Airflow Variables (set per environment):
  wm_env                     dev | test | prod
  wm_sns_alerts_topic_arn    SNS topic for failure alerts
  wm_powerbi_workspace_id    Power BI workspace (group) id
  wm_powerbi_dataset_id      Power BI semantic model (dataset) id
  wm_powerbi_secret_id       Secrets Manager id holding {tenant_id, client_id, client_secret}
"""
from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from airflow import DAG

# Resolved at run time (Jinja), not at DAG parse time, per MWAA best practice
ENV = "{{ var.value.get('wm_env', 'dev') }}"
TZ = pendulum.timezone("America/Toronto")


def glue(task_id: str, job: str, args: dict | None = None, **kw) -> GlueJobOperator:
    return GlueJobOperator(
        task_id=task_id,
        job_name=f"wm-{ENV}-{job}",
        script_args=args or {},
        wait_for_completion=True,
        verbose=True,               # stream Glue driver logs into the Airflow task log
        deferrable=True,            # frees the worker slot while Glue runs
        **kw,
    )


def notify_failure(context) -> None:
    import boto3
    ti = context["task_instance"]
    boto3.client("sns").publish(
        TopicArn=Variable.get("wm_sns_alerts_topic_arn"),
        Subject=f"[WM {Variable.get('wm_env', default_var='dev')}] {ti.dag_id}.{ti.task_id} failed",
        Message=json.dumps({"dag": ti.dag_id, "task": ti.task_id, "run_id": context["run_id"],
                            "log_url": ti.log_url, "try": ti.try_number}, indent=2),
    )


def is_monday(**context) -> bool:
    return context["logical_date"].in_timezone(TZ).day_of_week == pendulum.MONDAY


def refresh_powerbi() -> None:
    """Trigger a Power BI semantic-model refresh (service principal, REST API)."""
    import boto3
    import requests

    secret = json.loads(boto3.client("secretsmanager").get_secret_value(
        SecretId=Variable.get("wm_powerbi_secret_id"))["SecretString"])
    token = requests.post(
        f"https://login.microsoftonline.com/{secret['tenant_id']}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": secret["client_id"],
              "client_secret": secret["client_secret"], "scope": "https://analysis.windows.net/powerbi/api/.default"},
        timeout=30).json()["access_token"]
    url = (f"https://api.powerbi.com/v1.0/myorg/groups/{Variable.get('wm_powerbi_workspace_id')}"
           f"/datasets/{Variable.get('wm_powerbi_dataset_id')}/refreshes")
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json={"notifyOption": "MailOnFailure"},
                      timeout=30)
    r.raise_for_status()


default_args = {
    "owner": "wm-data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="wm_daily_reporting",
    description="Daily WM performance reporting: landing -> bronze -> silver -> gold -> semantic -> checks -> BI",
    schedule="30 6 * * 1-5",
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["wealth-management", "glue", "iceberg"],
) as dag:
    land = glue("land_daily_files", "generate-landing", {"--mode": "incremental", "--as_of_date": "auto"})
    bronze = glue("bronze_ingest", "bronze-ingest")
    silver = glue("silver_build", "silver-build", {"--tables": "all"})
    gold = glue("gold_build", "gold-build", {"--tables": "all"})
    semantic = glue("publish_semantic", "publish-semantic")
    health = glue("data_health", "data-health", {"--expected_date": "auto", "--fail_on_critical": "true"})
    monday = ShortCircuitOperator(task_id="is_monday", python_callable=is_monday,
                                  ignore_downstream_trigger_rules=False)
    impact = glue("incremental_impact", "incremental-impact", {"--n_boot": "1000"})
    powerbi = PythonOperator(task_id="refresh_powerbi", python_callable=refresh_powerbi,
                             execution_timeout=timedelta(minutes=10))

    land >> bronze >> silver >> gold >> [semantic, health]
    health >> monday >> impact
    [semantic, health] >> powerbi
