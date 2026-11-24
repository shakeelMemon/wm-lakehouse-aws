.PHONY: install test lint docs tf-fmt plan apply bootstrap

ENV ?= dev
TF   = terraform -chdir=infra/terraform

install:        ## local dev environment (Java 17+ needed for the Spark tests)
	pip install -e ".[dev]"

test:           ## unit + local end-to-end tests (same generator, rules and gold SQL as Glue)
	pytest

lint:
	ruff check src glue_jobs airflow tests tools

docs:           ## regenerate docs/metric_definitions.md from the metric catalog
	python tools/gen_metric_docs.py

tf-fmt:
	$(TF) fmt -recursive

plan:           ## terraform plan for ENV (dev|test|prod)
	$(TF) init -backend-config=envs/$(ENV).backend.hcl
	$(TF) plan -var-file=envs/$(ENV).tfvars

apply:
	$(TF) apply -var-file=envs/$(ENV).tfvars

bootstrap:      ## trigger the one-off history build in Airflow (MWAA CLI token flow)
	@echo "Trigger DAG wm_bootstrap in the Airflow UI (config: {\"scale\": \"small\"})"
