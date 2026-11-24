"""Declarative data-quality rules shared by the Glue silver job, the health checks and the tests.

Each rule is a SQL boolean expression that must hold for a valid record.
* ``DROP`` rules: failing rows are removed from silver and written to <table>_quarantine.
* ``WARN`` rules: rows are kept; pass/fail counts go to wm_ops.dq_rule_results.
"""
from __future__ import annotations

from .config import VALID_TXN_TYPES

_types = ", ".join(f"'{t}'" for t in VALID_TXN_TYPES)

RULES: dict[str, dict[str, dict[str, str]]] = {
    "transactions": {
        "DROP": {
            "valid_txn_id": "txn_id IS NOT NULL",
            "valid_account": "account_id IS NOT NULL",
            "valid_txn_type": f"txn_type IN ({_types})",
            "valid_trade_date": "trade_date IS NOT NULL AND trade_date <= current_date()",
            "valid_amount": "gross_amount_cad IS NOT NULL",
        },
        "WARN": {
            "settles_after_trade": "settle_date >= trade_date",
            "trade_has_security": "txn_type NOT IN ('BUY','SELL') OR security_id IS NOT NULL",
            "trade_quantity_positive": "txn_type NOT IN ('BUY','SELL') OR quantity > 0",
        },
    },
    "valuations": {
        "DROP": {
            "valid_account": "account_id IS NOT NULL AND account_id RLIKE '^AC[0-9]{8}$'",
            "valid_date": "as_of_date IS NOT NULL",
            "non_negative_mv": "market_value_cad >= 0",
        },
        "WARN": {
            "cash_within_mv": "cash_cad <= market_value_cad + 0.01",
        },
    },
    "holdings": {
        "DROP": {"valid_keys": "account_id IS NOT NULL AND security_id IS NOT NULL AND as_of_date IS NOT NULL"},
        "WARN": {"non_negative_position": "market_value_cad >= 0"},
    },
    "clients": {
        "DROP": {"valid_client_id": "client_id IS NOT NULL", "valid_household": "household_id IS NOT NULL"},
        "WARN": {"valid_email": "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]{2,}$'",
                 "plausible_birth_year": "birth_year BETWEEN 1900 AND year(current_date()) - 18"},
    },
    "prices": {
        "DROP": {"valid_price": "close_price > 0 AND price_date IS NOT NULL"},
        "WARN": {},
    },
    "service_cases": {
        "DROP": {"valid_case": "case_id IS NOT NULL AND opened_ts IS NOT NULL"},
        "WARN": {"closed_after_opened": "closed_ts IS NULL OR closed_ts >= opened_ts"},
    },
    "nps_responses": {
        "DROP": {"valid_score": "score BETWEEN 0 AND 10"},
        "WARN": {},
    },
}


def drop_rules(entity: str) -> dict[str, str]:
    return RULES.get(entity, {}).get("DROP", {})


def warn_rules(entity: str) -> dict[str, str]:
    return RULES.get(entity, {}).get("WARN", {})


def quarantine_predicate(entity: str) -> str:
    """Rows that violate at least one DROP rule (NULL-safe)."""
    rules = drop_rules(entity).values()
    if not rules:
        return "false"
    return " OR ".join(f"NOT coalesce(({r}), false)" for r in rules)


# Row-level expectations on gold tables (checked by the data-health job after every build).
# severity CRITICAL fails the Airflow run before BI refresh; WARNING is recorded only.
GOLD_EXPECTATIONS: list[tuple[str, str, str, str]] = [
    # (table, check name, predicate every row must satisfy, severity)
    ("dim_advisor", "pk_advisor_not_null", "advisor_id IS NOT NULL", "CRITICAL"),
    ("dim_household", "pk_household_not_null", "household_id IS NOT NULL", "CRITICAL"),
    ("dim_account", "pk_account_not_null", "account_id IS NOT NULL", "CRITICAL"),
    ("fct_account_daily", "non_negative_mv", "market_value_cad >= 0", "CRITICAL"),
    ("fct_account_daily", "plausible_daily_return", "daily_return IS NULL OR abs(daily_return) < 0.25", "WARNING"),
    ("fct_account_monthly_performance", "plausible_monthly_twr", "twr IS NULL OR abs(twr) < 0.6", "WARNING"),
    ("fct_aum_rollforward_monthly", "bridge_balances",
     "abs(begin_aum_cad + net_flows_cad + market_movement_cad - end_aum_cad) < 1", "CRITICAL"),
    ("fct_household_monthly", "pk_household_month", "household_id IS NOT NULL AND month_start IS NOT NULL", "CRITICAL"),
]
