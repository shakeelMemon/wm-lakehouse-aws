"""Load the gold-layer SQL files and render them for Glue (Iceberg) or local tests.

Each gold statement is an Iceberg CTAS:

    CREATE OR REPLACE TABLE ${lake}.wm_gold.<name> USING iceberg ... AS <select>

* In Glue, ``${lake}`` becomes the Spark catalog bound to the Glue Data Catalog
  (``glue_catalog``) and the statement runs as-is.
* In tests, only the SELECT body runs, against temp views named silver__<t> / gold__<t>,
  so the exact same business logic is verified without AWS.
"""
from __future__ import annotations

import re
from importlib import resources

GOLD_FILES = ["gold_dimensions.sql", "gold_performance.sql", "gold_advisor_client.sql", "gold_campaign.sql"]
_STMT = re.compile(r"CREATE OR REPLACE TABLE \$\{lake\}\.wm_gold\.(\w+)(.*?)\nAS\n(.*)", re.S)


def _read(name: str, sub: str = "gold") -> str:
    return resources.files("wm_common").joinpath("sql", sub, name).read_text()


def split_statements(text: str) -> list[str]:
    text = "\n".join(ln for ln in text.splitlines() if not ln.strip().startswith("--"))
    return [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", text) if s.strip()]


def gold_statements() -> list[tuple[str, str, str]]:
    """[(table_name, full_ctas_sql, select_body)] in dependency order."""
    out = []
    for f in GOLD_FILES:
        for st in split_statements(_read(f)):
            m = _STMT.match(st)
            if not m:
                raise ValueError(f"Unparseable statement in {f}: {st[:90]}")
            out.append((m.group(1), st, m.group(3)))
    return out


def render(sql: str, lake: str = "glue_catalog") -> str:
    return sql.replace("${lake}", lake)


def localize(sql: str) -> str:
    """Point a statement at local temp views (tests)."""
    return sql.replace("${lake}.wm_silver.", "silver__").replace("${lake}.wm_gold.", "gold__")


def athena_statements() -> list[str]:
    """CREATE OR REPLACE VIEW statements for the Athena semantic layer, in order."""
    return split_statements(_read("semantic_views.sql", "athena"))
