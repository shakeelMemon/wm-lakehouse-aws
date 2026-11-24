"""Glue job: simulate the upstream systems and drop their files into the S3 landing zone.

Stands in for the custodian / CRM / market-data / digital / service / marketing feeds.
In production this job is replaced by the real deliveries (SFTP -> S3, AppFlow, partner
buckets); everything downstream is unchanged.

Job parameters
  --raw_bucket   S3 bucket for raw files          e.g. wm-lakehouse-dev-raw-123456789012
  --mode         history | incremental
  --scale        tiny | small | medium | large
  --as_of_date   YYYY-MM-DD or "auto" (previous business day)
"""
import pandas as pd

from wm_common.config import SCALE_PRESETS, landing_path
from wm_common.generator import generate, read_state, write_landing_any, write_state
from wm_common.glue_runtime import init

spark, glue, job, args, run_id = init(["raw_bucket", "mode", "scale", "as_of_date"])

cfg = SCALE_PRESETS[args["scale"]]
as_of = args["as_of_date"]
if as_of in ("", "auto"):
    as_of = str((pd.Timestamp.now(tz="America/Toronto").normalize().tz_localize(None) - pd.offsets.BDay(1)).date())
cfg.end_date = as_of
base = landing_path(args["raw_bucket"])
state = read_state(base)

if args["mode"] == "incremental" and not state:
    raise RuntimeError("No generator state in the landing zone. Run once with --mode history first.")
since = state.get("last_written_date") if args["mode"] == "incremental" else None

if since and since >= as_of:
    print(f"Landing already current through {since}; nothing to do.")
else:
    written = write_landing_any(generate(cfg), base, since=since,
                                granularity="day" if args["mode"] == "incremental" else "month")
    write_state(base, cfg, as_of)
    print(f"Wrote {sum(written.values()):,} rows across {len(written)} feeds through {as_of}: {written}")

job.commit()
