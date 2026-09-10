"""Run-log heartbeat so a dead pipeline announces itself.

The Aug 2026 outage went unnoticed for ~16 days because a pipeline that never
starts produces no BigQuery error rows - absence of data is not an event. A
heartbeat converts that silence into a positive signal you can alert on: if
``pipeline_run_log`` has no recent success row for a pipeline, it is down.

Usage
-----
    from pipelines.lib.heartbeat import run_logged

    with run_logged("sp_ads_daily") as run:
        rows = load_sp_ads()
        run.rows_written = len(rows)
"""

from __future__ import annotations

import datetime as _dt
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field

RUN_LOG_TABLE = "punlabs.AMZSales.pipeline_run_log"


@dataclass
class RunContext:
    pipeline: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    rows_written: int = 0


def _write(row: dict) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project="punlabs")
    errors = client.insert_rows_json(RUN_LOG_TABLE, [row])
    if errors:
        # Never let logging failure mask the real outcome - surface it instead.
        print(f"WARNING: heartbeat write failed: {errors}")


@contextmanager
def run_logged(pipeline: str):
    """Record start/end of a pipeline run, including failures, then re-raise."""
    context = RunContext(pipeline=pipeline)
    started = _dt.datetime.now(_dt.timezone.utc)

    try:
        yield context
    except BaseException as exc:
        _write(
            {
                "pipeline": pipeline,
                "run_id": context.run_id,
                "started_at": started.isoformat(),
                "finished_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "status": "FAILED",
                "rows_written": context.rows_written,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc()[:8000],
            }
        )
        raise
    else:
        _write(
            {
                "pipeline": pipeline,
                "run_id": context.run_id,
                "started_at": started.isoformat(),
                "finished_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "status": "SUCCESS",
                "rows_written": context.rows_written,
                "error": None,
                "traceback": None,
            }
        )
