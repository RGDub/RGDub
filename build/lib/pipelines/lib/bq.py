"""BigQuery load helpers shared by every extractor.

The notebooks each hand-rolled the same three steps (coerce a DataFrame to the
table's schema, DELETE a window, append) with slightly different bugs. This is
the one copy. Rules baked in:

* A load never runs with an empty frame, and a window is never deleted unless
  there are rows to put back. An empty report must not wipe history.
* The DELETE is not swallowed. If it fails, the load stops before appending, so
  a failed delete can never double a window.
* Loads always pass the table's declared schema; autodetect is never used on
  an existing table, so an all-null day cannot flip a column type.
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

log = logging.getLogger(__name__)

PROJECT = "punlabs"


def client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def table_schema(bq, table: str):
    return bq.get_table(table).schema


def coerce_frame(df: pd.DataFrame, schema) -> pd.DataFrame:
    """Keep only columns the table has, and cast each to its BigQuery type."""
    keep = [f.name for f in schema if f.name in df.columns]
    out = df[keep].copy()
    for f in schema:
        col = f.name
        if col not in out.columns:
            continue
        t = f.field_type
        if t == "STRING":
            out[col] = out[col].map(lambda v: None if pd.isna(v) else str(v))
        elif t in ("TIMESTAMP", "DATETIME"):
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=(t == "TIMESTAMP"))
        elif t == "DATE":
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
        elif t in ("INTEGER", "INT64"):
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        elif t in ("FLOAT", "FLOAT64", "NUMERIC"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif t in ("BOOLEAN", "BOOL"):
            out[col] = out[col].map({"true": True, "false": False, "True": True, "False": False,
                                     True: True, False: False})
    return out


def delete_where(bq, table: str, where: str) -> int:
    job = bq.query(f"DELETE FROM `{table}` WHERE {where}")
    job.result()
    n = job.num_dml_affected_rows or 0
    log.info("deleted %d rows from %s where %s", n, table, where)
    return n


def load_frame(bq, df: pd.DataFrame, table: str, schema=None, write_disposition: str = "WRITE_APPEND") -> int:
    from google.cloud import bigquery

    schema = schema or table_schema(bq, table)
    df = coerce_frame(df, schema)
    # Columns the report does not carry stay NULL; the load job schema must
    # list only the columns present in the frame.
    present = [f for f in schema if f.name in df.columns]
    job = bq.load_table_from_dataframe(
        df, table, job_config=bigquery.LoadJobConfig(schema=present, write_disposition=write_disposition)
    )
    job.result()
    n = job.output_rows or 0
    log.info("loaded %d rows into %s (%s)", n, table, write_disposition)
    return n


def replace_window(bq, df: pd.DataFrame, table: str, where: str, schema=None) -> int:
    """DELETE the rows matching ``where``, then append ``df``. Skips both when ``df`` is empty."""
    if df is None or df.empty:
        log.warning("no rows for %s; leaving existing data untouched", table)
        return 0
    schema = schema or table_schema(bq, table)
    delete_where(bq, table, where)
    return load_frame(bq, df, table, schema=schema)


def sql_list(values: Iterable) -> str:
    """Render values as a quoted SQL list for an IN (...) clause."""
    return ", ".join("'" + str(v).replace("'", "\\'") + "'" for v in values)
