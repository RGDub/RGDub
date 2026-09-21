"""Ads billing invoices -> punlabs.AMZSales.PL-AMZSales-AdsInvoices.

Replaces notebook task ``PunData-DailyAMZAdINVCheck``. Lists invoice summaries
issued in the last 30 days through the Ads billing API (scoped by the global
advertising account id rather than a profile), deletes those invoice ids and
appends.

    from pipelines.ads_invoices import run
    run()
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

from pipelines.lib import bq as bqlib
from pipelines.lib.ads_api import AdsApiClient, ads_client_from_secrets
from pipelines.lib.heartbeat import run_logged

log = logging.getLogger(__name__)

TABLE = "punlabs.AMZSales.PL-AMZSales-AdsInvoices"
GLOBAL_ACCOUNT_ID = "amzn1.ads-account.g.1xtd44unfxu21xbz5lxkw6lmt"
MEDIA_TYPE = "application/vnd.billingInvoiceSummary.v1+json"
LOOKBACK_DAYS = 30


def list_invoices(client: AdsApiClient, start: dt.date, end: dt.date) -> list[dict]:
    body = {"invoiceIssuedDateRangeFilter": {"startDate": start.isoformat(), "endDate": end.isoformat()},
            "maxResults": 100}
    headers = {"Authorization": f"Bearer {client.access_token()}",
               "Amazon-Advertising-API-ClientId": client.client_id,
               "Amazon-Ads-AccountId": GLOBAL_ACCOUNT_ID,
               "Accept": MEDIA_TYPE, "Content-Type": MEDIA_TYPE}
    out: list[dict] = []
    while True:
        resp = client.session.post(f"{client.base_url}/invoiceSummaries/list", headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"invoiceSummaries/list -> HTTP {resp.status_code}: {resp.text[:400]}")
        data = resp.json()
        out.extend(data.get("invoiceSummaries", []))
        token = data.get("nextToken")
        if not token:
            return out
        body["nextToken"] = token


def transform(invoices: list[dict]) -> pd.DataFrame:
    rows = []
    for inv in invoices:
        status = inv.get("status")
        if isinstance(status, dict):
            status = status.get("code")
        period = inv.get("billingPeriod") or {}
        total, due = inv.get("totalAmount") or {}, inv.get("amountDue") or {}
        amount = total.get("value") or total.get("amount")
        if amount is None:
            amount = due.get("value") or due.get("amount", 0.0)
        rows.append({
            "invoice_id": inv.get("id"), "status": status,
            "from_date": period.get("startDate") or inv.get("fromDate"),
            "to_date": period.get("endDate") or inv.get("toDate"),
            "invoice_date": inv.get("invoiceIssuedDate") or inv.get("invoiceDate"),
            "amount": amount, "currency": total.get("currencyCode") or due.get("currencyCode", "USD"),
        })
    df = pd.DataFrame(rows)
    for col in ("from_date", "to_date", "invoice_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def run(*, dry_run: bool = False, client: AdsApiClient | None = None) -> int:
    with run_logged("ads_invoices_daily", enabled=not dry_run) as ctx:
        ads = client or ads_client_from_secrets()
        today = dt.date.today()
        invoices = list_invoices(ads, today - dt.timedelta(days=LOOKBACK_DAYS), today)
        log.info("%d invoices in the last %d days", len(invoices), LOOKBACK_DAYS)
        if not invoices:
            return 0
        df = transform(invoices)
        if dry_run:
            return len(df)
        bq = bqlib.client()
        ctx.rows_written = bqlib.replace_window(
            bq, df, TABLE, where=f"invoice_id IN ({bqlib.sql_list(df['invoice_id'].astype(str))})")
        return ctx.rows_written


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run(dry_run="--dry-run" in sys.argv))
