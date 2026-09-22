"""Minimal Amazon Selling Partner API client: LWA auth, reports, finances, AWD.

One implementation of what nine notebooks each re-implemented by hand, with the
things they were missing: a polling timeout, 429/5xx retry, pagination on the
Finances and AWD endpoints, and explicit report decoding.

    from pipelines.lib.spapi import SpApiClient, spapi_client_from_secrets

    sp = spapi_client_from_secrets()
    text = sp.run_report("GET_LEDGER_SUMMARY_VIEW_DATA", start, end,
                         report_options={"aggregateByLocation": "COUNTRY",
                                         "aggregatedByTimePeriod": "DAILY"})
"""

from __future__ import annotations

import datetime as dt
import gzip
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

import requests

log = logging.getLogger(__name__)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
ENDPOINTS = {
    "NA": "https://sellingpartnerapi-na.amazon.com",
    "EU": "https://sellingpartnerapi-eu.amazon.com",
    "FE": "https://sellingpartnerapi-fe.amazon.com",
}
MARKETPLACE_US = "ATVPDKIKX0DER"

# Secret Manager ids for the SP-API LWA app (distinct from the Ads API app).
SECRET_IDS = {
    "client_id": "amz-lwa-client-id",
    "client_secret": "amz-lwa-client-secret",
    "refresh_token": "amz-refresh-token",
}


class SpApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


class ReportFailed(SpApiError):
    """Amazon marked the report FATAL/CANCELLED, or it did not finish in time."""


def iso_z(t: dt.datetime) -> str:
    """SP-API wants ISO 8601 with a Z suffix and no fractional seconds."""
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class SpApiClient:
    client_id: str
    client_secret: str
    refresh_token: str
    region: str = "NA"
    marketplace_id: str = MARKETPLACE_US
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 8
    _access_token: str | None = field(default=None, repr=False)
    _token_expires_at: float = field(default=0.0, repr=False)

    @property
    def base_url(self) -> str:
        return ENDPOINTS[self.region]

    # ------------------------------------------------------------------ auth
    def access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token
        resp = self.session.post(LWA_TOKEN_URL, data={
            "grant_type": "refresh_token", "refresh_token": self.refresh_token,
            "client_id": self.client_id, "client_secret": self.client_secret,
        }, timeout=30)
        if resp.status_code != 200:
            raise SpApiError(f"LWA token refresh failed: HTTP {resp.status_code} {resp.text[:300]}",
                             resp.status_code, resp.text)
        payload = resp.json()
        self._access_token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._access_token

    # ------------------------------------------------------------- transport
    def request(self, method: str, path: str, *, params: dict | None = None,
                json_body: Any | None = None, timeout: int = 60) -> requests.Response:
        """HTTP call with retry on 429 / 5xx. Re-reads the token on every attempt."""
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(
                    method, url, params=params, json=json_body, timeout=timeout,
                    headers={"x-amz-access-token": self.access_token(), "Content-Type": "application/json"},
                )
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                # Amazon occasionally drops a long-lived connection; treat like a 5xx.
                if attempt == self.max_retries:
                    raise
                log.warning("SP-API %s %s: %s; retrying in %.0fs (%d/%d)", method, path, exc, delay, attempt, self.max_retries)
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue
            if resp.status_code < 400:
                return resp
            retryable = resp.status_code == 429 or resp.status_code >= 500
            if not retryable or attempt == self.max_retries:
                raise SpApiError(f"{method} {path} -> HTTP {resp.status_code}: {resp.text[:500]}",
                                 resp.status_code, resp.text)
            # createReport is limited to ~1/min with a small burst; a 429 there
            # needs a real pause, not a two-second one.
            wait = float(resp.headers.get("Retry-After") or max(delay, 15.0 if resp.status_code == 429 else 0))
            log.warning("SP-API %s %s returned %s; retrying in %.0fs (%d/%d)",
                        method, path, resp.status_code, wait, attempt, self.max_retries)
            time.sleep(wait)
            delay = min(delay * 2, 120)
        raise AssertionError("unreachable")

    # --------------------------------------------------------------- reports
    def create_report(self, report_type: str, start: dt.datetime, end: dt.datetime,
                      report_options: dict | None = None, marketplace_ids: list[str] | None = None) -> str:
        body: dict[str, Any] = {
            "reportType": report_type,
            "dataStartTime": iso_z(start),
            "dataEndTime": iso_z(end),
            "marketplaceIds": marketplace_ids or [self.marketplace_id],
        }
        if report_options:
            body["reportOptions"] = report_options
        resp = self.request("POST", "/reports/2021-06-30/reports", json_body=body)
        return resp.json()["reportId"]

    def wait_for_report(self, report_id: str, timeout_s: int = 1800, poll_s: int = 30) -> str:
        """Poll until DONE and return the reportDocumentId. Bounded, unlike the notebooks."""
        deadline = time.time() + timeout_s
        while True:
            status = self.request("GET", f"/reports/2021-06-30/reports/{report_id}").json()
            state = status.get("processingStatus")
            if state == "DONE":
                return status["reportDocumentId"]
            if state in ("FATAL", "CANCELLED"):
                raise ReportFailed(f"report {report_id} ended {state}")
            if time.time() + poll_s > deadline:
                raise ReportFailed(f"report {report_id} still {state} after {timeout_s}s")
            log.info("report %s is %s; polling again in %ds", report_id, state, poll_s)
            time.sleep(poll_s)

    def download_document(self, document_id: str, encoding: str = "utf-8") -> str:
        """Fetch a report document and return its text, gunzipped if Amazon compressed it."""
        doc = self.request("GET", f"/reports/2021-06-30/documents/{document_id}").json()
        raw = self.session.get(doc["url"], timeout=300)
        raw.raise_for_status()
        content = raw.content
        if doc.get("compressionAlgorithm") == "GZIP" or content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)
        return content.decode(encoding, errors="replace")

    def run_report(self, report_type: str, start: dt.datetime, end: dt.datetime, *,
                   report_options: dict | None = None, encoding: str = "utf-8",
                   timeout_s: int = 1800) -> str:
        """Create, wait for, and download one report. Returns the document text."""
        report_id = self.create_report(report_type, start, end, report_options)
        log.info("created %s report %s (%s..%s)", report_type, report_id, start.date(), end.date())
        return self.download_document(self.wait_for_report(report_id, timeout_s=timeout_s), encoding=encoding)

    def list_reports(self, report_types: Iterable[str], created_since: dt.datetime,
                     created_until: dt.datetime | None = None, processing_statuses: str = "DONE") -> list[dict]:
        """Reports Amazon generated on its own (settlements), newest first, all pages."""
        params: dict[str, Any] = {
            "reportTypes": ",".join(report_types), "processingStatuses": processing_statuses,
            "createdSince": iso_z(created_since), "pageSize": 100,
        }
        if created_until:
            params["createdUntil"] = iso_z(created_until)
        out: list[dict] = []
        while True:
            data = self.request("GET", "/reports/2021-06-30/reports", params=params).json()
            out.extend(data.get("reports", []))
            token = data.get("nextToken")
            if not token:
                return out
            params = {"nextToken": token}

    # -------------------------------------------------------------- finances
    def financial_events(self, posted_after: dt.datetime, posted_before: dt.datetime,
                         page_size: int = 100) -> Iterator[dict]:
        """Every FinancialEvents page merged, following NextToken until exhausted.

        Yields one dict per page (the ``FinancialEvents`` object). The notebook
        this replaces read only the first page, which silently capped pending
        finances at 100 events.
        """
        params: dict[str, Any] = {"PostedAfter": iso_z(posted_after), "PostedBefore": iso_z(posted_before),
                                  "MaxResultsPerPage": page_size}
        while True:
            payload = self.request("GET", "/finances/v0/financialEvents", params=params).json().get("payload", {})
            yield payload.get("FinancialEvents", {})
            token = payload.get("NextToken")
            if not token:
                return
            params = {"NextToken": token}

    # ------------------------------------------------------------------- AWD
    def awd_inventory(self, page_size: int = 200) -> Iterator[dict]:
        """Every AWD inventory item across pages (the notebook read only the first)."""
        params: dict[str, Any] = {"maxResults": page_size}
        while True:
            data = self.request("GET", "/awd/2024-05-09/inventory", params=params).json()
            yield from data.get("inventory", [])
            token = data.get("nextToken")
            if not token:
                return
            params = {"maxResults": page_size, "nextToken": token}


def spapi_client_from_secrets(region: str = "NA", secret_ids: dict[str, str] | None = None) -> SpApiClient:
    from pipelines.lib.secrets import get_secret, preflight

    ids = {**SECRET_IDS, **(secret_ids or {})}
    preflight(ids.values())
    return SpApiClient(
        client_id=get_secret(ids["client_id"]).strip(),
        client_secret=get_secret(ids["client_secret"]).strip(),
        refresh_token=get_secret(ids["refresh_token"]).strip(),
        region=region,
    )
