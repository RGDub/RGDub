"""SP-API Reports 2021-06-30: request, poll, download, parse.

Why this module exists
----------------------
The inventory-ledger notebook died on 2026-09-18 with::

    EmptyDataError: No columns to parse from file

after printing ``Status: DONE``. That combination is not a bug in Amazon's
report and not a transport failure - it is SP-API's ordinary way of saying
*"the job succeeded and matched no rows."* When a report has no data, the
report document is a **zero-byte file**, not a header-only TSV. ``read_csv``
therefore raises before any ``df.empty`` check can run, so the notebook's
"no data found" branch was unreachable and every empty day looked like a
crash.

This module draws the distinction the notebook could not:

* a report that produced no rows returns an **empty DataFrame** (and says so),
* a report that genuinely failed raises, loudly, with the API payload attached.

It also fixes four smaller traps in the hand-rolled version:

1. **Token churn.** The old loop called ``get_access_token()`` on every poll,
   which meant three Secret Manager round trips plus one LWA exchange every
   30 seconds. :class:`LwaTokenProvider` caches the token until it is nearly
   expired. (LWA is rate limited at 1 request/second; a tight poll loop over
   several reports can trip it.)
2. **Unbounded polling.** ``while True`` with no deadline means a report stuck
   ``IN_QUEUE`` pins a Colab Enterprise runtime until the schedule kills it.
   :func:`wait_for_report` takes a timeout.
3. **Unchecked responses.** ``requests.get(...).json()`` on a throttled poll
   yields no ``processingStatus``, so a 429 span looked identical to a slow
   report. Every response is now status-checked.
4. **Compression sniffing.** ``compressionAlgorithm`` was compared exactly
   against ``"GZIP"``; anything else fell through to ``.text`` and produced
   mojibake rather than an error. The payload's magic bytes are authoritative.

Usage
-----
    from pipelines.lib.spapi_reports import LwaTokenProvider, fetch_report

    tokens = LwaTokenProvider()
    result = fetch_report(
        tokens,
        report_type="GET_LEDGER_SUMMARY_VIEW_DATA",
        marketplace_ids=["ATVPDKIKX0DER"],
        data_start_time="2026-09-16T00:00:00Z",
        data_end_time="2026-09-17T00:00:00Z",
        report_options={"aggregateByLocation": "COUNTRY",
                        "aggregatedByTimePeriod": "DAILY"},
    )
    if result.is_empty:
        print(result.describe())   # why it was empty, not a traceback
    else:
        print(result.frame.shape)
"""

from __future__ import annotations

import gzip
import io
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import pandas as pd
import requests

from pipelines.lib.secrets import get_secret

# Regional endpoint for North America (ATVPDKIKX0DER / CA / MX).
NA_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

# Terminal processing states. DONE is success; the rest are failures.
_DONE = "DONE"
_FAILED_STATES = frozenset({"FATAL", "CANCELLED"})

_HTTP_TIMEOUT = 60


class ReportError(RuntimeError):
    """A report could not be produced or retrieved. Always fatal."""


class LwaTokenProvider:
    """Mints and caches SP-API access tokens from a Login-with-Amazon refresh token.

    Access tokens live for one hour. The previous implementation re-minted one
    on every poll; this one re-mints only when fewer than ``refresh_margin``
    seconds remain, so a long poll costs a single exchange.
    """

    def __init__(
        self,
        client_id_secret: str = "amz-lwa-client-id",
        client_secret_secret: str = "amz-lwa-client-secret",
        refresh_token_secret: str = "amz-refresh-token",
        refresh_margin: int = 300,
    ) -> None:
        self._client_id_secret = client_id_secret
        self._client_secret_secret = client_secret_secret
        self._refresh_token_secret = refresh_token_secret
        self._refresh_margin = refresh_margin
        self._token: str | None = None
        self._expires_at: float = 0.0

    def token(self) -> str:
        """Return a valid access token, minting a new one only when needed."""
        if self._token is not None and time.time() < self._expires_at:
            return self._token

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": get_secret(self._refresh_token_secret),
            "client_id": get_secret(self._client_id_secret),
            "client_secret": get_secret(self._client_secret_secret),
        }
        response = requests.post(LWA_TOKEN_URL, data=payload, timeout=_HTTP_TIMEOUT)
        if response.status_code != 200:
            # Do not echo the body verbatim - it can quote the refresh token back.
            raise ReportError(
                f"LWA token exchange failed: HTTP {response.status_code}. "
                "Check the refresh token and client credentials in Secret Manager; "
                "a rotated or revoked refresh token gives 400 invalid_grant."
            )

        body = response.json()
        self._token = body["access_token"]
        # expires_in is seconds (3600 in practice); refresh a little early.
        self._expires_at = time.time() + int(body.get("expires_in", 3600)) - self._refresh_margin
        return self._token

    def headers(self) -> dict[str, str]:
        return {"x-amz-access-token": self.token(), "Content-Type": "application/json"}


@dataclass
class ReportResult:
    """Outcome of one report request.

    ``frame`` is always a DataFrame. When the report matched no rows it is an
    empty one - callers branch on :attr:`is_empty` rather than catching parse
    errors.
    """

    frame: pd.DataFrame
    report_id: str
    document_id: str
    byte_count: int
    data_start_time: str
    data_end_time: str
    report_options: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return self.frame.empty

    def describe(self) -> str:
        """A one-paragraph explanation suitable for a scheduled run's log."""
        window = f"{self.data_start_time} .. {self.data_end_time}"
        if not self.is_empty:
            return (
                f"Report {self.report_id} returned {len(self.frame)} rows "
                f"({self.byte_count} bytes) for {window}."
            )
        return (
            f"Report {self.report_id} completed with status DONE but its document "
            f"({self.document_id}) was {self.byte_count} bytes - Amazon matched no "
            f"rows for {window} with options {dict(self.report_options)}. "
            "This is an API-level 'no data', not a transport failure. Usual causes, "
            "most likely first: (1) the window is too recent and the ledger has not "
            "settled yet - widen the lookback; (2) the window does not cover a whole "
            "aggregation period, since the summary view snaps to period boundaries "
            "and a partial period can match nothing; (3) a misspelled key in "
            "reportOptions, which SP-API accepts and silently ignores."
        )


def request_report(
    tokens: LwaTokenProvider,
    report_type: str,
    marketplace_ids: Sequence[str],
    data_start_time: str,
    data_end_time: str,
    report_options: Mapping[str, str] | None = None,
    endpoint: str = NA_ENDPOINT,
) -> str:
    """Create a report job and return its reportId."""
    payload: dict[str, Any] = {
        "reportType": report_type,
        "dataStartTime": data_start_time,
        "dataEndTime": data_end_time,
        "marketplaceIds": list(marketplace_ids),
    }
    if report_options:
        payload["reportOptions"] = dict(report_options)

    response = requests.post(
        f"{endpoint}/reports/2021-06-30/reports",
        headers=tokens.headers(),
        json=payload,
        timeout=_HTTP_TIMEOUT,
    )
    if response.status_code not in (200, 202):
        raise ReportError(
            f"createReport({report_type}) failed: HTTP {response.status_code} "
            f"{response.text}. Payload was {payload}."
        )
    report_id = response.json().get("reportId")
    if not report_id:
        raise ReportError(f"createReport returned no reportId: {response.text}")
    return report_id


def wait_for_report(
    tokens: LwaTokenProvider,
    report_id: str,
    endpoint: str = NA_ENDPOINT,
    poll_seconds: int = 30,
    timeout_seconds: int = 1800,
) -> str:
    """Poll a report to completion and return its reportDocumentId.

    Raises :class:`ReportError` on FATAL/CANCELLED or when ``timeout_seconds``
    elapses, so a stuck report cannot pin the runtime indefinitely.
    """
    deadline = time.time() + timeout_seconds
    url = f"{endpoint}/reports/2021-06-30/reports/{report_id}"

    while True:
        response = requests.get(url, headers=tokens.headers(), timeout=_HTTP_TIMEOUT)

        if response.status_code == 429:
            # Throttled, not finished. Back off without treating it as a status.
            print("  getReport throttled (429); backing off")
            time.sleep(poll_seconds)
            if time.time() > deadline:
                raise ReportError(
                    f"Report {report_id} still throttled after {timeout_seconds}s."
                )
            continue

        if response.status_code != 200:
            raise ReportError(
                f"getReport({report_id}) failed: HTTP {response.status_code} "
                f"{response.text}"
            )

        body = response.json()
        status = body.get("processingStatus")
        print(f"  Status: {status}")

        if status == _DONE:
            document_id = body.get("reportDocumentId")
            if not document_id:
                raise ReportError(f"Report {report_id} is DONE but carries no document: {body}")
            return document_id

        if status in _FAILED_STATES:
            raise ReportError(f"Report {report_id} ended in {status}: {body}")

        if time.time() > deadline:
            raise ReportError(
                f"Report {report_id} was still '{status}' after {timeout_seconds}s. "
                "Amazon's queue can lag; re-run, or raise timeout_seconds."
            )

        time.sleep(poll_seconds)


def download_report_document(
    tokens: LwaTokenProvider,
    document_id: str,
    endpoint: str = NA_ENDPOINT,
) -> bytes:
    """Fetch a report document's bytes, decompressing GZIP when present.

    Returns ``b""`` for a report with no rows - that is a legitimate result,
    not an error.
    """
    meta_response = requests.get(
        f"{endpoint}/reports/2021-06-30/documents/{document_id}",
        headers=tokens.headers(),
        timeout=_HTTP_TIMEOUT,
    )
    if meta_response.status_code != 200:
        raise ReportError(
            f"getReportDocument({document_id}) failed: HTTP "
            f"{meta_response.status_code} {meta_response.text}"
        )

    meta = meta_response.json()
    url = meta.get("url")
    if not url:
        raise ReportError(f"Report document {document_id} carries no download URL: {meta}")

    # The download URL is pre-signed. Sending the SP-API token to it would make
    # S3 reject the request, so this call deliberately carries no auth headers.
    payload_response = requests.get(url, timeout=_HTTP_TIMEOUT)
    payload_response.raise_for_status()
    payload = payload_response.content

    # Trust the magic bytes over the advertised algorithm: the header is absent
    # for uncompressed documents and has varied in case across marketplaces.
    if payload[:2] == b"\x1f\x8b":
        payload = gzip.decompress(payload)
    elif meta.get("compressionAlgorithm"):
        raise ReportError(
            f"Report document {document_id} advertises compression "
            f"'{meta['compressionAlgorithm']}' that this module cannot decode."
        )

    return payload


def parse_report(payload: bytes) -> pd.DataFrame:
    """Parse a tab-separated report document into a DataFrame.

    An empty payload yields an empty DataFrame rather than ``EmptyDataError``.
    Reports are latin-1 in practice (product titles carry accented characters
    that are not valid UTF-8), so decoding falls back rather than failing.

    GZIP is sniffed here as well as in :func:`download_report_document` so the
    parser is correct on its own - for a cached document read back from disk,
    say. Decompressing is idempotent: the magic bytes are gone afterwards.
    """
    if payload[:2] == b"\x1f\x8b":
        payload = gzip.decompress(payload)

    if not payload.strip():
        return pd.DataFrame()

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        text = payload.decode("latin-1")

    return pd.read_csv(io.StringIO(text), sep="\t")


def fetch_report(
    tokens: LwaTokenProvider,
    report_type: str,
    marketplace_ids: Sequence[str],
    data_start_time: str,
    data_end_time: str,
    report_options: Mapping[str, str] | None = None,
    endpoint: str = NA_ENDPOINT,
    poll_seconds: int = 30,
    timeout_seconds: int = 1800,
) -> ReportResult:
    """Request a report, wait for it, and return it parsed.

    The end-to-end convenience wrapper. See :class:`ReportResult` for how an
    empty result is reported.
    """
    print(f"Requesting {report_type} for {data_start_time} .. {data_end_time}")
    report_id = request_report(
        tokens,
        report_type=report_type,
        marketplace_ids=marketplace_ids,
        data_start_time=data_start_time,
        data_end_time=data_end_time,
        report_options=report_options,
        endpoint=endpoint,
    )
    document_id = wait_for_report(
        tokens,
        report_id,
        endpoint=endpoint,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    payload = download_report_document(tokens, document_id, endpoint=endpoint)

    return ReportResult(
        frame=parse_report(payload),
        report_id=report_id,
        document_id=document_id,
        byte_count=len(payload),
        data_start_time=data_start_time,
        data_end_time=data_end_time,
        report_options=dict(report_options or {}),
    )
