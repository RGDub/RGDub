"""Minimal Amazon Ads API client: LWA auth, v3 async reports, Marketing Stream subscriptions.

Deliberately dependency-light (``requests`` only) so it runs unchanged on a
Colab Enterprise runtime, Cloud Run, or a laptop.

Usage
-----
    from pipelines.lib.ads_api import AdsApiClient, ads_client_from_secrets

    client = ads_client_from_secrets()            # reads Secret Manager
    report_id = client.create_report(body)
    url = client.wait_for_report(report_id)
    rows = client.download_report(url)

Endpoints and headers follow the Ads API v3 reporting guide and the Marketing
Stream onboarding guide. If Amazon changes a media type, the constants at the
top are the only thing to touch.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable

import requests

log = logging.getLogger(__name__)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

API_ENDPOINTS = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}

REPORT_V3_MEDIA_TYPE = "application/vnd.createasyncreportrequest.v3+json"
STREAM_SUB_MEDIA_TYPE = (
    "application/vnd.MarketingStreamSubscriptions.StreamSubscriptionResource.v1.0+json"
)

# Secret Manager ids. Create them once with:
#   gcloud secrets create amz-ads-client-id --data-file=- <<< "$CLIENT_ID"
# and grant amzsales@punlabs.iam.gserviceaccount.com secretAccessor on each.
SECRET_IDS = {
    "client_id": "amz-ads-client-id",
    "client_secret": "amz-ads-client-secret",
    "refresh_token": "amz-ads-refresh-token",
    "profile_id": "amz-ads-profile-id",
}


class AdsApiError(RuntimeError):
    """Non-retryable error from the Ads API. Carries the HTTP status and body."""

    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


class ReportFailed(AdsApiError):
    """Amazon marked the report FAILED, or it never completed in time."""


@dataclass
class AdsApiClient:
    client_id: str
    client_secret: str
    refresh_token: str
    profile_id: str | None = None
    region: str = "NA"
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 6
    _access_token: str | None = field(default=None, repr=False)
    _token_expires_at: float = field(default=0.0, repr=False)

    # ------------------------------------------------------------------ auth
    @property
    def base_url(self) -> str:
        try:
            return API_ENDPOINTS[self.region]
        except KeyError as exc:
            raise ValueError(f"Unknown Ads API region {self.region!r}; use NA, EU or FE") from exc

    def access_token(self) -> str:
        """Return a valid LWA access token, refreshing when within 60 s of expiry."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token
        resp = self.session.post(
            LWA_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise AdsApiError(
                f"LWA token refresh failed: HTTP {resp.status_code} {resp.text[:300]}. "
                "If this is invalid_grant the refresh token has been revoked or expired "
                "and must be re-issued through the LWA consent flow.",
                resp.status_code,
                resp.text,
            )
        payload = resp.json()
        self._access_token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._access_token

    def _headers(self, content_type: str | None = None, scoped: bool = True) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
        }
        if scoped:
            if not self.profile_id:
                raise AdsApiError("profile_id is required for this call")
            headers["Amazon-Advertising-API-Scope"] = str(self.profile_id)
        if content_type:
            headers["Content-Type"] = content_type
            headers["Accept"] = content_type
        return headers

    # -------------------------------------------------------------- transport
    def request(
        self,
        method: str,
        path: str,
        *,
        content_type: str | None = None,
        scoped: bool = True,
        json_body: Any | None = None,
        params: dict | None = None,
        timeout: int = 60,
    ) -> requests.Response:
        """HTTP call with retry on 429 / 5xx using Retry-After or exponential backoff."""
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            resp = self.session.request(
                method,
                url,
                headers=self._headers(content_type, scoped),
                json=json_body,
                params=params,
                timeout=timeout,
            )
            if resp.status_code < 400:
                return resp
            retryable = resp.status_code == 429 or resp.status_code >= 500
            if not retryable or attempt == self.max_retries:
                raise AdsApiError(
                    f"{method} {url} -> HTTP {resp.status_code}: {resp.text[:500]}",
                    resp.status_code,
                    resp.text,
                )
            wait = float(resp.headers.get("Retry-After") or delay)
            log.warning("Ads API %s %s returned %s; retrying in %.0fs (attempt %d/%d)",
                        method, path, resp.status_code, wait, attempt, self.max_retries)
            time.sleep(wait)
            delay = min(delay * 2, 120)
        raise AssertionError("unreachable")

    # ---------------------------------------------------------------- profiles
    def list_profiles(self) -> list[dict]:
        """Profiles visible to this LWA user. Use it once to find the profile_id."""
        return self.request("GET", "/v2/profiles", scoped=False).json()

    # ---------------------------------------------------------------- reports
    def create_report(self, body: dict) -> str:
        resp = self.request("POST", "/reporting/reports", content_type=REPORT_V3_MEDIA_TYPE, json_body=body)
        return resp.json()["reportId"]

    def get_report(self, report_id: str) -> dict:
        return self.request(
            "GET", f"/reporting/reports/{report_id}", content_type=REPORT_V3_MEDIA_TYPE
        ).json()

    def wait_for_report(self, report_id: str, timeout_s: int = 1800, first_wait_s: int = 30) -> str:
        """Poll until COMPLETED and return the download URL.

        Amazon typically needs 1–10 minutes; polling faster than every 30 s
        only burns rate limit. Backs off from ``first_wait_s`` up to 5 minutes.
        """
        deadline = time.time() + timeout_s
        wait = first_wait_s
        while True:
            status = self.get_report(report_id)
            state = status.get("status")
            if state == "COMPLETED":
                return status["url"]
            if state == "FAILED":
                raise ReportFailed(f"Report {report_id} failed: {status.get('failureReason')}")
            if time.time() + wait > deadline:
                raise ReportFailed(f"Report {report_id} still {state} after {timeout_s}s")
            log.info("report %s is %s; next poll in %ds", report_id, state, wait)
            time.sleep(wait)
            wait = min(wait * 2, 300)

    def download_report(self, url: str) -> list[dict]:
        """Download a GZIP_JSON report and return its rows."""
        resp = self.session.get(url, timeout=300)
        resp.raise_for_status()
        with gzip.open(io.BytesIO(resp.content)) as fh:
            return json.load(fh)

    def run_report(self, body: dict, timeout_s: int = 1800) -> list[dict]:
        """Create, wait for, and download a report in one call."""
        report_id = self.create_report(body)
        log.info("created report %s (%s %s..%s)", report_id,
                 body.get("configuration", {}).get("reportTypeId"), body.get("startDate"), body.get("endDate"))
        return self.download_report(self.wait_for_report(report_id, timeout_s=timeout_s))

    # ------------------------------------------- Ads API v1 campaign management
    def query_entities(self, kind: str, ad_product: str = "SPONSORED_PRODUCTS",
                       page_size: int = 1000, **filters) -> Iterable[dict]:
        """Page through ``POST /adsApi/v1/query/{kind}`` (campaigns | adGroups | ads | targets).

        Yields entities in the v1 common model (camelCase). ``filters`` are passed
        through as extra body fields, e.g. ``stateFilter={"include": ["ENABLED"]}``.
        """
        if kind not in ("campaigns", "adGroups", "ads", "targets"):
            raise ValueError(f"unknown entity kind {kind!r}")
        body: dict[str, Any] = {"adProductFilter": {"include": [ad_product]}, "maxResults": page_size, **filters}
        while True:
            data = self.request("POST", f"/adsApi/v1/query/{kind}", content_type="application/json",
                                json_body=body).json()
            yield from data.get(kind, [])
            token = data.get("nextToken")
            if not token:
                return
            body["nextToken"] = token

    # ------------------------------------------------- Marketing Stream subs
    def create_stream_subscription(
        self,
        dataset_id: str,
        destination_arn: str,
        notes: str | None = None,
        client_request_token: str | None = None,
    ) -> dict:
        """Subscribe this profile's ``dataset_id`` (e.g. ``sp-traffic``) to an SQS queue ARN.

        After this call Amazon sends an SNS ``SubscriptionConfirmation`` message
        to the queue; ``pipelines/ads_stream/poller.py`` confirms it.
        """
        body = {
            "clientRequestToken": client_request_token or uuid.uuid4().hex,
            "dataSetId": dataset_id,
            "destinationArn": destination_arn,
        }
        if notes:
            body["notes"] = notes
        return self.request(
            "POST", "/streams/subscriptions", content_type=STREAM_SUB_MEDIA_TYPE, json_body=body
        ).json()

    def list_stream_subscriptions(self) -> list[dict]:
        resp = self.request("GET", "/streams/subscriptions", content_type=STREAM_SUB_MEDIA_TYPE)
        data = resp.json()
        return data.get("subscriptions", data) if isinstance(data, dict) else data

    def archive_stream_subscription(self, subscription_id: str) -> dict:
        return self.request(
            "PUT",
            f"/streams/subscriptions/{subscription_id}",
            content_type=STREAM_SUB_MEDIA_TYPE,
            json_body={"status": "ARCHIVED"},
        ).json()


def ads_client_from_secrets(region: str = "NA", secret_ids: dict[str, str] | None = None) -> AdsApiClient:
    """Build a client from Secret Manager using ``pipelines.lib.secrets``."""
    from pipelines.lib.secrets import get_secret, preflight

    ids = {**SECRET_IDS, **(secret_ids or {})}
    preflight(ids.values())
    return AdsApiClient(
        client_id=get_secret(ids["client_id"]).strip(),
        client_secret=get_secret(ids["client_secret"]).strip(),
        refresh_token=get_secret(ids["refresh_token"]).strip(),
        profile_id=get_secret(ids["profile_id"]).strip(),
        region=region,
    )


def chunk_date_range(start, end, max_days: int = 31) -> Iterable[tuple]:
    """Yield (start, end) date pairs no longer than ``max_days`` (the v3 report limit)."""
    import datetime as _dt

    cursor = start
    while cursor <= end:
        stop = min(cursor + _dt.timedelta(days=max_days - 1), end)
        yield cursor, stop
        cursor = stop + _dt.timedelta(days=1)
