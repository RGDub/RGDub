"""QuickBooks Online API client: OAuth refresh with token rotation, and paged queries.

Credentials live in Secret Manager as ``qbo-client-{id,secret,realmid,refreshtoken}-{env}``.

The refresh token is the fragile part. Intuit issues a new one on a refresh
roughly once a day, and the old one stops working soon after; a token nobody
uses for ~100 days expires outright (that is how the connection was found dead
on 2026-09-23). So the rotated token is written back to Secret Manager
*immediately*, before any data is requested, and a daily run keeps it alive.
"""

from __future__ import annotations

import base64
import logging
import time

import requests

from pipelines.lib.secrets import add_secret_version, get_secret

log = logging.getLogger(__name__)

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
BASE_URLS = {
    "production": "https://quickbooks.api.intuit.com",
    "sandbox": "https://sandbox-quickbooks.api.intuit.com",
}
MINOR_VERSION = "75"
PAGE_SIZE = 1000   # QBO's maximum MAXRESULTS


class QboAuthError(RuntimeError):
    """The refresh token was refused. Needs a human: reconnect via the Intuit OAuth Playground."""


class QboClient:
    def __init__(self, env: str = "production"):
        self.env = env
        self.base = BASE_URLS[env]
        self.realm_id = get_secret(f"qbo-client-realmid-{env}").strip()
        self._access_token = None
        self._token_expires = 0.0
        self.session = requests.Session()

    # -- auth ----------------------------------------------------------------
    def _refresh(self) -> None:
        env = self.env
        client_id = get_secret(f"qbo-client-id-{env}").strip()
        client_secret = get_secret(f"qbo-client-secret-{env}").strip()
        secret_id = f"qbo-client-refreshtoken-{env}"
        refresh_token = get_secret(secret_id).strip()

        r = requests.post(
            TOKEN_URL,
            headers={
                "Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode(),
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=60,
        )
        if r.status_code != 200:
            err = r.json().get("error", "") if "json" in r.headers.get("content-type", "") else ""
            raise QboAuthError(
                f"QuickBooks token refresh failed: HTTP {r.status_code} {err}. "
                + ("The refresh token has expired or been revoked; get a new one from the Intuit "
                   "OAuth 2.0 Playground and save it to Secret Manager as " + secret_id + "."
                   if err == "invalid_grant" else "")
            )
        tokens = r.json()
        new_refresh = tokens.get("refresh_token")
        if new_refresh and new_refresh != refresh_token:
            # Save before anything else: losing a rotated token breaks the connection.
            add_secret_version(secret_id, new_refresh)
            log.info("QuickBooks refresh token rotated; saved new version of %s", secret_id)
        self._access_token = tokens["access_token"]
        self._token_expires = time.time() + int(tokens.get("expires_in", 3600)) - 300
        log.info("QuickBooks access token refreshed; refresh token valid %d more days",
                 int(tokens.get("x_refresh_token_expires_in", 0)) // 86400)

    def _token(self) -> str:
        if not self._access_token or time.time() >= self._token_expires:
            self._refresh()
        return self._access_token

    # -- requests ------------------------------------------------------------
    def get(self, path: str, params: dict | None = None, max_retries: int = 6) -> dict:
        url = f"{self.base}/v3/company/{self.realm_id}/{path}"
        params = {"minorversion": MINOR_VERSION, **(params or {})}
        for attempt in range(max_retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=120, headers={
                    "Authorization": f"Bearer {self._token()}", "Accept": "application/json"})
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == max_retries:
                    raise
                log.warning("QuickBooks transport error (%s); retrying", exc)
                time.sleep(min(60, 5 * 2 ** attempt))
                continue
            if r.status_code == 401 and attempt == 0:
                self._access_token = None   # expired early; refresh once
                continue
            if r.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                wait = min(60, 5 * 2 ** attempt)
                log.warning("QuickBooks HTTP %s; retrying in %ss", r.status_code, wait)
                time.sleep(wait)
                continue
            if r.status_code != 200:
                raise RuntimeError(f"QuickBooks GET {path} -> HTTP {r.status_code}: {r.text[:500]}")
            return r.json()
        raise RuntimeError(f"QuickBooks GET {path}: retries exhausted")

    def query(self, sql: str) -> dict:
        return self.get("query", {"query": sql}).get("QueryResponse", {})

    def count(self, entity: str, where: str = "") -> int:
        return int(self.query(f"SELECT COUNT(*) FROM {entity} {where}".strip()).get("totalCount", 0))

    def query_all(self, entity: str, where: str = "") -> list[dict]:
        """Every record of ``entity``, paged, checked against COUNT(*) so a short read fails loudly."""
        expected = self.count(entity, where)
        rows: list[dict] = []
        start = 1
        while len(rows) < expected:
            page = self.query(
                f"SELECT * FROM {entity} {where} STARTPOSITION {start} MAXRESULTS {PAGE_SIZE}".replace("  ", " ")
            ).get(entity, [])
            if not page:
                break
            rows += page
            start += len(page)
        if len(rows) != expected:
            raise RuntimeError(f"QuickBooks {entity}: COUNT(*) said {expected} but paging returned {len(rows)}")
        return rows

    def company_info(self) -> dict:
        return self.get(f"companyinfo/{self.realm_id}")["CompanyInfo"]
