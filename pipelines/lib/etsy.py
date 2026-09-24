"""Etsy Open API v3 client for the Pop Colors shop.

Two credentials on every call: the app's API key (``x-api-key: keystring:shared_secret``)
and, for shop data, an OAuth 2.0 bearer token. Access tokens last an hour; the
refresh token lasts 90 days and Etsy issues a new one on every refresh, so the
client saves the rotated token to Secret Manager the moment it arrives (same
pattern as QuickBooks). A refresh that fails with invalid_grant means a human
has to re-authorize once with ops/etsy_authorize.py.

Secret Manager ids:
    etsy-api-keystring, etsy-api-shared-secret   from the Etsy developer portal
    etsy-oauth-refresh-token                     written by ops/etsy_authorize.py, rotated here
    etsy-shop-id                                 numeric shop id (the helper stores it too)

    from pipelines.lib.etsy import etsy_client_from_secrets
    etsy = etsy_client_from_secrets()
    for receipt in etsy.receipts(min_last_modified=1_700_000_000):
        ...

Rate limits are per API key (10 qps / 10,000 per rolling day by default); 429s
carry retry-after and are retried with backoff, as are 5xx and dropped
connections. Timestamps in and out are unix seconds (UTC).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.etsy.com/v3/application"
TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token"
AUTHORIZE_URL = "https://www.etsy.com/oauth/connect"
SCOPES = "transactions_r shops_r listings_r"
PAGE_LIMIT = 100                      # Etsy's maximum per page

SECRET_IDS = {
    "keystring": "etsy-api-keystring",
    "shared_secret": "etsy-api-shared-secret",
    "refresh_token": "etsy-oauth-refresh-token",
    "shop_id": "etsy-shop-id",
}


class EtsyApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


class EtsyAuthError(EtsyApiError):
    """The refresh token was refused. Needs a human: run ops/etsy_authorize.py again."""


@dataclass
class EtsyClient:
    keystring: str
    shared_secret: str
    refresh_token: str | None = None
    shop_id: int | None = None
    refresh_token_secret_id: str | None = None   # when set, rotated tokens are saved here
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 8
    _access_token: str | None = field(default=None, repr=False)
    _token_expires_at: float = field(default=0.0, repr=False)

    # ------------------------------------------------------------------ auth
    @property
    def api_key_header(self) -> str:
        return f"{self.keystring}:{self.shared_secret}"

    def _refresh(self) -> None:
        if not self.refresh_token:
            raise EtsyAuthError("no refresh token; run ops/etsy_authorize.py to authorize the shop")
        r = self.session.post(TOKEN_URL, data={
            "grant_type": "refresh_token", "client_id": self.keystring, "refresh_token": self.refresh_token,
        }, timeout=60)
        if r.status_code != 200:
            err = ""
            try:
                err = r.json().get("error", "")
            except ValueError:
                pass
            raise EtsyAuthError(
                f"Etsy token refresh failed: HTTP {r.status_code} {r.text[:300]}"
                + (" The refresh token has expired or been revoked; re-authorize with ops/etsy_authorize.py."
                   if err == "invalid_grant" else ""), r.status_code, r.text)
        tokens = r.json()
        new_refresh = tokens.get("refresh_token")
        if new_refresh and new_refresh != self.refresh_token:
            # Save before anything else can fail: a lost rotated token breaks the connection.
            if self.refresh_token_secret_id:
                from pipelines.lib.secrets import add_secret_version
                add_secret_version(self.refresh_token_secret_id, new_refresh)
                log.info("Etsy refresh token rotated; saved new version of %s", self.refresh_token_secret_id)
            self.refresh_token = new_refresh
        self._access_token = tokens["access_token"]
        self._token_expires_at = time.time() + int(tokens.get("expires_in", 3600))

    def access_token(self) -> str:
        if not self._access_token or time.time() > self._token_expires_at - 120:
            self._refresh()
        return self._access_token

    # ------------------------------------------------------------- transport
    def request(self, method: str, path: str, *, params: dict | None = None, json_body: Any | None = None,
                oauth: bool = True, timeout: int = 60) -> requests.Response:
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
        delay = 2.0
        refreshed_once = False
        for attempt in range(1, self.max_retries + 1):
            headers = {"x-api-key": self.api_key_header, "Accept": "application/json"}
            if oauth:
                headers["Authorization"] = f"Bearer {self.access_token()}"
            try:
                resp = self.session.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt == self.max_retries:
                    raise
                log.warning("Etsy %s %s: %s; retrying in %.0fs (%d/%d)", method, path, exc, delay, attempt, self.max_retries)
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue
            if resp.status_code < 400:
                return resp
            if resp.status_code == 401 and oauth and not refreshed_once:
                refreshed_once = True
                self._access_token = None      # expired early; refresh once and retry
                continue
            retryable = resp.status_code == 429 or resp.status_code >= 500
            if not retryable or attempt == self.max_retries:
                raise EtsyApiError(f"{method} {path} -> HTTP {resp.status_code}: {resp.text[:500]}",
                                   resp.status_code, resp.text)
            wait = float(resp.headers.get("retry-after") or max(delay, 5.0 if resp.status_code == 429 else 0))
            log.warning("Etsy %s %s returned %s; retrying in %.0fs (%d/%d)", method, path, resp.status_code,
                        wait, attempt, self.max_retries)
            time.sleep(wait)
            delay = min(delay * 2, 120)
        raise AssertionError("unreachable")

    def _paginate(self, path: str, params: dict | None = None, oauth: bool = True) -> Iterator[dict]:
        """Follow limit/offset until a page comes back short. Yields ``results`` items."""
        params = {"limit": PAGE_LIMIT, **(params or {})}
        offset = 0
        while True:
            data = self.request("GET", path, params={**params, "offset": offset}, oauth=oauth).json()
            items = data.get("results", [])
            yield from items
            if len(items) < PAGE_LIMIT:
                return
            offset += PAGE_LIMIT

    # ------------------------------------------------------------ resources
    def me(self) -> dict:
        """{user_id, shop_id} for the authorizing user."""
        return self.request("GET", "/users/me").json()

    def shop(self, shop_id: int | None = None) -> dict:
        return self.request("GET", f"/shops/{shop_id or self.shop_id}").json()

    def receipts(self, *, min_last_modified: int | None = None, min_created: int | None = None,
                 max_created: int | None = None) -> Iterator[dict]:
        """Receipts (orders) with their transactions, shipments and refunds embedded.

        Ascending by last update, so a run cut short can resume from its watermark.
        """
        params: dict[str, Any] = {"sort_on": "updated", "sort_order": "asc"}
        if min_last_modified is not None:
            params["min_last_modified"] = int(min_last_modified)
        if min_created is not None:
            params["min_created"] = int(min_created)
        if max_created is not None:
            params["max_created"] = int(max_created)
        yield from self._paginate(f"/shops/{self.shop_id}/receipts", params)

    def ledger_entries(self, min_created: int, max_created: int) -> Iterator[dict]:
        """Payment account ledger: sales, fees, refunds, deposits (payouts)."""
        yield from self._paginate(f"/shops/{self.shop_id}/payment-account/ledger-entries",
                                  {"min_created": int(min_created), "max_created": int(max_created)})

    def listings(self, state: str = "active", includes: tuple[str, ...] = ("Inventory",)) -> Iterator[dict]:
        """Listings in one state (active, inactive, sold_out, draft, expired), with inventory (SKUs, prices)."""
        params = {"state": state, "includes": ",".join(includes)} if includes else {"state": state}
        yield from self._paginate(f"/shops/{self.shop_id}/listings", params)


def etsy_client_from_secrets(secret_ids: dict[str, str] | None = None) -> EtsyClient:
    from pipelines.lib.secrets import get_secret, preflight

    ids = {**SECRET_IDS, **(secret_ids or {})}
    preflight(ids.values())
    return EtsyClient(
        keystring=get_secret(ids["keystring"]).strip(),
        shared_secret=get_secret(ids["shared_secret"]).strip(),
        refresh_token=get_secret(ids["refresh_token"]).strip(),
        shop_id=int(get_secret(ids["shop_id"]).strip()),
        refresh_token_secret_id=ids["refresh_token"],
    )
