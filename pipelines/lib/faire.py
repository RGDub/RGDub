"""Minimal Faire External API v2 client (brand side).

Auth is two headers on every call: the app credentials (base64 of
``applicationId:applicationSecret``) and a brand access token generated in the
Faire Brand Portal (Settings > Integrations > "Have an unpublished
integration?"). Both live in Secret Manager.

    from pipelines.lib.faire import faire_client_from_secrets
    faire = faire_client_from_secrets()
    for order in faire.orders(updated_at_min=some_iso_timestamp):
        ...

Lists are cursor paginated; the client follows ``cursor`` until the page comes
back short. 429s and 5xx are retried with backoff, as are dropped connections.
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://www.faire.com/external-api/v2"
SECRET_IDS = {
    "app_id": "FAIRE-API-APP-ID",
    "app_secret": "FAIRE-API-SECRET-ID",
    "access_token": "FAIRE-API-ACCESS-TOKEN",
}
PAGE_LIMIT = 50


class FaireApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass
class FaireClient:
    app_id: str
    app_secret: str
    access_token: str
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 8

    @property
    def headers(self) -> dict[str, str]:
        creds = base64.b64encode(f"{self.app_id}:{self.app_secret}".encode()).decode()
        return {"X-FAIRE-APP-CREDENTIALS": creds, "X-FAIRE-OAUTH-ACCESS-TOKEN": self.access_token,
                "Accept": "application/json"}

    def request(self, method: str, path: str, *, params: dict | None = None, json_body: Any | None = None,
                timeout: int = 60) -> requests.Response:
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(method, url, headers=self.headers, params=params, json=json_body, timeout=timeout)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt == self.max_retries:
                    raise
                log.warning("Faire %s %s: %s; retrying in %.0fs (%d/%d)", method, path, exc, delay, attempt, self.max_retries)
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue
            if resp.status_code < 400:
                return resp
            retryable = resp.status_code == 429 or resp.status_code >= 500
            if not retryable or attempt == self.max_retries:
                raise FaireApiError(f"{method} {path} -> HTTP {resp.status_code}: {resp.text[:500]}",
                                    resp.status_code, resp.text)
            wait = float(resp.headers.get("Retry-After") or max(delay, 10.0 if resp.status_code == 429 else 0))
            log.warning("Faire %s %s returned %s; retrying in %.0fs (%d/%d)", method, path, resp.status_code, wait,
                        attempt, self.max_retries)
            time.sleep(wait)
            delay = min(delay * 2, 120)
        raise AssertionError("unreachable")

    def _paginate(self, path: str, key: str, params: dict | None = None) -> Iterator[dict]:
        params = {"limit": PAGE_LIMIT, **(params or {})}
        cursor = None
        while True:
            page_params = {**params, "cursor": cursor} if cursor else params
            data = self.request("GET", path, params=page_params).json()
            items = data.get(key, [])
            yield from items
            cursor = data.get("cursor")
            if not cursor or len(items) < params["limit"]:
                return

    # ------------------------------------------------------------ resources
    def brand(self) -> dict:
        return self.request("GET", "/brand").json()

    def orders(self, updated_at_min: str | None = None, created_at_min: str | None = None,
               excluded_states: str | None = None) -> Iterator[dict]:
        """All orders, ascending by updated_at. ISO 8601 timestamps for the filters."""
        params: dict[str, Any] = {}
        if updated_at_min:
            params["updated_at_min"] = updated_at_min
        if created_at_min:
            params["created_at_min"] = created_at_min
        if excluded_states:
            params["excluded_states"] = excluded_states
        yield from self._paginate("/orders", "orders", params)

    def products(self) -> Iterator[dict]:
        yield from self._paginate("/products", "products")

    def retailer(self, retailer_id: str) -> dict:
        return self.request("GET", f"/retailers/{retailer_id}").json()


def faire_client_from_secrets(secret_ids: dict[str, str] | None = None) -> FaireClient:
    from pipelines.lib.secrets import get_secret, preflight

    ids = {**SECRET_IDS, **(secret_ids or {})}
    preflight(ids.values())
    return FaireClient(get_secret(ids["app_id"]).strip(), get_secret(ids["app_secret"]).strip(),
                       get_secret(ids["access_token"]).strip())
