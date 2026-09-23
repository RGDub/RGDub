"""Secret Manager access that survives Colab Enterprise runtime image changes.

Why this module exists
----------------------
The SP-API extraction notebooks are scheduled through BigQuery Data Pipelines,
which execute the ``.ipynb`` on a Colab Enterprise runtime. That runtime's base
image used to ship ``google-cloud-secret-manager`` preinstalled. It no longer
does, so::

    from google.cloud import secretmanager

raises ``ModuleNotFoundError`` on the first cell. The scheduled run dies before
it issues a single BigQuery job, which is why the failure was invisible from
BigQuery's side: the ``amzsales@`` service account simply stopped appearing in
``INFORMATION_SCHEMA.JOBS`` with no error rows to alert on.

This module talks to the Secret Manager REST API directly using ``google.auth``,
which *is* guaranteed to be present on the runtime (``google-cloud-bigquery``
depends on it). It therefore has no dependency that a base-image refresh can
take away.

Usage
-----
    from pipelines.lib.secrets import get_secret, preflight

    preflight(["sp-api-refresh-token", "sp-api-client-secret"])
    refresh_token = get_secret("sp-api-refresh-token")
"""

from __future__ import annotations

import base64
import os
from functools import lru_cache
from typing import Iterable

DEFAULT_PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get(
    "GOOGLE_CLOUD_PROJECT", "punlabs"
)

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_API_ROOT = "https://secretmanager.googleapis.com/v1"


class SecretAccessError(RuntimeError):
    """Raised when a secret cannot be read. Always fatal - never swallow it."""


def _authorized_session():
    """Build an AuthorizedSession from the runtime's ambient credentials.

    On a Colab Enterprise runtime this resolves to the runtime template's
    service account (``amzsales@punlabs.iam.gserviceaccount.com``).
    """
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=[_SCOPE])
    return AuthorizedSession(credentials)


@lru_cache(maxsize=32)
def get_secret(
    secret_id: str,
    project_id: str = DEFAULT_PROJECT,
    version: str = "latest",
) -> str:
    """Return the payload of ``secret_id`` as a string.

    Results are cached for the life of the process so a notebook that reads the
    same credential in several cells only pays for one API round trip.
    """
    session = _authorized_session()
    url = f"{_API_ROOT}/projects/{project_id}/secrets/{secret_id}/versions/{version}:access"

    response = session.get(url, timeout=30)
    if response.status_code != 200:
        raise SecretAccessError(
            f"Could not read secret '{secret_id}' (project '{project_id}', "
            f"version '{version}'): HTTP {response.status_code} {response.text}. "
            "Check that the runtime service account holds "
            "roles/secretmanager.secretAccessor on this secret."
        )

    try:
        encoded = response.json()["payload"]["data"]
    except (KeyError, ValueError) as exc:
        raise SecretAccessError(
            f"Malformed Secret Manager response for '{secret_id}': {exc}"
        ) from exc

    return base64.b64decode(encoded).decode("utf-8")


def preflight(secret_ids: Iterable[str], project_id: str = DEFAULT_PROJECT) -> None:
    """Fail loudly, at the top of a notebook, if credentials are unreachable.

    Put this in the first cell. A scheduled run that cannot read its secrets
    should abort immediately with a clear message rather than proceed and write
    a partial or empty load, which is far harder to detect downstream.
    """
    missing: list[str] = []
    for secret_id in secret_ids:
        try:
            if not get_secret(secret_id, project_id=project_id):
                missing.append(f"{secret_id} (empty payload)")
        except SecretAccessError as exc:
            missing.append(f"{secret_id} ({exc})")

    if missing:
        raise SecretAccessError(
            "Preflight failed - the pipeline cannot authenticate:\n  - "
            + "\n  - ".join(missing)
        )


def add_secret_version(secret_id: str, value: str, project_id: str = DEFAULT_PROJECT) -> None:
    """Store ``value`` as the new latest version of ``secret_id``.

    Used for credentials the provider rotates on use (the QuickBooks refresh
    token). The caller needs roles/secretmanager.secretVersionAdder on the
    secret. Clears the read cache so the next ``get_secret`` sees the new value.
    """
    session = _authorized_session()
    url = f"{_API_ROOT}/projects/{project_id}/secrets/{secret_id}:addVersion"
    body = {"payload": {"data": base64.b64encode(value.encode("utf-8")).decode("ascii")}}
    response = session.post(url, json=body, timeout=30)
    if response.status_code != 200:
        raise SecretAccessError(
            f"Could not add a version to secret '{secret_id}': HTTP {response.status_code} "
            f"{response.text[:300]}. The runtime service account needs "
            "roles/secretmanager.secretVersionAdder on this secret."
        )
    get_secret.cache_clear()
