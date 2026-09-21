# --- Secret Manager shim (added 2026-09-21) ----------------------------------
# The Colab Enterprise runtime image no longer ships google-cloud-secret-manager,
# which killed every scheduled run of this notebook from 2026-08-25. This cell
# provides the one class the notebook uses from that package
# (SecretManagerServiceClient.access_secret_version) over the Secret Manager
# REST API via google.auth, which google-cloud-bigquery guarantees is present.
# Remove it once the notebook calls pipelines.lib.secrets from the shared package.
import base64 as _b64, sys as _sys, types as _types
import google.auth as _gauth
from google.auth.transport.requests import AuthorizedSession as _AuthorizedSession


class _SecretPayload:
    def __init__(self, data):
        self.data = data


class _SecretResponse:
    def __init__(self, data):
        self.payload = _SecretPayload(data)


class SecretManagerServiceClient:
    def __init__(self, credentials=None, **_ignored):
        if credentials is None:
            credentials, _ = _gauth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self._session = _AuthorizedSession(credentials)

    def access_secret_version(self, request=None, name=None, **_ignored):
        name = name or (request or {})["name"]
        resp = self._session.get(f"https://secretmanager.googleapis.com/v1/{name}:access", timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Secret Manager {name}: HTTP {resp.status_code} {resp.text[:300]}")
        return _SecretResponse(_b64.b64decode(resp.json()["payload"]["data"]))


_shim = _types.ModuleType("google.cloud.secretmanager")
_shim.SecretManagerServiceClient = SecretManagerServiceClient
import google.cloud as _gcloud
_gcloud.secretmanager = _shim
_sys.modules["google.cloud.secretmanager"] = _shim
print("secret-manager shim active")
