"""One-time Etsy OAuth authorization for the Pop Colors shop. Run it on a Mac
with a browser; it never prints a secret.

    .venv/bin/python ops/etsy_authorize.py

Prerequisites (once, in the Etsy developer portal at etsy.com/developers/your-apps):
  * an app whose callback URL is exactly  http://localhost:3003/oauth/redirect
  * its keystring and shared secret stored in Secret Manager:
        pbpaste | tr -d '[:space:]' | gcloud secrets create etsy-api-keystring     --project punlabs --data-file=- --replication-policy=automatic
        pbpaste | tr -d '[:space:]' | gcloud secrets create etsy-api-shared-secret --project punlabs --data-file=- --replication-policy=automatic

What it does: builds a PKCE authorization URL, opens it in your browser, receives
Etsy's redirect on localhost:3003, exchanges the code for tokens, stores the
refresh token as etsy-oauth-refresh-token and the shop id as etsy-shop-id
(creating the secrets if needed), then prints the shop name to confirm.
Re-run it whenever the loader reports invalid_grant (the refresh token is good
for 90 days and rotates on every daily run, so that should be rare).
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import secrets as pysecrets
import subprocess
import sys
import threading
import urllib.parse
import webbrowser

import requests

sys.path.insert(0, ".")
from pipelines.lib.etsy import AUTHORIZE_URL, SCOPES, TOKEN_URL, EtsyClient, SECRET_IDS  # noqa: E402
from pipelines.lib.secrets import get_secret  # noqa: E402

PROJECT = "punlabs"
REDIRECT_URI = "http://localhost:3003/oauth/redirect"


def store_secret(secret_id: str, value: str) -> None:
    """Create-or-add-version through gcloud so the caller's own credentials are used."""
    exists = subprocess.run(["gcloud", "secrets", "describe", secret_id, "--project", PROJECT],
                            capture_output=True).returncode == 0
    cmd = (["gcloud", "secrets", "versions", "add", secret_id, "--project", PROJECT, "--data-file=-"] if exists else
           ["gcloud", "secrets", "create", secret_id, "--project", PROJECT, "--data-file=-", "--replication-policy=automatic"])
    subprocess.run(cmd, input=value.encode(), check=True, capture_output=True)
    print(f"  stored {secret_id} ({'new version' if exists else 'created'})")


def main() -> None:
    keystring = get_secret(SECRET_IDS["keystring"]).strip()
    shared_secret = get_secret(SECRET_IDS["shared_secret"]).strip()

    verifier = base64.urlsafe_b64encode(pysecrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    state = pysecrets.token_urlsafe(24)
    url = AUTHORIZE_URL + "?" + urllib.parse.urlencode({
        "response_type": "code", "client_id": keystring, "redirect_uri": REDIRECT_URI, "scope": SCOPES,
        "state": state, "code_challenge": challenge, "code_challenge_method": "S256",
    })

    result: dict = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            result.update({k: v[0] for k, v in q.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Etsy authorization received. You can close this tab.</h2>")

        def log_message(self, *a):  # keep the terminal quiet
            pass

    server = http.server.HTTPServer(("localhost", 3003), Handler)
    threading.Thread(target=server.handle_request, daemon=True).start()

    print("Opening Etsy's authorization page. Sign in as the shop owner and click Allow.")
    print("If the browser does not open, paste this URL into it:\n  " + url)
    webbrowser.open(url)
    while "code" not in result and "error" not in result:
        threading.Event().wait(0.5)
    server.server_close()

    if "error" in result:
        raise SystemExit(f"Etsy refused: {result.get('error')}: {result.get('error_description', '')}")
    if result.get("state") != state:
        raise SystemExit("state mismatch; refusing the code (possible CSRF). Run again.")

    r = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code", "client_id": keystring, "redirect_uri": REDIRECT_URI,
        "code": result["code"], "code_verifier": verifier,
    }, timeout=60)
    if r.status_code != 200:
        raise SystemExit(f"token exchange failed: HTTP {r.status_code} {r.text[:300]}")
    tokens = r.json()
    print(f"  tokens granted with scope: {tokens.get('scope')}")

    client = EtsyClient(keystring, shared_secret)
    client._access_token = tokens["access_token"]
    client._token_expires_at = 9e12
    me = client.me()
    shop = client.shop(me["shop_id"])
    print(f"  authorized as user {me['user_id']} for shop {shop.get('shop_name')} (id {me['shop_id']})")

    store_secret(SECRET_IDS["refresh_token"], tokens["refresh_token"])
    store_secret(SECRET_IDS["shop_id"], str(me["shop_id"]))
    print("done. Next: bash ops/cloud_run/setup_etsy.sh")


if __name__ == "__main__":
    main()
