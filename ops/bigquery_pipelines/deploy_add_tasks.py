"""Add notebook tasks for new package modules to a BigQuery pipeline.

Each new task is a two-cell notebook identical in shape to the thin notebooks
deploy_thin.py made: install the package from GCS, call ``run()``. The task is
appended to definitions/actions.yaml with the given dependencies. Then run
release.py to move the release pin (and optionally start a run).

    python deploy_add_tasks.py --check
    python deploy_add_tasks.py
"""
import base64, json, subprocess, sys, time
import requests

B = "https://dataform.googleapis.com/v1beta1/projects/punlabs/locations/us-central1/repositories"
PACKAGES = "gs://punlabsamzraw/packages"
CHECK = "--check" in sys.argv
REPO = "eb6fd087-cb1a-4bef-84df-1f622a1c1843"   # PL-AMZSales-PunDataPipe-DailyActivity

# task name -> (module, dependency task names). Box-design data sources, 2026-09-24.
NEW_TASKS = {
    "PunData-BrandAnalyticsWeekly": ("pipelines.spapi.brand_analytics", []),
    "PunData-AdsSearchTermsPlacements": ("pipelines.ads_reports", ["PunData-SPAdDailyETL"]),   # share the Ads API quota politely
    "PunData-CatalogSnapshot": ("pipelines.spapi.catalog_snapshot", ["PunDataPipe-AMZTrans"]),
    "PunData-PricingSnapshot": ("pipelines.spapi.pricing_snapshot", ["PunData-CatalogSnapshot"]),
}

INSTALL = f"""# Shared pipeline package (source: github.com/RGDub/RGDub). Built with
# `python -m build --wheel`, copied to {PACKAGES}/, and named in
# {PACKAGES}/latest.txt. Downloaded with google.auth so this cell depends on
# nothing the runtime image could drop.
import subprocess, sys, urllib.parse
import google.auth
from google.auth.transport.requests import AuthorizedSession
_creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
_s = AuthorizedSession(_creds)
_bucket, _prefix = "{PACKAGES}"[5:].split("/", 1)
def _gcs(obj):
    r = _s.get(f"https://storage.googleapis.com/storage/v1/b/{{_bucket}}/o/{{urllib.parse.quote(obj, safe='')}}?alt=media", timeout=60)
    r.raise_for_status()
    return r.content
_wheel = _gcs(f"{{_prefix}}/latest.txt").decode().strip()
open(f"/tmp/{{_wheel}}", "wb").write(_gcs(f"{{_prefix}}/{{_wheel}}"))
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--upgrade", f"/tmp/{{_wheel}}"], check=True)
import importlib.metadata as _m
print("punlabs-pipelines", _m.version("punlabs-pipelines"), "from", _wheel)
"""
RUN = """import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
from {module} import run
result = run()
print(f"{module}: {{result}}")
"""


def cell(src):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": src.splitlines(keepends=True)}


def notebook(module):
    return json.dumps({"cells": [cell(INSTALL), cell(RUN.format(module=module))],
                       "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
                       "nbformat": 4, "nbformat_minor": 5}, indent=1)


token = subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()
H = {"Authorization": f"Bearer {token}"}


def get(url):
    r = requests.get(url, headers=H, timeout=60); r.raise_for_status(); return r.json()


def post(url, body):
    r = requests.post(url, headers=H, json=body, timeout=120)
    if r.status_code >= 300:
        raise SystemExit(f"POST {url.split('/repositories/')[1]} -> {r.status_code} {r.text[:400]}")
    return r.json() if r.text else {}


head = get(f"{B}/{REPO}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
actions = base64.b64decode(get(f"{B}/{REPO}:readFile?commitSha={head}&path=definitions/actions.yaml")["contents"]).decode()
ops = {}
for name, (module, deps) in NEW_TASKS.items():
    if f"name: {name}\n" in actions:
        print(f"  {name}: already in actions.yaml"); continue
    fname = f"notebook_{name.lower().replace('-', '_')}.ipynb"
    ops[f"definitions/{fname}"] = {"writeFile": {"contents": base64.b64encode(notebook(module).encode()).decode()}}
    entry = f"- notebook:\n    name: {name}\n    project: punlabs\n"
    if deps:
        entry += "    dependencyTargets:\n" + "".join(f"    - name: {d}\n" for d in deps)
    entry += f"    filename: {fname}\n"
    actions += entry
    print(f"  {name}: {fname} -> {module}  deps={deps}")
if ops:
    ops["definitions/actions.yaml"] = {"writeFile": {"contents": base64.b64encode(actions.encode()).decode()}}
if CHECK or not ops:
    print(f"  --check: would commit {len(ops)} files on top of {head[:8]}" if ops else "  nothing to do"); sys.exit()
post(f"{B}/{REPO}:commit", {
    "commitMetadata": {"author": {"name": "Grant Weatherford", "emailAddress": "grant.weatherford@punlabs.io"},
                       "commitMessage": "Add Brand Analytics, Ads search-term/placement, catalog and pricing snapshot tasks"},
    "requiredHeadCommitSha": head, "fileOperations": ops})
print("  committed ->", get(f"{B}/{REPO}:fetchHistory?pageSize=1")["commits"][0]["commitSha"][:8])
print("  now run release.py to move the release pin to this commit")
