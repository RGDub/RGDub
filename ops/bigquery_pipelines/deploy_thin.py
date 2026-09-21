"""Step 2 deploy: replace each scheduled notebook's body with two cells that
install the shared package from GCS and call its extractor.

Usage:  .venv/bin/python deploy_thin.py --check   # show the mapping, change nothing
        .venv/bin/python deploy_thin.py           # commit + compile a new release
"""
import base64, json, os, subprocess, sys, time
import requests

B = "https://dataform.googleapis.com/v1beta1/projects/punlabs/locations/us-central1/repositories"
PACKAGES = "gs://punlabsamzraw/packages"   # holds versioned wheels + latest.txt naming the current one
CHECK = "--check" in sys.argv

# repo -> {notebook file: (pipeline module, heartbeat name)}
PLAN = {
    "eb6fd087-cb1a-4bef-84df-1f622a1c1843": {   # PL-AMZSales-PunDataPipe-DailyActivity
        "notebook_1773151299272.ipynb": "pipelines.spapi.orders",        # PunDataPipe-AMZTrans
        "notebook_1773335195567.ipynb": "pipelines.spapi.traffic",       # PundData-DailyTraffic
        "notebook_1777759202315.ipynb": "pipelines.spapi.settlements",   # PunData-DailyAMZSettlementCheck
        "notebook_1777765331709.ipynb": "pipelines.spapi.finances",      # PunData-DailyAMZPendingFinance
        "notebook_1773260797098.ipynb": "pipelines.sp_ads_daily",        # PunData-SPAdDailyETL
        "notebook_1777782445290.ipynb": "pipelines.ads_invoices",        # PunData-DailyAMZAdINVCheck
    },
    "2ceefade-8ee6-4519-9540-de19149d2f3a": {   # AMZSales-DailyInventory
        "notebook_1775918866911.ipynb": "pipelines.spapi.fba_ledger",    # AMZSales-DailyINV-FBAINVLedger
        "notebook_1775918997172.ipynb": "pipelines.spapi.awd_inventory", # AMZSales-DailyINV-AWDINVLedger
    },
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
rows = run()
print(f"{module}: {{rows}} rows written")
"""


def cell(src):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": src.splitlines(keepends=True)}


token = subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()
H = {"Authorization": f"Bearer {token}"}


def get(url):
    r = requests.get(url, headers=H, timeout=60); r.raise_for_status(); return r.json()


def post(url, body):
    r = requests.post(url, headers=H, json=body, timeout=120)
    if r.status_code >= 300:
        raise SystemExit(f"POST {url.split('/repositories/')[1]} -> {r.status_code} {r.text[:400]}")
    return r.json() if r.text else {}


for repo, mapping in PLAN.items():
    print(f"\n=== {repo[:8]}")
    head = get(f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
    ops = {}
    for f, module in mapping.items():
        raw = base64.b64decode(get(f"{B}/{repo}:readFile?commitSha={head}&path=definitions/{f}")["contents"])
        nb = json.loads(raw)
        nb["cells"] = [cell(INSTALL), cell(RUN.format(module=module))]
        out = json.dumps(nb, indent=1, ensure_ascii=False)
        ops[f"definitions/{f}"] = {"writeFile": {"contents": base64.b64encode(out.encode()).decode()}}
        print(f"  {f} -> {module}")
    if CHECK:
        print(f"  --check: would commit {len(ops)} files on top of {head[:8]}"); continue
    post(f"{B}/{repo}:commit", {
        "commitMetadata": {"author": {"name": "Grant Weatherford", "emailAddress": "grant.weatherford@punlabs.io"},
                           "commitMessage": "Replace notebook bodies with calls into the shared punlabs-pipelines package"},
        "requiredHeadCommitSha": head, "fileOperations": ops})
    head = get(f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
    print(f"  committed -> {head[:8]}")
    before = get(f"{B}/{repo}/releaseConfigs/default").get("releaseCompilationResult", "")
    comp = post(f"{B}/{repo}/compilationResults", {"releaseConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/releaseConfigs/default"})
    print(f"  compiled {comp['name'].split('/')[-1][:12]}; errors: {len(comp.get('compilationErrors', []))}")
    for _ in range(6):
        after = get(f"{B}/{repo}/releaseConfigs/default").get("releaseCompilationResult", "")
        if after != before:
            break
        time.sleep(5)
    print("  release now points at:", after.split("/")[-1][:12], "(changed)" if after != before else "(UNCHANGED)")
    print("  now run release.py to move the release pin to this commit")
