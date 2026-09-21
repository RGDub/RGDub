"""Deploy the Secret Manager shim to both BigQuery pipelines.

For each repository:
  1. prepend the shim cell to every deployed notebook that imports secretmanager
  2. commit the patched notebooks to main (requires the current head SHA, so a
     concurrent edit fails loudly instead of being overwritten)
  3. compile a new release from the release config, which is what the schedule runs
  4. confirm the release config now points at the new compilation

Usage:  .venv/bin/python deploy_shim.py            # do it
        .venv/bin/python deploy_shim.py --check    # only show what would change
"""
import base64, glob, json, os, subprocess, sys, time
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
B = "https://dataform.googleapis.com/v1beta1/projects/punlabs/locations/us-central1/repositories"
REPOS = {
    "eb6fd087-cb1a-4bef-84df-1f622a1c1843": "PL-AMZSales-PunDataPipe-DailyActivity",
    "2ceefade-8ee6-4519-9540-de19149d2f3a": "AMZSales-DailyInventory",
}
MARK = "Secret Manager shim (added 2026-09-21)"
CHECK = "--check" in sys.argv

token = subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()
H = {"Authorization": f"Bearer {token}"}
shim = open(os.path.join(HERE, "secret_shim_cell.py")).read()


def get(url, **kw):
    r = requests.get(url, headers=H, timeout=60, **kw)
    r.raise_for_status()
    return r.json()


def post(url, body):
    r = requests.post(url, headers=H, json=body, timeout=120)
    if r.status_code >= 300:
        raise SystemExit(f"POST {url.split('/repositories/')[1]} -> {r.status_code} {r.text[:400]}")
    return r.json() if r.text else {}


for repo, name in REPOS.items():
    print(f"\n=== {name}")
    head = get(f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
    files = [e["file"] for e in get(f"{B}/{repo}:queryDirectoryContents?commitSha={head}&path=definitions")["directoryEntries"] if e.get("file", "").endswith(".ipynb")]
    ops = {}
    for f in files:
        raw = base64.b64decode(get(f"{B}/{repo}:readFile?commitSha={head}&path=definitions/{f}")["contents"])
        nb = json.loads(raw)
        src = " ".join("".join(c["source"]) for c in nb["cells"])
        if MARK in src:
            print(f"  {f}: already patched"); continue
        if "from google.cloud import secretmanager" not in src:
            print(f"  {f}: no secretmanager import, left alone"); continue
        nb["cells"].insert(0, {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
                               "source": shim.splitlines(keepends=True)})
        out = json.dumps(nb, indent=1, ensure_ascii=False)
        ops[f"definitions/{f}"] = {"writeFile": {"contents": base64.b64encode(out.encode()).decode()}}
        print(f"  {f}: will prepend shim cell ({len(nb['cells'])-1} -> {len(nb['cells'])} cells)")
    if not ops:
        print("  nothing to commit")
    elif CHECK:
        print(f"  --check: would commit {len(ops)} files on top of {head[:8]}")
        continue
    else:
        post(f"{B}/{repo}:commit", {
            "commitMetadata": {"author": {"name": "Grant Weatherford", "emailAddress": "grant.weatherford@punlabs.io"},
                               "commitMessage": "Add Secret Manager REST shim so scheduled runs survive the runtime image change (2026-09-21)"},
            "requiredHeadCommitSha": head, "fileOperations": ops})
        head = get(f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
        print(f"  committed -> {head[:8]}")

    if CHECK:
        continue
    before = get(f"{B}/{repo}/releaseConfigs/default").get("releaseCompilationResult", "")
    comp = post(f"{B}/{repo}/compilationResults", {"releaseConfig": f"{B}/{repo}/releaseConfigs/default".replace("https://dataform.googleapis.com/v1beta1/", "")})
    errs = comp.get("compilationErrors", [])
    print(f"  compiled {comp['name'].split('/')[-1][:12]} from {comp.get('resolvedGitCommitSha','?')[:8]}; errors: {len(errs)}")
    for e in errs[:5]:
        print("    ", e.get("path"), e.get("message", "")[:160])
    for _ in range(6):
        after = get(f"{B}/{repo}/releaseConfigs/default").get("releaseCompilationResult", "")
        if after != before:
            break
        time.sleep(5)
    print("  release now points at:", after.split("/")[-1][:12], "(changed)" if after != before else "(UNCHANGED)")
    print("  now run release.py to move the release pin to this commit")
