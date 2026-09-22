"""Point each pipeline's release at the current head of main, then optionally run it.

A Dataform release config is pinned to one compilation result and scheduled
runs use that pin (workflowInvocation.resolvedCompilationResult). Committing
to main and even compiling from the release config do not move the pin; only a
release does. Re-saving the release config triggers an immediate release.

    python release.py            # release both pipelines, show the new pin
    python release.py --run      # ...and start a Daily Activity run right away
    python release.py --run-inventory   # ...and/or start the Daily Inventory run
"""
import subprocess, sys, time
import requests

B = "https://dataform.googleapis.com/v1beta1/projects/punlabs/locations/us-central1/repositories"
REPOS = {
    "eb6fd087-cb1a-4bef-84df-1f622a1c1843": ("PL-AMZSales-PunDataPipe-DailyActivity", "PunDataPipe-DailyRun", "--run"),
    "2ceefade-8ee6-4519-9540-de19149d2f3a": ("AMZSales-DailyInventory", "DailyINVDownload", "--run-inventory"),
}
token = subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()
H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def call(method, url, **kw):
    r = requests.request(method, url, headers=H, timeout=120, **kw)
    if r.status_code >= 300:
        raise SystemExit(f"{method} {url.split('/repositories/')[1]} -> {r.status_code} {r.text[:400]}")
    return r.json() if r.text else {}


for repo, (name, workflow, flag) in REPOS.items():
    print(f"\n=== {name}")
    head = call("GET", f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
    cfg = call("GET", f"{B}/{repo}/releaseConfigs/default")
    before = cfg.get("releaseCompilationResult", "")
    # A release only fires when the config actually changes, so pin it to the
    # head commit SHA. Re-running at the same head bounces through "main" first.
    if cfg.get("gitCommitish") == head:
        call("PATCH", f"{B}/{repo}/releaseConfigs/default?updateMask=gitCommitish", json={"gitCommitish": "main"})
        time.sleep(3)
    call("PATCH", f"{B}/{repo}/releaseConfigs/default?updateMask=gitCommitish", json={"gitCommitish": head})
    after, resolved = before, ""
    for _ in range(18):
        time.sleep(5)
        cfg = call("GET", f"{B}/{repo}/releaseConfigs/default")
        after = cfg.get("releaseCompilationResult", "")
        if after and after != before:
            break
    if after == before:
        # Fall back to compiling from the release config and pointing the pin at it explicitly.
        comp = call("POST", f"{B}/{repo}/compilationResults",
                    json={"releaseConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/releaseConfigs/default"})
        call("PATCH", f"{B}/{repo}/releaseConfigs/default?updateMask=releaseCompilationResult",
             json={"releaseCompilationResult": comp["name"]})
        time.sleep(3)
        cfg = call("GET", f"{B}/{repo}/releaseConfigs/default")
        after = cfg.get("releaseCompilationResult", "")
    if not after or after == before:
        print(f"  pin did not move (still {before.split('/')[-1][:12]}); gitCommitish now {cfg.get('gitCommitish','?')[:8]}")
        continue
    comp = call("GET", f"https://dataform.googleapis.com/v1beta1/{after}")
    resolved = comp.get("resolvedGitCommitSha", "")
    errs = comp.get("compilationErrors", [])
    print(f"  release -> {after.split('/')[-1][:12]} from commit {resolved[:8]} (head {head[:8]}); errors: {len(errs)}")
    for e in errs[:5]:
        print("    ", e.get("path"), e.get("message", "")[:160])
    if resolved and resolved != head:
        print("  WARNING: release is not at head of main")
    if flag in sys.argv:
        inv = call("POST", f"{B}/{repo}/workflowInvocations",
                   json={"workflowConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/workflowConfigs/{workflow}"})
        print(f"  started {workflow}: {inv['name'].split('/')[-1][:12]} "
              f"using {inv.get('resolvedCompilationResult','').split('/')[-1][:12]}")
