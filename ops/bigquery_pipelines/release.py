"""Release head of main to the pipelines, and optionally run one now.

These are BigQuery-managed ("first-party") Dataform repositories. Rules learned
on 2026-09-21, each from an API error:
* scheduled runs use releaseConfig.releaseCompilationResult (the "pin");
* committing to main does not move the pin; compiling by hand does not either;
* gitCommitish must be exactly "main";
* release cron schedules are not supported ("strictActAsChecks").
What remains is the documented roll-back path: set releaseCompilationResult
to a compilation result created from this release config. So a deploy is:
commit to main, compile from the release config, point the pin at it.

    python release.py                 # release head of main on both pipelines
    python release.py --run           # ...and start Daily Activity now
    python release.py --run-inventory # ...and/or Daily Inventory
    python release.py --pin eb6fd087=<compilationResultId> [--pin 2ceefade=<id>]
                                      # roll a pipeline back to an earlier compilation
                                      # (known good: eb6fd087=d1e8a4f0-6cb..., 2ceefade=20950fad-305...)
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


PINS = {a.split("=")[0]: a.split("=")[1] for a in sys.argv[sys.argv.index("--pin") + 1:] if "=" in a} if "--pin" in sys.argv else {}

for repo, (name, workflow, flag) in REPOS.items():
    print(f"\n=== {name}")
    rc = f"{B}/{repo}/releaseConfigs/default"
    if PINS:
        wanted = PINS.get(repo[:8])
        if not wanted:
            continue
        full = next(c["name"] for c in call("GET", f"{B}/{repo}/compilationResults?pageSize=200")["compilationResults"]
                    if c["name"].split("/")[-1].startswith(wanted))
        r = requests.patch(f"{rc}?updateMask=releaseCompilationResult", headers=H,
                           json={"gitCommitish": "main", "releaseCompilationResult": full}, timeout=120)
        print(f"  rollback -> {full.split('/')[-1][:12]}: HTTP {r.status_code}")
        continue
    head = call("GET", f"{B}/{repo}:fetchHistory?pageSize=1")["commits"][0]["commitSha"]
    cfg = call("GET", rc)
    if cfg.get("gitCommitish") != "main":
        call("PATCH", f"{rc}?updateMask=gitCommitish", json={"gitCommitish": "main"})
        print("  gitCommitish restored to main")
    before = cfg.get("releaseCompilationResult", "")

    comp = call("POST", f"{B}/{repo}/compilationResults",
                json={"releaseConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/releaseConfigs/default"})
    errs = comp.get("compilationErrors", [])
    print(f"  compiled {comp['name'].split('/')[-1][:12]} from {comp.get('resolvedGitCommitSha','?')[:8]} (head {head[:8]}); errors: {len(errs)}")
    for e in errs[:5]:
        print("    ", e.get("path"), e.get("message", "")[:160])
    if errs:
        continue

    # The API validates the whole resource on update, so gitCommitish must be restated.
    r = requests.patch(f"{rc}?updateMask=releaseCompilationResult",
                       headers=H, json={"gitCommitish": "main", "releaseCompilationResult": comp["name"]}, timeout=120)
    if r.status_code >= 300:
        print(f"  setting the pin was refused: HTTP {r.status_code} {r.text[:300]}")
    time.sleep(3)
    after = call("GET", rc).get("releaseCompilationResult", "")
    if after == comp["name"]:
        print(f"  release pin -> {after.split('/')[-1][:12]}  (was {before.split('/')[-1][:12]})")
    else:
        print(f"  release pin did NOT move (still {after.split('/')[-1][:12]}); falling back to an explicit-compilation run")

    if flag in sys.argv:
        if after == comp["name"]:
            inv = call("POST", f"{B}/{repo}/workflowInvocations",
                       json={"workflowConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/workflowConfigs/{workflow}"})
        else:
            wf = call("GET", f"{B}/{repo}/workflowConfigs/{workflow}")
            inv = call("POST", f"{B}/{repo}/workflowInvocations",
                       json={"compilationResult": comp["name"], "invocationConfig": wf.get("invocationConfig", {})})
        print(f"  started {workflow} as {inv['name'].split('/')[-1][:12]} "
              f"on {inv.get('resolvedCompilationResult', comp['name']).split('/')[-1][:12]} ({inv.get('state')})")
