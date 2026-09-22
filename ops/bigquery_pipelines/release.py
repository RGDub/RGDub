"""Make the pipelines pick up commits to main, and optionally run one now.

What we learned on 2026-09-21 about these BigQuery-managed Dataform repos:
* scheduled runs use releaseConfig.releaseCompilationResult (the "pin");
* committing to main does not move the pin, nor does compiling by hand, nor
  does a PATCH that changes nothing;
* the release config only accepts gitCommitish "main";
* a release config WITH a cronSchedule compiles from main on that schedule and
  moves the pin. That is the supported path, so this script sets one: 06:30
  America/New_York, before the 07:00 Daily Activity and 15:00 Daily Inventory
  runs. Every deploy is then just a commit to main.

For an immediate test, --run / --run-inventory compile from the release config
right now and start a workflow invocation against that explicit compilation,
which does not need the pin.

    python release.py                 # set gitCommitish=main + 06:30 ET release cron on both
    python release.py --run           # ...and run Daily Activity now on a fresh compilation
    python release.py --run-inventory # ...and/or Daily Inventory
"""
import subprocess, sys, time
import requests

B = "https://dataform.googleapis.com/v1beta1/projects/punlabs/locations/us-central1/repositories"
REPOS = {
    "eb6fd087-cb1a-4bef-84df-1f622a1c1843": ("PL-AMZSales-PunDataPipe-DailyActivity", "PunDataPipe-DailyRun", "--run"),
    "2ceefade-8ee6-4519-9540-de19149d2f3a": ("AMZSales-DailyInventory", "DailyINVDownload", "--run-inventory"),
}
RELEASE_CRON, TZ = "30 6 * * *", "America/New_York"
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
    if cfg.get("gitCommitish") != "main" or cfg.get("cronSchedule") != RELEASE_CRON:
        call("PATCH", f"{B}/{repo}/releaseConfigs/default?updateMask=gitCommitish,cronSchedule,timeZone",
             json={"gitCommitish": "main", "cronSchedule": RELEASE_CRON, "timeZone": TZ})
        cfg = call("GET", f"{B}/{repo}/releaseConfigs/default")
    print(f"  release config: gitCommitish={cfg.get('gitCommitish')} cron='{cfg.get('cronSchedule')}' {cfg.get('timeZone')}"
          f" | pin {cfg.get('releaseCompilationResult','').split('/')[-1][:12]} | head of main {head[:8]}")
    for _ in range(6):            # a real config change sometimes releases immediately
        time.sleep(5)
        after = call("GET", f"{B}/{repo}/releaseConfigs/default").get("releaseCompilationResult", "")
        if after != before:
            print(f"  pin moved -> {after.split('/')[-1][:12]}")
            break
    else:
        print("  pin unchanged for now; the 06:30 ET release will move it to head of main")

    if flag in sys.argv:
        comp = call("POST", f"{B}/{repo}/compilationResults",
                    json={"releaseConfig": f"projects/punlabs/locations/us-central1/repositories/{repo}/releaseConfigs/default"})
        errs = comp.get("compilationErrors", [])
        print(f"  compiled {comp['name'].split('/')[-1][:12]} from {comp.get('resolvedGitCommitSha','?')[:8]}; errors: {len(errs)}")
        for e in errs[:5]:
            print("    ", e.get("path"), e.get("message", "")[:160])
        if errs:
            continue
        wf = call("GET", f"{B}/{repo}/workflowConfigs/{workflow}")
        inv = call("POST", f"{B}/{repo}/workflowInvocations",
                   json={"compilationResult": comp["name"], "invocationConfig": wf.get("invocationConfig", {})})
        print(f"  started {workflow} as {inv['name'].split('/')[-1][:12]} on that compilation ({inv.get('state')})")
