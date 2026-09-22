# BigQuery pipelines (Dataform) deploy tooling

The two scheduled pipelines live in Dataform repositories with no git remote:

| Repository id | Display name | Schedule (America/New_York) |
|---|---|---|
| `eb6fd087-cb1a-4bef-84df-1f622a1c1843` | PL-AMZSales-PunDataPipe-DailyActivity | 07:00 daily |
| `2ceefade-8ee6-4519-9540-de19149d2f3a` | AMZSales-DailyInventory | 15:00 daily |

Each release config is pinned to a compilation result and scheduled runs use
that pin. In these BigQuery-managed repositories: committing to main does not
move it, compiling by hand does not, `gitCommitish` must be `main`, and release
cron schedules are refused. The path that remains is the documented roll-back
call: compile from the release config, then PATCH `releaseCompilationResult`
to that compilation. `release.py` does exactly that for both repositories.

- `release.py` — release head of main on both pipelines; `--run` /
  `--run-inventory` also start a workflow invocation immediately.
- `deploy_shim.py` — step 1 (2026-09-21): prepend `secret_shim_cell.py` to every
  notebook so scheduled runs survive the runtime image dropping
  `google-cloud-secret-manager`. Idempotent.
- `deploy_thin.py` — step 2: replace each notebook body with two cells (install
  the `punlabs-pipelines` wheel from GCS, call the extractor's `run()`).

Publish a new package version before running `deploy_thin.py`:

    python -m build --wheel -o dist/
    gsutil cp dist/punlabs_pipelines-<ver>-py3-none-any.whl gs://punlabsamzraw/packages/
    echo punlabs_pipelines-<ver>-py3-none-any.whl | gsutil cp - gs://punlabsamzraw/packages/latest.txt

The notebooks' install cell reads `latest.txt` and installs the wheel it names,
so publishing a new version is those two copies; no notebook edit is needed.

Both scripts take `--check` to print what they would change without touching anything.
