# BigQuery pipelines (Dataform) deploy tooling

The two scheduled pipelines live in Dataform repositories with no git remote:

| Repository id | Display name | Schedule (America/New_York) |
|---|---|---|
| `eb6fd087-cb1a-4bef-84df-1f622a1c1843` | PL-AMZSales-PunDataPipe-DailyActivity | 07:00 daily |
| `2ceefade-8ee6-4519-9540-de19149d2f3a` | AMZSales-DailyInventory | 15:00 daily |

Each release config is pinned to a compilation result and scheduled runs use
that pin. Committing to main does not move it, compiling by hand does not,
and the config only accepts `gitCommitish: main`. The supported way is a
release cron: the config compiles from main on its schedule and moves the pin.
`release.py` sets that cron to 06:30 America/New_York on both repositories,
ahead of the 07:00 and 15:00 runs. After that, a deploy is just a commit.

- `release.py` — set the release cron; `--run` / `--run-inventory` also compile
  from main right now and start a workflow invocation on that compilation,
  which does not wait for the pin.

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
