# Daily Finance Briefing

`FinanceDataReader` based static report generator for a daily market summary page.

## What it does

- Collects the latest two valid close prices for each configured market symbol
- Calculates the latest value, direction, and daily percentage change
- Renders a static HTML briefing page similar to a market digest card layout
- Stores both `latest` output and date-based archives under `site/`
- Supports scheduled execution with GitHub Actions and GitHub Pages deployment

## Default symbols

- Domestic: `KS11`, `KQ11`
- Global: `DJI`, `IXIC`, `SSEC`, `N225`
- FX: `USD/KRW`, `USD/CNY`
- Commodities: `GC=F`, `SI=F`, `CL=F`

## Local setup

```bash
python -m pip install --upgrade pip
pip install -e ".[test]"
python -m daily_finance_briefing generate --output site --config config/assets.yml
```

## CLI

```bash
python -m daily_finance_briefing generate --run-date 2026-04-24 --output site --config config/assets.yml
```

- `--run-date`: optional KST run date in `YYYY-MM-DD`
- `--output`: output folder, default `site`
- `--config`: asset config file, default `config/assets.yml`

## Output structure

```text
site/
  index.html
  data.json
  latest/
    index.html
    data.json
  archive/
    YYYY-MM-DD/
      index.html
      data.json
```

## GitHub setup

1. Enable GitHub Actions for the repository.
2. In repository settings, set GitHub Pages source to `GitHub Actions`.
3. Keep the workflow on the default branch so the schedule trigger can run.

The workflow is scheduled for `7 1 * * 1-6`, which is `10:07 KST` from Monday to Saturday.
