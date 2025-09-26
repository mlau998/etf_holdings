# Holdings Scraper (MVP)

Purpose
- Fetch ETF holdings from CSV URLs.
- Fetch TCI (or any manager) 13F holdings from SEC EDGAR.
- Normalize to a single CSV per run.
- Optional: store snapshots and compute diffs.

Quick start
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Edit config.yml with real URLs for ETFs and CIK for TCI
python -m scraper.main --config config.yml --out data/holdings_latest.csv

# To save a snapshot and diff against the previous snapshot:
python -m scraper.main --config config.yml --out data/holdings_$(date +%F).csv
python -m scraper.main --config config.yml --out data/holdings_new.csv --prev data/holdings_OLD.csv
```
