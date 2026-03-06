---
name: "cnki-advsearch-api-export"
description: "Use when you need fast CNKI KNS8 advanced-search extraction via /kns8s/brief/grid and CSV export (paper rows + author summary), especially for medical subjects A006/E056/E057."
---

# CNKI Advanced Search API Export Skill

## When to use
- Need to export CNKI advanced-search results faster than UI page-by-page scraping.
- Need paper-level CSV and author-level CSV from the same query.
- Need medical subject filtering for:
  - `A006` 生物学
  - `E056` 中医学
  - `E057` 中药学

## Script
- `scripts/export_cnki_advsearch_csv.py`

## Prerequisites
- Python 3.9+
- Playwright Python package + browser:
```bash
python3 -m pip install playwright
python3 -m playwright install chromium
```

## Quick start
From repo root:
```bash
python3 skills/cnki-advsearch-api-export/scripts/export_cnki_advsearch_csv.py \
  --url "https://kns.cnki.net/kns8s/AdvSearch?classid=YSTT4HG0&isFirst=1&rlang=both" \
  --subject-codes "A006,E056,E057" \
  --output-dir "output/spreadsheet"
```

## Outputs
- `output/spreadsheet/cnki_medical_records.csv`
- `output/spreadsheet/cnki_medical_authors.csv`

## Useful options
- Limit pages:
```bash
--max-pages 10
```
- Change page size:
```bash
--page-size 50
```
- Keep browser visible for troubleshooting:
```bash
--headed
```
- Save extracted `briefRequest` payload:
```bash
--save-brief-request output/spreadsheet/brief_request.json
```
- Reuse a saved payload directly:
```bash
--load-brief-request output/spreadsheet/brief_request.json
```

## Notes
- The script uses CNKI page session + `/brief/grid` interface pagination.
- Subject filters are first attempted through in-page DOM apply; if missing, payload is patched with `CCL` conditions.
- If CNKI returns anti-bot or empty pages intermittently, retry with `--headed` and increase `--delay-ms`.
