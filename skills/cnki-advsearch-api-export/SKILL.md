---
name: "cnki-advsearch-api-export"
description: "Use when you need fast CNKI KNS8 advanced-search extraction via /kns8s/brief/grid and CSV export (paper rows + author summary + author contact info)."
---

# CNKI Advanced Search API Export Skill

## When to use
- Need to export CNKI advanced-search results faster than UI page-by-page scraping.
- Need paper-level CSV and author-level CSV from the same query.
- Need keyword-focused extraction (default keyword: `病`) and author contact enrichment.

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
  --keyword "病" \
  --output-dir "output/spreadsheet"
```

## Outputs
- `output/spreadsheet/cnki_medical_records.csv`（含每条文献作者联系方式字段）
- `output/spreadsheet/cnki_medical_authors.csv`（含作者聚合联系方式字段）

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
- Slow down author-contact requests to reduce anti-bot risk:
```bash
--author-contact-delay-ms 120
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
- Keyword is patched into query payload (`SU=病` by default), and any `CCL` subject filter conditions are removed.
- If CNKI returns anti-bot or empty pages intermittently, retry with `--headed` and increase `--delay-ms`.
