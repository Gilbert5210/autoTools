#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

try:
    from playwright.async_api import async_playwright
except Exception as exc:  # pragma: no cover
    async_playwright = None  # type: ignore
    _playwright_import_error = exc


DEFAULT_URL = "https://kns.cnki.net/kns8s/AdvSearch?classid=YSTT4HG0&isFirst=1&rlang=both"
DEFAULT_SUBJECT_CODES = ["A006", "E056", "E057"]
AUTHOR_SPLIT_RE = re.compile(r"[;；]+")
DIGIT_RE = re.compile(r"\d+")


@dataclass
class ExportResult:
    total_records: int
    total_authors: int
    records_csv: Path
    authors_csv: Path


def _normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _to_int(value: Any) -> int:
    text = _normalize_text(value)
    m = DIGIT_RE.search(text.replace(",", ""))
    return int(m.group(0)) if m else 0


def _resolve_brief_grid_url(page_url: str) -> str:
    parsed = urlparse(page_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"无法解析页面 URL：{page_url}")
    segments = [seg for seg in parsed.path.split("/") if seg]
    app_path = f"/{segments[0]}" if segments else "/kns8s"
    return f"{parsed.scheme}://{parsed.netloc}{app_path}/brief/grid"


def _payload_to_form(payload: Dict[str, Any]) -> Dict[str, str]:
    form: Dict[str, str] = {}
    for key, value in payload.items():
        if value is None:
            form[key] = ""
            continue
        if isinstance(value, bool):
            form[key] = "true" if value else "false"
            continue
        if isinstance(value, (dict, list)):
            form[key] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            continue
        form[key] = str(value)
    return form


def _parse_query_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = payload.get("queryJson")
    if not raw:
        raise ValueError("briefRequest 缺少 queryJson 字段")
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        raise ValueError("queryJson 不是有效字符串")
    return json.loads(raw)


def _dump_query_json(payload: Dict[str, Any], query_obj: Dict[str, Any]) -> None:
    payload["queryJson"] = json.dumps(query_obj, ensure_ascii=False, separators=(",", ":"))


def _has_subject_codes(payload: Dict[str, Any], subject_codes: Iterable[str]) -> bool:
    try:
        query_obj = _parse_query_json(payload)
    except Exception:
        return False

    wanted = {str(code).strip() for code in subject_codes if str(code).strip()}
    if not wanted:
        return True

    qgroups = (((query_obj.get("qnode") or {}).get("qgroup")) or [])
    found: set[str] = set()
    for group in qgroups:
        if not isinstance(group, dict):
            continue
        items = group.get("items") or []
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("field") or "").upper() != "CCL":
                continue
            val = _normalize_text(item.get("value"))
            if val:
                found.add(val)
    return wanted.issubset(found)


def _inject_subject_codes(payload: Dict[str, Any], subject_codes: List[str]) -> None:
    subject_codes = [code.strip() for code in subject_codes if code.strip()]
    if not subject_codes:
        return

    query_obj = _parse_query_json(payload)
    qnode = query_obj.setdefault("qnode", {})
    qgroups = qnode.setdefault("qgroup", [])
    if not isinstance(qgroups, list):
        raise ValueError("queryJson.qnode.qgroup 结构异常")

    control_group: Optional[Dict[str, Any]] = None
    for group in qgroups:
        if isinstance(group, dict) and str(group.get("key") or "") == "ControlGroup":
            control_group = group
            break
    if control_group is None:
        control_group = {
            "key": "ControlGroup",
            "title": "",
            "logic": "AND",
            "items": [],
            "childItems": [],
        }
        qgroups.append(control_group)

    items = control_group.get("items")
    if not isinstance(items, list):
        items = []

    kept_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("field") or "").upper() == "CCL":
            continue
        kept_items.append(item)

    for idx, code in enumerate(subject_codes):
        kept_items.append(
            {
                "logic": "AND" if idx == 0 else "OR",
                "operator": "DEFAULT",
                "field": "CCL",
                "value": code,
                "value2": None,
                "title": "学科",
            }
        )

    control_group["items"] = kept_items
    _dump_query_json(payload, query_obj)


async def _extract_brief_request_payload(page: Any) -> Dict[str, Any]:
    raw = await page.locator("#briefRequest").input_value(timeout=15000)
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("briefRequest 不是对象")
    return data


async def _try_apply_subject_filters_via_dom(page: Any, subject_codes: List[str]) -> None:
    if not subject_codes:
        return
    await page.evaluate(
        r"""(codes) => {
  const normalize = (v) => String(v == null ? '' : v).trim();
  const wanted = new Set(codes.map((x) => normalize(x)).filter(Boolean));
  if (!wanted.size) return;

  const subjectDl = document.querySelector("dl[groupid='CCL']");
  if (!subjectDl) return;

  const allInputs = Array.from(subjectDl.querySelectorAll("dd input[type='checkbox']"));
  for (const input of allInputs) {
    input.checked = wanted.has(normalize(input.value));
  }

  const sidebarBtns = document.querySelector(".sidebar-filter-btns");
  if (sidebarBtns) {
    sidebarBtns.classList.remove('disableclick');
    sidebarBtns.style.display = 'block';
  }

  if (typeof window.mutiSelectedGroup === 'function') {
    window.mutiSelectedGroup();
  }
}""",
        subject_codes,
    )


async def _parse_grid_html(parse_page: Any, html_text: str) -> Dict[str, Any]:
    return await parse_page.evaluate(
        r"""(html) => {
  const norm = (v) => String(v == null ? '' : v).replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
  const parser = new DOMParser();
  const doc = parser.parseFromString(html || '', 'text/html');

  const totalCountText = norm(doc.querySelector('#countPageDiv em')?.textContent || '0');
  const pageMarkText = norm(doc.querySelector('span.countPageMark')?.textContent || '');
  const pageMarkAttr = norm(doc.querySelector('span.countPageMark')?.getAttribute('data-pagenum') || '');
  const turnpage = norm(doc.querySelector('#hidTurnPage')?.getAttribute('value') || doc.querySelector('#hidTurnPage')?.value || '');

  let curPage = 0;
  let totalPages = 0;
  const markMatch = pageMarkText.match(/(\d+)\s*\/\s*(\d+)/);
  if (markMatch) {
    curPage = Number(markMatch[1]) || 0;
    totalPages = Number(markMatch[2]) || 0;
  }
  if (!totalPages && pageMarkAttr) {
    totalPages = Number(pageMarkAttr) || 0;
  }

  const rows = [];
  const tableRows = Array.from(doc.querySelectorAll('table.result-table-list tr'));
  for (const tr of tableRows.slice(1)) {
    const tds = tr.querySelectorAll('td');
    if (!tds || tds.length < 4) continue;

    const titleAnchor = tr.querySelector('td.name a.fz14') || tr.querySelector('td.name a');
    const authorCell = tr.querySelector('td.author');
    const authorNames = authorCell
      ? Array.from(authorCell.querySelectorAll('a.KnowledgeNetLink, a')).map((a) => norm(a.textContent)).filter(Boolean)
      : [];

    const collectNode = tr.querySelector('.icon-collect');
    const seqNode = tr.querySelector('td.seq');
    const checkbox = tr.querySelector('td.seq input[type="checkbox"]');

    rows.push({
      seq: norm(seqNode?.textContent || ''),
      title: norm(titleAnchor?.textContent || ''),
      title_url: norm(titleAnchor?.getAttribute('href') || ''),
      authors: authorNames.join(';'),
      authors_raw: norm(authorCell?.textContent || ''),
      journal: norm(tr.querySelector('td.source')?.textContent || ''),
      publish_time: norm(tr.querySelector('td.date')?.textContent || ''),
      cited: norm(tr.querySelector('td.quote')?.textContent || ''),
      downloads: norm(tr.querySelector('td.download')?.textContent || ''),
      dbname: norm(collectNode?.getAttribute('data-dbname') || ''),
      filename: norm(collectNode?.getAttribute('data-filename') || ''),
      cookie_value: norm(checkbox?.getAttribute('value') || ''),
    });
  }

  return {
    meta: {
      total_count_text: totalCountText,
      page_mark_text: pageMarkText,
      current_page: curPage,
      total_pages: totalPages,
      turnpage,
    },
    rows,
  };
}""",
        html_text,
    )


def _split_authors(value: str) -> List[str]:
    text = _normalize_text(value)
    if not text:
        return []
    names = [_normalize_text(x) for x in AUTHOR_SPLIT_RE.split(text)]
    return [n for n in names if n and n != "等"]


def _parse_publish_time(value: str) -> Optional[datetime]:
    text = _normalize_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _build_author_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    title_sets: Dict[str, set[str]] = {}

    for rec in records:
        names = _split_authors(str(rec.get("authors") or ""))
        if not names:
            names = _split_authors(str(rec.get("authors_raw") or ""))

        title = _normalize_text(rec.get("title"))
        journal = _normalize_text(rec.get("journal"))
        publish_time = _normalize_text(rec.get("publish_time"))
        title_key = f"{title}|{_normalize_text(rec.get('filename'))}|{_normalize_text(rec.get('title_url'))}"

        for name in names:
            if name not in index:
                index[name] = {
                    "author": name,
                    "paper_count": 0,
                    "sample_title": title,
                    "sample_journal": journal,
                    "last_publish_time": publish_time,
                }
                title_sets[name] = set()

            if title_key and title_key not in title_sets[name]:
                title_sets[name].add(title_key)
                index[name]["paper_count"] += 1

            prev_dt = _parse_publish_time(str(index[name].get("last_publish_time") or ""))
            cur_dt = _parse_publish_time(publish_time)
            if cur_dt and (prev_dt is None or cur_dt > prev_dt):
                index[name]["last_publish_time"] = publish_time

    rows = list(index.values())
    rows.sort(key=lambda x: x["author"])
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


async def _run(args: argparse.Namespace) -> ExportResult:
    if async_playwright is None:
        raise RuntimeError(
            "未检测到 Playwright，请先安装：`python3 -m pip install playwright && python3 -m playwright install chromium`"
        ) from globals().get("_playwright_import_error")

    subject_codes = [x.strip() for x in str(args.subject_codes or "").split(",") if x.strip()]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headed, channel=args.channel or None)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(args.url, wait_until="domcontentloaded", timeout=args.navigation_timeout_ms)
        await page.wait_for_selector("#briefRequest", timeout=args.selector_timeout_ms)

        if args.load_brief_request:
            payload = json.loads(Path(args.load_brief_request).read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("load_brief_request 文件不是 JSON 对象")
        else:
            if subject_codes:
                await _try_apply_subject_filters_via_dom(page, subject_codes)
                await page.wait_for_timeout(args.subject_wait_ms)
            payload = await _extract_brief_request_payload(page)

        if subject_codes and not _has_subject_codes(payload, subject_codes):
            _inject_subject_codes(payload, subject_codes)

        if args.save_brief_request:
            save_path = Path(args.save_brief_request)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        page_size = int(args.page_size)
        if page_size <= 0:
            raise ValueError("page_size 必须大于 0")

        start_page = int(payload.get("pageNum") or 1)
        if args.start_page > 0:
            start_page = int(args.start_page)

        payload["pageSize"] = page_size
        payload["size"] = page_size
        payload["pageNum"] = start_page
        payload["start"] = (start_page - 1) * page_size + 1
        payload["boolSearch"] = bool(payload.get("boolSearch", True))

        endpoint = _resolve_brief_grid_url(page.url)
        parse_page = await context.new_page()

        current_page = start_page
        fetched_pages = 0
        all_records: List[Dict[str, Any]] = []
        max_pages = int(args.max_pages)

        while True:
            payload["pageNum"] = current_page
            payload["start"] = (current_page - 1) * page_size + 1
            payload["CurPage"] = str(current_page)
            if current_page > start_page:
                payload["boolSearch"] = False

            form = _payload_to_form(payload)
            response = await context.request.post(
                endpoint,
                form=form,
                timeout=args.request_timeout_ms,
                headers={
                    "Referer": args.url,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )

            html_text = await response.text()
            parsed = await _parse_grid_html(parse_page, html_text)
            meta = parsed.get("meta") or {}
            rows = parsed.get("rows") or []

            for row in rows:
                row["page_num"] = current_page
                row["subject_codes"] = ";".join(subject_codes)
                row["query_url"] = args.url
                row["total_count"] = _to_int(meta.get("total_count_text"))
                row["cited"] = _to_int(row.get("cited"))
                row["downloads"] = _to_int(row.get("downloads"))
            all_records.extend(rows)

            fetched_pages += 1
            total_pages = int(meta.get("total_pages") or 0)
            turnpage = _normalize_text(meta.get("turnpage"))
            if turnpage:
                payload["turnpage"] = turnpage

            print(
                f"[page={current_page}] rows={len(rows)} total={_to_int(meta.get('total_count_text'))} total_pages={total_pages or '-'}"
            )

            if not rows:
                break
            if max_pages > 0 and fetched_pages >= max_pages:
                break
            if total_pages > 0 and current_page >= total_pages:
                break

            current_page += 1
            if args.delay_ms > 0:
                await asyncio.sleep(args.delay_ms / 1000.0)

        await parse_page.close()
        await context.close()
        await browser.close()

    author_rows = _build_author_rows(all_records)

    output_dir = Path(args.output_dir)
    records_csv = output_dir / args.records_csv
    authors_csv = output_dir / args.authors_csv

    record_headers = [
        "page_num",
        "seq",
        "title",
        "title_url",
        "authors",
        "authors_raw",
        "journal",
        "publish_time",
        "cited",
        "downloads",
        "dbname",
        "filename",
        "cookie_value",
        "total_count",
        "subject_codes",
        "query_url",
    ]
    author_headers = [
        "author",
        "paper_count",
        "sample_title",
        "sample_journal",
        "last_publish_time",
    ]

    _write_csv(records_csv, all_records, record_headers)
    _write_csv(authors_csv, author_rows, author_headers)

    return ExportResult(
        total_records=len(all_records),
        total_authors=len(author_rows),
        records_csv=records_csv,
        authors_csv=authors_csv,
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CNKI 高级检索（接口分页）导出 CSV")
    p.add_argument("--url", default=DEFAULT_URL, help="高级检索页面 URL")
    p.add_argument("--subject-codes", default=",".join(DEFAULT_SUBJECT_CODES), help="学科编码，逗号分隔")
    p.add_argument("--start-page", type=int, default=1, help="起始页码")
    p.add_argument("--max-pages", type=int, default=0, help="最大抓取页数，0=抓到末页")
    p.add_argument("--page-size", type=int, default=20, help="每页条数（通常 20/50/100）")
    p.add_argument("--delay-ms", type=int, default=120, help="分页请求间隔毫秒")
    p.add_argument("--output-dir", default="output/spreadsheet", help="CSV 输出目录")
    p.add_argument("--records-csv", default="cnki_medical_records.csv", help="文献明细 CSV 文件名")
    p.add_argument("--authors-csv", default="cnki_medical_authors.csv", help="作者汇总 CSV 文件名")
    p.add_argument("--save-brief-request", default="", help="保存当前 briefRequest JSON 到文件")
    p.add_argument("--load-brief-request", default="", help="从文件加载 briefRequest JSON，跳过页面提取")
    p.add_argument("--headed", action="store_true", help="启用有头浏览器")
    p.add_argument("--channel", default="", help="浏览器 channel（可选：chrome/edge）")
    p.add_argument("--subject-wait-ms", type=int, default=1200, help="学科筛选提交后等待毫秒")
    p.add_argument("--navigation-timeout-ms", type=int, default=30000, help="页面打开超时毫秒")
    p.add_argument("--selector-timeout-ms", type=int, default=15000, help="选择器等待超时毫秒")
    p.add_argument("--request-timeout-ms", type=int, default=30000, help="接口请求超时毫秒")
    return p


def main() -> int:
    args = _build_parser().parse_args()
    result = asyncio.run(_run(args))
    print(
        "导出完成："
        f"records={result.total_records}, authors={result.total_authors}, "
        f"records_csv={result.records_csv}, authors_csv={result.authors_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
