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
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urljoin, urlparse

try:
    from playwright.async_api import async_playwright
except Exception as exc:  # pragma: no cover
    async_playwright = None  # type: ignore
    _playwright_import_error = exc


DEFAULT_URL = "https://kns.cnki.net/kns8s/AdvSearch?classid=YSTT4HG0&isFirst=1&rlang=both"
DEFAULT_KEYWORD = "病"
AUTHOR_SPLIT_RE = re.compile(r"[;；]+")
SPLIT_RE = re.compile(r"[;；,，]+")
DIGIT_RE = re.compile(r"\d+")
CONTACT_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
CONTACT_PHONE_LABEL_RE = re.compile(
    r"(?:手机|电话|联系电话|联系方式|Tel|TEL|Mobile|mobile)\s*[:：]?\s*(\+?\d[\d\-\s]{6,20}\d)"
)
CONTACT_MOBILE_RE = re.compile(r"(?<!\d)(?:\+?86[-\s]?)?1[3-9]\d{9}(?!\d)")


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


def _parse_urlencoded_form(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    return {k: v for k, v in parse_qsl(raw, keep_blank_values=True)}


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


def _ensure_control_group(query_obj: Dict[str, Any]) -> Dict[str, Any]:
    qnode = query_obj.setdefault("qnode", {})
    qgroups = qnode.setdefault("qgroup", [])
    if not isinstance(qgroups, list):
        raise ValueError("queryJson.qnode.qgroup 结构异常")

    for group in qgroups:
        if isinstance(group, dict) and str(group.get("key") or "") == "ControlGroup":
            return group

    control_group = {
        "key": "ControlGroup",
        "title": "",
        "logic": "AND",
        "items": [],
        "childItems": [],
    }
    qgroups.append(control_group)
    return control_group


def _remove_subject_code_conditions(payload: Dict[str, Any]) -> None:
    query_obj = _parse_query_json(payload)
    qgroups = (((query_obj.get("qnode") or {}).get("qgroup")) or [])
    for group in qgroups:
        if not isinstance(group, dict):
            continue
        items = group.get("items")
        if not isinstance(items, list):
            continue
        group["items"] = [
            item
            for item in items
            if isinstance(item, dict) and str(item.get("field") or "").upper() != "CCL"
        ]
    _dump_query_json(payload, query_obj)


def _inject_keyword_condition(payload: Dict[str, Any], keyword: str) -> None:
    keyword = _normalize_text(keyword)
    if not keyword:
        raise ValueError("keyword 不能为空")

    query_obj = _parse_query_json(payload)
    control_group = _ensure_control_group(query_obj)
    items = control_group.get("items")
    if not isinstance(items, list):
        items = []

    filtered_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").upper()
        if field in {"SU", "KY"}:
            continue
        if field == "CCL":
            continue
        filtered_items.append(item)

    filtered_items.insert(
        0,
        {
            "logic": "AND",
            "operator": "DEFAULT",
            "field": "SU",
            "value": keyword,
            "value2": None,
            "title": "主题",
        },
    )

    control_group["items"] = filtered_items
    _dump_query_json(payload, query_obj)


def _split_values(value: str) -> List[str]:
    text = _normalize_text(value)
    if not text:
        return []
    parts = [_normalize_text(x) for x in SPLIT_RE.split(text)]
    return [x for x in parts if x]


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) < 7 or len(digits) > 12:
        return ""
    return digits


def _extract_contacts_from_text(text: str) -> Dict[str, List[str]]:
    emails = sorted({x.lower() for x in CONTACT_EMAIL_RE.findall(text or "")})

    phones: set[str] = set()
    for match in CONTACT_PHONE_LABEL_RE.findall(text or ""):
        normalized = _normalize_phone(match)
        if normalized:
            phones.add(normalized)
    for match in CONTACT_MOBILE_RE.findall(text or ""):
        normalized = _normalize_phone(match)
        if normalized:
            phones.add(normalized)

    return {
        "emails": emails,
        "phones": sorted(phones),
    }


def _to_absolute_http_url(base_url: str, raw_url: str) -> str:
    value = _normalize_text(raw_url)
    if not value:
        return ""
    if value.lower().startswith("javascript:"):
        return ""
    absolute = urljoin(base_url, value)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return absolute


def _parse_author_profiles(value: Any) -> List[Dict[str, str]]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []

    rows: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        name = _normalize_text(item.get("name"))
        url = _normalize_text(item.get("url"))
        if not name and not url:
            continue
        rows.append({"name": name, "url": url})
    return rows


def _serialize_profile_contacts(data: Dict[str, Dict[str, set[str]]]) -> str:
    dumped: Dict[str, Dict[str, str]] = {}
    for name, bucket in data.items():
        dumped[name] = {
            "emails": ";".join(sorted(bucket.get("emails") or set())),
            "phones": ";".join(sorted(bucket.get("phones") or set())),
            "sources": ";".join(sorted(bucket.get("sources") or set())),
        }
    return json.dumps(dumped, ensure_ascii=False, separators=(",", ":")) if dumped else ""


async def _fetch_contacts_by_url(
    request_context: Any,
    target_url: str,
    referer: str,
    timeout_ms: int,
    cache: Dict[str, Dict[str, List[str]]],
) -> Dict[str, List[str]]:
    if not target_url:
        return {"emails": [], "phones": []}
    if target_url in cache:
        return cache[target_url]

    result = {"emails": [], "phones": []}
    try:
        response = await request_context.get(
            target_url,
            timeout=timeout_ms,
            headers={
                "Referer": referer,
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        if response.ok:
            text = await response.text()
            result = _extract_contacts_from_text(text)
    except Exception:
        result = {"emails": [], "phones": []}

    cache[target_url] = result
    return result


async def _enrich_record_author_contacts(
    record: Dict[str, Any],
    *,
    request_context: Any,
    base_url: str,
    referer: str,
    timeout_ms: int,
    delay_ms: int,
    cache: Dict[str, Dict[str, List[str]]],
) -> None:
    row_emails: set[str] = set()
    row_phones: set[str] = set()
    row_sources: set[str] = set()
    profile_contacts: Dict[str, Dict[str, set[str]]] = {}

    article_url = _to_absolute_http_url(base_url, str(record.get("title_url") or ""))
    if article_url:
        was_cached = article_url in cache
        contacts = await _fetch_contacts_by_url(
            request_context=request_context,
            target_url=article_url,
            referer=referer,
            timeout_ms=timeout_ms,
            cache=cache,
        )
        if contacts.get("emails") or contacts.get("phones"):
            row_sources.add(article_url)
        row_emails.update(contacts.get("emails") or [])
        row_phones.update(contacts.get("phones") or [])
        if delay_ms > 0 and not was_cached:
            await asyncio.sleep(delay_ms / 1000.0)

    profiles = _parse_author_profiles(record.get("author_profiles_json"))
    resolved_profile_urls: List[str] = []
    for profile in profiles:
        profile_name = _normalize_text(profile.get("name"))
        profile_url = _to_absolute_http_url(base_url, profile.get("url") or "")
        if not profile_url:
            continue
        resolved_profile_urls.append(profile_url)

        was_cached = profile_url in cache
        contacts = await _fetch_contacts_by_url(
            request_context=request_context,
            target_url=profile_url,
            referer=referer,
            timeout_ms=timeout_ms,
            cache=cache,
        )
        if contacts.get("emails") or contacts.get("phones"):
            row_sources.add(profile_url)
        row_emails.update(contacts.get("emails") or [])
        row_phones.update(contacts.get("phones") or [])

        if profile_name:
            bucket = profile_contacts.setdefault(
                profile_name,
                {"emails": set(), "phones": set(), "sources": set()},
            )
            bucket["emails"].update(contacts.get("emails") or [])
            bucket["phones"].update(contacts.get("phones") or [])
            if contacts.get("emails") or contacts.get("phones"):
                bucket["sources"].add(profile_url)

        if delay_ms > 0 and not was_cached:
            await asyncio.sleep(delay_ms / 1000.0)

    record["title_url"] = article_url
    record["author_profile_urls"] = ";".join(sorted(set(resolved_profile_urls)))
    record["author_contact_emails"] = ";".join(sorted(row_emails))
    record["author_contact_phones"] = ";".join(sorted(row_phones))
    record["author_contact_sources"] = ";".join(sorted(row_sources))
    record["author_profile_contacts"] = _serialize_profile_contacts(profile_contacts)


async def _extract_brief_request_payload(page: Any, timeout_ms: int = 15000) -> Dict[str, Any]:
    async def _read_raw(scope: Any) -> str:
        return await scope.evaluate(
            r"""() => {
  const node = document.querySelector('#briefRequest');
  if (!node) return '';
  return String(
    node.value ?? node.getAttribute?.('value') ?? node.textContent ?? ''
  ).trim();
}"""
        )

    raw = ""
    candidates = [page] + list(getattr(page, "frames", []))
    for scope in candidates:
        try:
            await scope.wait_for_function(
                r"""() => {
  const node = document.querySelector('#briefRequest');
  if (!node) return false;
  const raw = String(
    node.value ?? node.getAttribute?.('value') ?? node.textContent ?? ''
  ).trim();
  return raw.length > 2;
}""",
                timeout=timeout_ms,
            )
            raw = await _read_raw(scope)
            if raw:
                break
        except Exception:
            continue

    if not raw:
        raise RuntimeError(
            "未获取到 briefRequest 内容（#briefRequest），请尝试 --headed 或增大 --selector-timeout-ms"
        )

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("briefRequest 不是对象")
    return data


async def _capture_search_endpoint_and_payload(
    page: Any,
    *,
    keyword: str,
    timeout_ms: int,
) -> Dict[str, Any]:
    keyword = _normalize_text(keyword)
    if not keyword:
        raise ValueError("keyword 不能为空")

    keyword_selector_candidates = [
        "input#txt_1_value1",
        ".search-condition .input-box input[type='text']",
        ".search-condition input[type='text'][maxlength='120']",
        "input[type='text'][maxlength='120']",
    ]
    keyword_selector = ""
    for selector in keyword_selector_candidates:
        try:
            await page.wait_for_selector(selector, timeout=timeout_ms)
            keyword_selector = selector
            break
        except Exception:
            continue
    if not keyword_selector:
        return {}

    search_button_selector_candidates = [
        ".search-condition .search-btn",
        "input.search-btn[value='检索']",
        "input[value='检索']",
    ]
    search_button_selector = ""
    for selector in search_button_selector_candidates:
        try:
            await page.wait_for_selector(selector, timeout=timeout_ms)
            search_button_selector = selector
            break
        except Exception:
            continue
    if not search_button_selector:
        return {}

    try:
        await page.fill(keyword_selector, keyword)
    except Exception:
        return {}

    captured: Dict[str, Any] = {}

    def _match_grid_response(resp: Any) -> bool:
        req = resp.request
        if req.method.upper() != "POST":
            return False
        return "/brief/grid" in (resp.url or "")

    try:
        async with page.expect_response(_match_grid_response, timeout=timeout_ms) as response_info:
            await page.click(search_button_selector)
        response = await response_info.value
        request = response.request
        endpoint = _normalize_text(response.url or request.url)
        form_data = _parse_urlencoded_form(request.post_data or "")
        captured = {
            "endpoint": endpoint,
            "form": form_data,
            "briefRequest": _normalize_text(form_data.get("briefRequest") or ""),
        }
    except Exception:
        captured = {}

    if not captured:
        return {}

    raw_brief_request = captured.get("briefRequest") or ""
    if raw_brief_request:
        try:
            payload = json.loads(raw_brief_request)
            if isinstance(payload, dict):
                captured["payload"] = payload
        except Exception:
            pass

    return captured


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
    const authorProfiles = authorCell
      ? Array.from(authorCell.querySelectorAll('a.KnowledgeNetLink, a')).map((a) => ({
          name: norm(a.textContent),
          url: norm(a.getAttribute('href') || a.getAttribute('data-href') || ''),
      }))
      : [];
    const authorNames = authorProfiles.map((item) => item.name).filter(Boolean);

    const collectNode = tr.querySelector('.icon-collect');
    const seqNode = tr.querySelector('td.seq');
    const checkbox = tr.querySelector('td.seq input[type="checkbox"]');

    rows.push({
      seq: norm(seqNode?.textContent || ''),
      title: norm(titleAnchor?.textContent || ''),
      title_url: norm(titleAnchor?.getAttribute('href') || ''),
      authors: authorNames.join(';'),
      authors_raw: norm(authorCell?.textContent || ''),
      author_profiles_json: JSON.stringify(authorProfiles),
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


def _parse_author_profile_contacts(value: Any) -> Dict[str, Dict[str, List[str]]]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}

    result: Dict[str, Dict[str, List[str]]] = {}
    for name, payload in parsed.items():
        clean_name = _normalize_text(name)
        if not clean_name or not isinstance(payload, dict):
            continue
        result[clean_name] = {
            "emails": _split_values(str(payload.get("emails") or "")),
            "phones": _split_values(str(payload.get("phones") or "")),
            "sources": _split_values(str(payload.get("sources") or "")),
        }
    return result


def _build_author_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    title_sets: Dict[str, set[str]] = {}
    email_sets: Dict[str, set[str]] = {}
    phone_sets: Dict[str, set[str]] = {}

    for rec in records:
        names = _split_authors(str(rec.get("authors") or ""))
        if not names:
            names = _split_authors(str(rec.get("authors_raw") or ""))

        row_emails = set(_split_values(str(rec.get("author_contact_emails") or "")))
        row_phones = set(_split_values(str(rec.get("author_contact_phones") or "")))
        profile_contact_map = _parse_author_profile_contacts(rec.get("author_profile_contacts"))

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
                    "contact_emails": "",
                    "contact_phones": "",
                }
                title_sets[name] = set()
                email_sets[name] = set()
                phone_sets[name] = set()

            if title_key and title_key not in title_sets[name]:
                title_sets[name].add(title_key)
                index[name]["paper_count"] += 1

            by_profile = profile_contact_map.get(name, {})
            email_sets[name].update(by_profile.get("emails") or row_emails)
            phone_sets[name].update(by_profile.get("phones") or row_phones)

            prev_dt = _parse_publish_time(str(index[name].get("last_publish_time") or ""))
            cur_dt = _parse_publish_time(publish_time)
            if cur_dt and (prev_dt is None or cur_dt > prev_dt):
                index[name]["last_publish_time"] = publish_time

    for name, row in index.items():
        row["contact_emails"] = ";".join(sorted(email_sets.get(name) or set()))
        row["contact_phones"] = ";".join(sorted(phone_sets.get(name) or set()))

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

    keyword = _normalize_text(args.keyword) or DEFAULT_KEYWORD
    author_contact_cache: Dict[str, Dict[str, List[str]]] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headed, channel=args.channel or None)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(args.url, wait_until="domcontentloaded", timeout=args.navigation_timeout_ms)

        endpoint = ""
        captured: Dict[str, Any] = {}

        if args.load_brief_request:
            payload = json.loads(Path(args.load_brief_request).read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("load_brief_request 文件不是 JSON 对象")
        else:
            captured = await _capture_search_endpoint_and_payload(
                page,
                keyword=keyword,
                timeout_ms=args.selector_timeout_ms,
            )
            if isinstance(captured.get("payload"), dict):
                payload = captured["payload"]
            else:
                print("[capture] 未捕获到检索请求，回退使用 #briefRequest")
                payload = await _extract_brief_request_payload(page, timeout_ms=args.selector_timeout_ms)

        _remove_subject_code_conditions(payload)
        _inject_keyword_condition(payload, keyword)

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

        endpoint = _normalize_text(captured.get("endpoint") or "") or _resolve_brief_grid_url(page.url)
        print(f"[endpoint] {endpoint}")
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
                row["keyword"] = keyword
                row["query_url"] = args.url
                row["total_count"] = _to_int(meta.get("total_count_text"))
                row["cited"] = _to_int(row.get("cited"))
                row["downloads"] = _to_int(row.get("downloads"))
                await _enrich_record_author_contacts(
                    row,
                    request_context=context.request,
                    base_url=args.url,
                    referer=args.url,
                    timeout_ms=args.author_contact_timeout_ms,
                    delay_ms=args.author_contact_delay_ms,
                    cache=author_contact_cache,
                )
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
        "author_profiles_json",
        "author_profile_urls",
        "author_profile_contacts",
        "author_contact_emails",
        "author_contact_phones",
        "author_contact_sources",
        "journal",
        "publish_time",
        "cited",
        "downloads",
        "dbname",
        "filename",
        "cookie_value",
        "total_count",
        "keyword",
        "query_url",
    ]
    author_headers = [
        "author",
        "paper_count",
        "sample_title",
        "sample_journal",
        "last_publish_time",
        "contact_emails",
        "contact_phones",
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
    p.add_argument("--keyword", default=DEFAULT_KEYWORD, help="检索关键词（默认：病）")
    p.add_argument("--start-page", type=int, default=1, help="起始页码")
    p.add_argument("--max-pages", type=int, default=0, help="最大抓取页数，0=抓到末页")
    p.add_argument("--page-size", type=int, default=20, help="每页条数（通常 20/50/100）")
    p.add_argument("--delay-ms", type=int, default=120, help="分页请求间隔毫秒")
    p.add_argument("--author-contact-delay-ms", type=int, default=80, help="作者信息请求间隔毫秒")
    p.add_argument("--author-contact-timeout-ms", type=int, default=30000, help="作者信息请求超时毫秒")
    p.add_argument("--output-dir", default="output/spreadsheet", help="CSV 输出目录")
    p.add_argument("--records-csv", default="cnki_medical_records.csv", help="文献明细 CSV 文件名")
    p.add_argument("--authors-csv", default="cnki_medical_authors.csv", help="作者汇总 CSV 文件名")
    p.add_argument("--save-brief-request", default="", help="保存当前 briefRequest JSON 到文件")
    p.add_argument("--load-brief-request", default="", help="从文件加载 briefRequest JSON，跳过页面提取")
    p.add_argument("--headed", action="store_true", help="启用有头浏览器")
    p.add_argument("--channel", default="", help="浏览器 channel（可选：chrome/edge）")
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
