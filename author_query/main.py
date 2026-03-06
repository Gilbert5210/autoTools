#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import html as html_lib
import hashlib
import json
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import yaml

try:
    import ddddocr  # type: ignore
except Exception as e:  # pragma: no cover
    ddddocr = None  # type: ignore
    _ddddocr_import_error = e

try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError  # type: ignore
    from playwright.async_api import async_playwright  # type: ignore
except Exception as e:  # pragma: no cover
    async_playwright = None  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore
    _playwright_import_error = e


SCRIPT_DIR = Path(__file__).resolve().parent


def _build_ddddocr_unavailable_message(extra_error: Exception | None = None) -> str:
    """
    /* ddddocr 兼容性提示：
       - 统一收敛导入失败/初始化失败的错误信息
       - 明确给出已验证的版本区间和一键修复命令 */
    """
    details: List[str] = []
    if "_ddddocr_import_error" in globals():
        details.append(f"import_error={globals().get('_ddddocr_import_error')}")
    if extra_error is not None:
        details.append(f"init_error={extra_error}")

    try:
        installed_version = importlib_metadata.version("ddddocr")
    except importlib_metadata.PackageNotFoundError:
        installed_version = "未安装"
    except Exception:
        installed_version = "未知"

    detail_text = "; ".join(details) if details else "未知错误"
    return (
        f"ddddocr 不可用（当前环境版本: {installed_version}，详情: {detail_text}）。"
        "请安装兼容版本：`python3 -m pip install \"ddddocr>=1.5.6,<1.6.0\" --upgrade --force-reinstall`"
    )


def _now_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def _sha1_8(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]


_CHAR_INDEX_KEY_RE = re.compile(r"^\d+$")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"(?:\+?86[- ]?)?1[3-9]\d{9}")
_JOURNAL_ROOT_PATH_RE = re.compile(r"(/Journalx_[^/]+)", re.IGNORECASE)
_SEARCH_AUTHORS_ACTION_RE = re.compile(
    r"(https?://[^\s\"']*Contribution!searchAuthors\.action[^\s\"']*|/[^\s\"']*Contribution!searchAuthors\.action[^\s\"']*)",
    re.IGNORECASE,
)
_CAPTCHA_SRC_RE = re.compile(r"id=[\"']randomCodePic[\"'][^>]*src=[\"']([^\"']+)[\"']", re.IGNORECASE)


def _normalize_scalar_text(value: Any) -> str:
    """
    /* 结果单元格归一化：
       - 兼容站点返回的 String 对象序列化结果（{"0":"王","1":"强"}）
       - 统一把 dict/list/None 收敛为可读字符串，避免导出成 JSON 碎片 */
    """
    if value is None:
        text = ""
    elif isinstance(value, str):
        text = value
    elif isinstance(value, dict):
        keys = list(value.keys())
        if keys and all(isinstance(k, str) and _CHAR_INDEX_KEY_RE.match(k) for k in keys):
            chars: List[str] = []
            for k in sorted((int(k) for k in keys)):
                chars.append(str(value.get(str(k)) or ""))
            text = "".join(chars)
        else:
            try:
                text = json.dumps(value, ensure_ascii=False)
            except Exception:
                text = str(value)
    elif isinstance(value, list):
        parts = [_normalize_scalar_text(x) for x in value]
        text = " ".join(x for x in parts if x)
    else:
        text = str(value)

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_email(text: str) -> str:
    m = _EMAIL_RE.search(text or "")
    return m.group(0) if m else ""


def _extract_phone(text: str) -> str:
    m = _PHONE_RE.search(text or "")
    return m.group(0) if m else ""


def _md_escape_cell(text: Any) -> str:
    # /* 报告导出边界处理：
    #    - 单元格值可能来自 dict/list/None，先做安全字符串化
    #    - 避免直接调用 replace 导致 "'dict' object has no attribute 'replace'" */
    t = _normalize_scalar_text(text)

    # Markdown 表格单元格转义：避免竖线破坏列结构；保留换行信息。
    t = t.replace("|", "\\|")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = t.replace("\n", "<br>")
    return t.strip()


def _format_md_table(headers: List[str], rows: List[List[str]]) -> str:
    if not headers:
        return "_未检测到结果表头_"
    safe_headers = [_md_escape_cell(h) for h in headers]
    out_lines = [
        "| " + " | ".join(safe_headers) + " |",
        "| " + " | ".join(["---"] * len(safe_headers)) + " |",
    ]
    for r in rows:
        rr = list(r[: len(safe_headers)]) + [""] * max(0, len(safe_headers) - len(r))
        out_lines.append("| " + " | ".join(_md_escape_cell(x) for x in rr) + " |")
    return "\n".join(out_lines)


_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand_env_vars(value: str) -> str:
    def repl(m: re.Match[str]) -> str:
        var = m.group(1)
        if var not in os.environ:
            raise RuntimeError(f"配置引用了未设置的环境变量: {var}")
        return os.environ[var]

    return _ENV_VAR_PATTERN.sub(repl, value)


def _deep_expand(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _deep_expand(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_expand(x) for x in obj]
    if isinstance(obj, str):
        return _expand_env_vars(obj)
    return obj


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("配置文件顶层必须是 YAML map/dict")
    return _deep_expand(data)


def _resolve_path(base_dir: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()


def _detect_local_chrome_executable() -> Optional[Path]:
    """
    /* 本机 Chrome 探测：
       - 先按操作系统常见安装路径探测
       - 再通过 PATH 查找可执行命令，覆盖自定义安装场景 */
    """
    candidates: List[Path] = []

    if sys.platform == "darwin":
        candidates.extend(
            [
                Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
                Path.home() / "Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            ]
        )
    elif sys.platform.startswith("win"):
        for root in [
            os.environ.get("PROGRAMFILES"),
            os.environ.get("PROGRAMFILES(X86)"),
            os.environ.get("LOCALAPPDATA"),
        ]:
            if root:
                candidates.append(Path(root) / "Google/Chrome/Application/chrome.exe")
    else:
        candidates.extend(
            [
                Path("/usr/bin/google-chrome"),
                Path("/usr/bin/google-chrome-stable"),
                Path("/usr/bin/chromium-browser"),
                Path("/usr/bin/chromium"),
                Path("/snap/bin/chromium"),
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    for cmd in ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium", "chrome"]:
        cmd_path = shutil.which(cmd)
        if cmd_path:
            return Path(cmd_path)

    return None


async def _launch_browser_with_local_chrome_preferred(
    *,
    playwright: Any,
    headless: bool,
    browser_cfg: Dict[str, Any],
    cfg_dir: Path,
) -> Any:
    """
    /* 浏览器启动策略：
       - 优先使用本机 Chrome 内核（显式路径 > 自动探测路径 > channel=chrome）
       - 若本机内核不可用，按配置决定是否回退 Playwright 自带 Chromium */
    """
    prefer_local_chrome = bool(browser_cfg.get("prefer_local_chrome", True))
    fallback_to_playwright = bool(browser_cfg.get("fallback_to_playwright_chromium", True))
    configured_channel = str(browser_cfg.get("channel") or "").strip()
    configured_executable = str(browser_cfg.get("executable_path") or "").strip()

    launch_errors: List[str] = []

    # 显式配置 executable_path 时优先使用，便于在多浏览器环境中精准指定内核。
    if configured_executable:
        explicit_path = _resolve_path(cfg_dir, configured_executable)
        if explicit_path.exists():
            try:
                return await playwright.chromium.launch(headless=headless, executable_path=str(explicit_path))
            except Exception as e:
                launch_errors.append(f"executable_path={explicit_path}: {e}")
        else:
            launch_errors.append(f"executable_path={explicit_path}: 文件不存在")

    # 未显式指定路径时，按系统默认路径自动探测本机 Chrome。
    if prefer_local_chrome and not configured_executable:
        auto_path = _detect_local_chrome_executable()
        if auto_path is not None:
            try:
                return await playwright.chromium.launch(headless=headless, executable_path=str(auto_path))
            except Exception as e:
                launch_errors.append(f"auto_detected_executable={auto_path}: {e}")

    # 若未指定 channel，则在本机优先模式下默认尝试 channel=chrome。
    channel_to_try = configured_channel or ("chrome" if prefer_local_chrome else "")
    if channel_to_try:
        try:
            return await playwright.chromium.launch(headless=headless, channel=channel_to_try)
        except Exception as e:
            launch_errors.append(f"channel={channel_to_try}: {e}")

    if fallback_to_playwright:
        try:
            return await playwright.chromium.launch(headless=headless)
        except Exception as e:
            launch_errors.append(f"playwright_chromium: {e}")

    detail = " | ".join(launch_errors) if launch_errors else "未知错误"
    raise RuntimeError(
        "浏览器启动失败：已优先尝试本机 Chrome 内核，但均未成功。"
        f"详情：{detail}。"
        "可在 config.yaml 中设置 global.browser.executable_path 指向本机 Chrome 可执行文件，"
        "或启用 fallback_to_playwright_chromium 并执行 `python3 -m playwright install chromium`。"
    )


def _load_names(names_file: Path) -> List[str]:
    if not names_file.exists():
        raise FileNotFoundError(f"姓名列表文件不存在: {names_file}")
    out: List[str] = []
    for line in names_file.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    # 保持顺序去重
    seen = set()
    uniq: List[str] = []
    for n in out:
        if n in seen:
            continue
        seen.add(n)
        uniq.append(n)
    return uniq


def _generate_similar_names(
    base_name: str,
    enabled: bool,
    max_variants: int,
    replace_map: Dict[str, List[str]],
) -> List[str]:
    """
    /* 相似名生成：
       - 目标：对用户给定姓名做“字符级”替换，快速生成高重复/高相似的查询名。
       - 注意：该策略不会理解语义，仅做可控扰动；自动去重并严格限额，避免组合爆炸。 */
    """
    base_name = base_name.strip()
    if not base_name:
        return []
    if not enabled:
        return [base_name]
    if max_variants <= 1:
        return [base_name]

    # BFS 逐步扩展：先单字替换，再多字组合替换，直到达到 max_variants。
    seen = {base_name}
    queue = [base_name]
    idx = 0
    while idx < len(queue) and len(seen) < max_variants:
        cur = queue[idx]
        idx += 1
        chars = list(cur)
        for i, ch in enumerate(chars):
            alts = replace_map.get(ch) or []
            for alt in alts:
                if not alt or alt == ch:
                    continue
                nxt = cur[:i] + alt + cur[i + 1 :]
                if nxt in seen:
                    continue
                seen.add(nxt)
                queue.append(nxt)
                if len(seen) >= max_variants:
                    break
            if len(seen) >= max_variants:
                break
    # 保证原名排第一
    result = [base_name] + [x for x in queue[1:] if x != base_name]
    return result[:max_variants]


@dataclass
class QueryResult:
    ok: bool
    site: str
    create_url: str
    base_name: str
    query_name: str
    final_url: str
    headers: List[str]
    rows: List[List[str]]
    elapsed_ms: int
    submit_attempts: int
    captcha_attempts: int
    error: str = ""
    debug: Dict[str, Any] | None = None


@dataclass
class QueryJob:
    site: str
    create_url: str
    base_name: str
    query_name: str


@dataclass
class ApiQuerySpec:
    create_url: str
    resolved_create_url: str
    search_action_url: str
    captcha_url: str
    form_defaults: Dict[str, str]


class SiteSession:
    def __init__(
        self,
        *,
        site_cfg: Dict[str, Any],
        global_cfg: Dict[str, Any],
        context: Any,
        ocr_engine: Any,
        state_path: Path,
        debug_dir: Path,
    ) -> None:
        self.site_cfg = site_cfg
        self.global_cfg = global_cfg
        self.context = context
        self.ocr_engine = ocr_engine
        self.state_path = state_path
        self.debug_dir = debug_dir
        self._api_spec_cache: Dict[str, ApiQuerySpec] = {}

    @property
    def name(self) -> str:
        return str(self.site_cfg.get("name") or "site")

    @property
    def selectors(self) -> Dict[str, str]:
        return dict(self.site_cfg.get("selectors") or {})

    def _timeout(self, key: str, default_ms: int) -> int:
        return int((self.global_cfg.get("timeouts_ms") or {}).get(key, default_ms))

    def _log(self, message: str) -> None:
        print(f"[{self.name}] {message}")

    def _query_cfg(self) -> Dict[str, Any]:
        cfg = self.global_cfg.get("query")
        return dict(cfg) if isinstance(cfg, dict) else {}

    def _query_mode(self) -> str:
        """
        /* 查询通道选择：
           - 默认 cbkx 站点走 auto（优先接口，失败回退页面）
           - 其他站点默认维持 ui，避免对未知站点引入行为变化 */
        """
        default_mode = "auto" if self.name == "cbkx_whu" else "ui"
        site_mode = str(self.site_cfg.get("query_mode") or "").strip().lower()
        query_cfg = self._query_cfg()
        global_mode = str(query_cfg.get("mode") or self.global_cfg.get("query_mode") or "").strip().lower()
        mode = site_mode or global_mode or default_mode
        if mode in {"ui", "api", "auto"}:
            return mode
        self._log(f"未知 query_mode={mode}，回退到默认模式 {default_mode}")
        return default_mode

    def _api_http_timeout_ms(self) -> int:
        query_cfg = self._query_cfg()
        return int(query_cfg.get("http_timeout_ms") or self._timeout("navigation", 25000))

    def _resolve_abs_url(self, *, base_url: str, target_url: str) -> str:
        if not target_url:
            return ""
        return urljoin(base_url, target_url)

    def _extract_search_action_candidate(self, text: str) -> str:
        if not text:
            return ""
        m = _SEARCH_AUTHORS_ACTION_RE.search(text)
        if not m:
            return ""
        return html_lib.unescape(str(m.group(1) or "")).strip()

    def _extract_captcha_src_candidate(self, text: str) -> str:
        if not text:
            return ""
        m = _CAPTCHA_SRC_RE.search(text)
        if not m:
            return ""
        return html_lib.unescape(str(m.group(1) or "")).strip()

    def _build_captcha_request_url(self, *, captcha_url: str) -> str:
        if not captcha_url:
            return ""
        cleaned = re.sub(r"([?&])d_a_=\d+", r"\1", captcha_url)
        cleaned = re.sub(r"[?&]+$", "", cleaned)
        sep = "&" if "?" in cleaned else "?"
        return f"{cleaned}{sep}d_a_={int(time.time() * 1000)}"

    def _build_captcha_url_candidates(self, *, captcha_url: str, base_url: str) -> List[str]:
        """
        /* 验证码地址候选：
           - 优先使用页面提取值，再根据 Journalx 根路径推导兜底地址
           - 部分页面会返回 /author/kaptcha.jpg（可能 404），自动补充 /kaptcha.jpg 候选 */
        """
        candidates: List[str] = []

        def add(url: str) -> None:
            u = str(url or "").strip()
            if not u:
                return
            u = re.sub(r"([?&])d_a_=[^&]*", r"\1", u)
            u = re.sub(r"[?&]+$", "", u)
            if u not in candidates:
                candidates.append(u)

        add(captcha_url)

        refs: List[str] = [
            captcha_url,
            base_url,
            str(self.site_cfg.get("login_url") or ""),
        ]
        create_urls = self.site_cfg.get("create_urls") or []
        if isinstance(create_urls, list) and create_urls:
            refs.append(str(create_urls[0] or ""))

        for ref in refs:
            try:
                parsed = urlparse(ref)
            except Exception:
                continue
            if not parsed.scheme or not parsed.netloc:
                continue

            path = str(parsed.path or "")
            prefix = f"{parsed.scheme}://{parsed.netloc}"

            if path.endswith("/author/kaptcha.jpg"):
                add(f"{prefix}{path[: -len('/author/kaptcha.jpg')]}/kaptcha.jpg")

            m = _JOURNAL_ROOT_PATH_RE.search(path)
            if m:
                add(f"{prefix}{m.group(1)}/kaptcha.jpg")

        return candidates

    def _sanitize_response_html(self, html_text: str) -> str:
        if not html_text:
            return ""
        return re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.IGNORECASE | re.DOTALL)

    def _persist_api_response_html(
        self,
        *,
        create_url: str,
        base_name: str,
        query_name: str,
        submit_attempt: int,
        response_status: int,
        response_url: str,
        html_text: str,
    ) -> Path:
        """
        /* 接口响应 HTML 落盘：
           - 在 API 查询通道中将待解析 HTML 持久化到本地
           - 使用“确定文件名”覆盖写入，避免目录无限增长 */
        """
        dump_dir = self.debug_dir / "api_html"
        dump_dir.mkdir(parents=True, exist_ok=True)

        site_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.name).strip("_") or "site"
        key = (
            f"{site_key}_{_sha1_8(base_name)}_{_sha1_8(query_name)}_"
            f"{_sha1_8(create_url)}_try{int(submit_attempt)}"
        )
        path = dump_dir / f"{key}.html"
        path.write_text(html_text or "", encoding="utf-8")
        return path

    def _persist_api_captcha_image(
        self,
        *,
        create_url: str,
        base_name: str,
        query_name: str,
        submit_attempt: int,
        captcha_url: str,
        img_bytes: bytes,
    ) -> Path:
        """
        /* 接口验证码图片落盘：
           - 每次提交仅请求一次验证码并立即落盘
           - 使用“确定文件名”覆盖写入，避免目录无限增长 */
        """
        dump_dir = self.debug_dir / "api_captcha"
        dump_dir.mkdir(parents=True, exist_ok=True)

        site_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.name).strip("_") or "site"
        key = (
            f"{site_key}_{_sha1_8(base_name)}_{_sha1_8(query_name)}_"
            f"{_sha1_8(create_url)}_try{int(submit_attempt)}"
        )
        path = dump_dir / f"{key}.jpg"
        path.write_bytes(img_bytes or b"")
        return path

    def _looks_like_login_response(self, *, final_url: str, html_text: str) -> bool:
        if not final_url and not html_text:
            return False
        login_url = str(self.site_cfg.get("login_url") or "")
        final_url = final_url or ""
        if login_url and login_url in final_url:
            return True
        if "authorLogOn.action" in final_url or "Login.action" in final_url:
            return True

        low = (html_text or "").lower()
        return bool(
            re.search(r"id=['\"]user_name['\"]", low)
            and re.search(r"id=['\"]password['\"]", low)
        )

    def _login_success_url_hint(self) -> str:
        """
        /* 登录成功 URL 提示：
           - 优先使用配置项 sites[].login_success_url_contains
           - cbkx 默认使用 Author.action 作为登录成功强信号 */
        """
        configured = str(self.site_cfg.get("login_success_url_contains") or "").strip()
        if configured:
            return configured
        if self.name == "cbkx_whu":
            return "/author/Author.action"
        return ""

    def _result_row_marker_selector(self) -> str:
        """
        /* 结果行标记：
           - URL 不变时，不能只依赖结果表容器，需要依赖“结果行特征”定位真实结果
           - 优先读取 selectors.result_row_marker，cbkx 默认使用 sid 单选框 */
        """
        configured = str(self.selectors.get("result_row_marker") or "").strip()
        if configured:
            return configured
        if self.name == "cbkx_whu":
            return "input[type='radio'][name='sid']"
        return ""

    def _normalize_header_token(self, value: Any) -> str:
        return re.sub(r"[\s\-_:：/]+", "", _normalize_scalar_text(value).lower())

    def _is_search_authors_url(self, url: str) -> bool:
        if not url:
            return False
        if self.name == "cbkx_whu":
            return "Contribution!searchAuthors.action" in url
        return "searchAuthors.action" in url

    def _is_cbkx_target_result_table(self, table_sig: Dict[str, Any]) -> bool:
        """
        /* cbkx 目标结果表判定：
           - 必须命中 sid 单选框，且表头包含“选择作者/姓名/单位/Email”
           - 显式排除 create22 管理表关键列，防止把“点击前”表格误识别为查询结果 */
        """
        headers_raw = table_sig.get("headers") or []
        headers = [self._normalize_header_token(x) for x in headers_raw if _normalize_scalar_text(x)]
        if not headers:
            return False

        header_line = "|".join(headers)
        sid_count = int(table_sig.get("sid_count") or 0)
        comm_count = int(table_sig.get("comm_count") or 0)

        has_pick_author = any("选择作者" in h for h in headers)
        has_name = any("姓名" in h for h in headers)
        has_org = any(("单位" in h) or ("机构" in h) for h in headers)
        has_email = any(("email" in h) or ("邮箱" in h) for h in headers)
        has_create22_columns = any(x in header_line for x in ["通讯作者", "第一作者", "删除", "调整顺序"])

        return bool(
            sid_count > 0
            and comm_count == 0
            and has_pick_author
            and has_name
            and has_org
            and has_email
            and not has_create22_columns
        )

    def _is_cbkx_create22_table(self, table_sig: Dict[str, Any]) -> bool:
        headers_raw = table_sig.get("headers") or []
        headers = [self._normalize_header_token(x) for x in headers_raw if _normalize_scalar_text(x)]
        header_line = "|".join(headers)
        comm_count = int(table_sig.get("comm_count") or 0)
        has_create22_columns = any(x in header_line for x in ["通讯作者", "第一作者", "删除", "调整顺序"])
        return bool(comm_count > 0 or has_create22_columns)

    def _summarize_table_signature(self, table_sig: Dict[str, Any]) -> str:
        headers = table_sig.get("headers") or []
        header_text = "/".join(_normalize_scalar_text(h) for h in headers[:4])
        if len(headers) > 4:
            header_text = f"{header_text}/..."
        return (
            f"idx={table_sig.get('dom_index')},sources={table_sig.get('sources')},"
            f"sid={table_sig.get('sid_count')},comm={table_sig.get('comm_count')},"
            f"headers={header_text or '-'}"
        )

    def _compact_page_signature(self, page_signature: Dict[str, Any]) -> Dict[str, Any]:
        tables = page_signature.get("tables") if isinstance(page_signature.get("tables"), list) else []
        return {
            "url": str(page_signature.get("url") or ""),
            "is_search_authors_url": bool(page_signature.get("is_search_authors_url")),
            "has_skip_form": bool(page_signature.get("has_skip_form")),
            "has_none_btn": bool(page_signature.get("has_none_btn")),
            "has_cancel_btn": bool(page_signature.get("has_cancel_btn")),
            "tables": [self._summarize_table_signature(t) for t in tables if isinstance(t, dict)][:8],
        }

    async def _collect_result_page_signature(
        self,
        *,
        page: Any,
        result_marker_sel: str,
        result_table_sel: str,
        override_url: str = "",
    ) -> Dict[str, Any]:
        """
        /* 查询页结构采样：
           - 采集候选表头、sid/comm 单选框数量、来源（marker/result_selector/#Ttable）
           - 作为“是否已进入 searchAuthors 目标结果页”的判定依据，避免误抓 create22 表 */
        """
        try:
            data = await page.evaluate(
                r"""({ markerSel, tableSel }) => {
  const norm = (v) => String(v == null ? '' : v).replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
  const entryMap = new Map();

  const addTable = (table, source) => {
    if (!table) return;
    const key = table;
    const existing = entryMap.get(key);
    if (existing) {
      if (!existing.sources.includes(source)) existing.sources.push(source);
      return;
    }
    entryMap.set(key, { table, sources: [source] });
  };

  if (markerSel) {
    document.querySelectorAll(markerSel).forEach((node) => addTable(node.closest('table'), 'marker'));
  }

  if (tableSel) {
    document.querySelectorAll(tableSel).forEach((node) => addTable(node.closest('table') || node, 'result_selector'));
  }

  addTable(document.querySelector('#Ttable'), 'id_Ttable');

  if (!entryMap.size) {
    Array.from(document.querySelectorAll('table')).slice(0, 10).forEach((table) => addTable(table, 'fallback_scan'));
  }

  const allTables = Array.from(document.querySelectorAll('table'));
  const toSignature = (table, sources) => {
    const rows = Array.from(table.querySelectorAll('tr'));
    const firstRow = rows.length ? rows[0] : null;
    const headers = firstRow
      ? Array.from(firstRow.querySelectorAll('th,td')).map((cell) => norm(cell.innerText || cell.textContent || '')).filter(Boolean)
      : [];
    return {
      dom_index: allTables.indexOf(table),
      sources,
      headers,
      row_count: Math.max(0, rows.length - 1),
      sid_count: table.querySelectorAll("input[type='radio'][name='sid']").length,
      comm_count: table.querySelectorAll("input[type='radio'][name='comm']").length,
      marker_count: markerSel ? table.querySelectorAll(markerSel).length : 0,
      result_selector_match: tableSel ? table.matches(tableSel) : false
    };
  };

  const tables = Array.from(entryMap.values()).map((item) => toSignature(item.table, item.sources));

  return {
    url: location.href,
    is_search_authors_url: location.href.includes('searchAuthors.action'),
    has_skip_form: !!document.querySelector("form#frm[name='frm'][onsubmit*='next()']"),
    has_none_btn: !!document.querySelector("input.my_button[value='都不是'][onclick*='cancel()']"),
    has_cancel_btn: !!document.querySelector("input.my_button[value='取消'][onclick*='Contribution!create22.action']"),
    tables
  };
}""",
                {
                    "markerSel": result_marker_sel,
                    "tableSel": result_table_sel,
                },
            )
        except Exception as e:
            effective_url = str(override_url or page.url or "")
            return {
                "url": effective_url,
                "is_search_authors_url": self._is_search_authors_url(effective_url),
                "tables": [],
                "signature_error": str(e),
            }

        if not isinstance(data, dict):
            effective_url = str(override_url or page.url or "")
            return {
                "url": effective_url,
                "is_search_authors_url": self._is_search_authors_url(effective_url),
                "tables": [],
                "signature_error": "invalid_signature_type",
            }

        tables = data.get("tables")
        if not isinstance(tables, list):
            data["tables"] = []
        else:
            data["tables"] = [x for x in tables if isinstance(x, dict)]

        effective_url = str(override_url or data.get("url") or page.url or "")
        data["url"] = effective_url
        data["is_search_authors_url"] = self._is_search_authors_url(effective_url)
        return data

    def _select_result_table_signature(
        self,
        *,
        page_signature: Dict[str, Any],
        result_marker_sel: str,
        result_table_sel: str,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        /* 目标结果表选择：
           - cbkx 走强约束（searchAuthors URL + sid + 目标表头）
           - 其他站点保留原有 marker/selector 选择策略，兼容历史配置 */
        """
        tables = page_signature.get("tables")
        if not isinstance(tables, list) or not tables:
            return None, "no_table_candidate"

        page_url = str(page_signature.get("url") or "")
        if self.name == "cbkx_whu":
            if not self._is_search_authors_url(page_url):
                return None, "not_searchAuthors_url"
            for t in tables:
                if self._is_cbkx_target_result_table(t):
                    return t, "cbkx_target_table_matched"
            if any(self._is_cbkx_create22_table(t) for t in tables):
                return None, "hit_create22_management_table"
            return None, "searchAuthors_but_target_table_missing"

        if result_marker_sel:
            for t in tables:
                if int(t.get("marker_count") or 0) > 0:
                    return t, "marker_table_matched"

        allow_fallback_selector = bool(result_table_sel and ((not result_marker_sel) or self._is_search_authors_url(page_url)))
        if allow_fallback_selector:
            for t in tables:
                if bool(t.get("result_selector_match")):
                    return t, "result_selector_table_matched"

        return None, "no_target_table_matched"

    def _find_header_index(self, headers: List[str], aliases: List[str]) -> int:
        alias_tokens = [re.sub(r"[\s\-_:：]+", "", a.lower()) for a in aliases if a]
        for idx, h in enumerate(headers):
            token = re.sub(r"[\s\-_:：]+", "", _normalize_scalar_text(h).lower())
            if not token:
                continue
            if any(a in token for a in alias_tokens):
                return idx
        return -1

    def _normalize_author_rows(self, *, raw_headers: List[Any], raw_rows: List[Any]) -> Tuple[List[str], List[List[str]]]:
        """
        /* 作者表结构标准化：
           - 固定输出列：姓名、单位、Email、手机号码
           - 仅保留包含有效 Email 的记录，其他结果自动跳过
           - 缺失字段统一填充 '-'，提升导出可读性 */
        """
        headers = [_normalize_scalar_text(h) for h in raw_headers]

        name_idx = self._find_header_index(headers, ["姓名", "name"])
        org_idx = self._find_header_index(headers, ["单位", "机构", "学校", "affiliation", "organization"])
        email_idx = self._find_header_index(headers, ["email", "e-mail", "邮箱", "邮件"])
        phone_idx = self._find_header_index(headers, ["手机", "手机号", "mobile", "phone", "tel", "电话"])

        normalized_rows: List[List[str]] = []
        for raw in raw_rows:
            cells_raw = raw if isinstance(raw, list) else [raw]
            cells = [_normalize_scalar_text(c) for c in cells_raw]
            if not cells:
                continue

            row_text = " ".join(x for x in cells if x)

            def pick(idx: int) -> str:
                if idx < 0 or idx >= len(cells):
                    return ""
                return cells[idx]

            name = pick(name_idx)
            org = pick(org_idx)

            email = ""
            if email_idx >= 0:
                email = _extract_email(pick(email_idx))
            if not email:
                email = _extract_email(row_text)
            if not email:
                continue

            phone = ""
            if phone_idx >= 0:
                phone = _extract_phone(pick(phone_idx))
            if not phone:
                phone = _extract_phone(row_text)

            normalized_rows.append(
                [
                    name or "-",
                    org or "-",
                    email,
                    phone or "-",
                ]
            )

        return ["姓名", "单位", "Email", "手机号码"], normalized_rows

    async def ensure_login(self) -> None:
        # 通过打开一个 create_url 快速探测是否需要登录；如果已登录则直接返回。
        create_urls = self.site_cfg.get("create_urls") or []
        if not create_urls:
            raise RuntimeError(f"[{self.name}] 未配置 create_urls")
        probe_url = str(create_urls[0])
        self._log(f"登录预检查开始，probe_url={probe_url}")

        page = await self.context.new_page()
        try:
            await page.goto(probe_url, wait_until="domcontentloaded", timeout=self._timeout("navigation", 25000))
            is_login_page, reason = await self._is_login_page(page)
            self._log(f"登录预检查落地页={page.url}，is_login_page={is_login_page}，reason={reason}")
            if is_login_page:
                await self._do_login(page)
                self._log("登录预检查触发登录完成")
        finally:
            await page.close()

    async def _is_login_page(self, page: Any) -> Tuple[bool, str]:
        # 简单启发式：URL 命中 login_url 或出现用户名/密码输入框
        login_url = str(self.site_cfg.get("login_url") or "")
        page_url = page.url or ""
        login_success_hint = self._login_success_url_hint()
        # /* 登录成功强信号：命中登录后主页 URL，直接视为已登录 */
        if login_success_hint and login_success_hint in page_url:
            return False, "url_hit_login_success_hint"
        if login_url and login_url in page_url:
            return True, "url_hit_login_url"
        if "Login.action" in page_url:
            return True, "url_hit_login_action"

        author_login_anchor_sel = "a[href*='authorLogOn.action']"
        try:
            if await page.locator(author_login_anchor_sel).count() > 0:
                return True, "found_author_login_anchor"
        except Exception:
            pass

        u_sel = self.selectors.get("username")
        p_sel = self.selectors.get("password")
        if not u_sel or not p_sel:
            return False, "missing_username_or_password_selector"
        try:
            has_user = await page.locator(u_sel).count() > 0
            has_pass = await page.locator(p_sel).count() > 0
            if has_user and has_pass:
                return True, "found_username_password_input"
            return False, "query_page_or_other"
        except Exception:
            return False, "selector_check_error"

    async def _collect_login_state(self, *, page: Any, u_sel: str, p_sel: str) -> Tuple[bool, str, bool, bool]:
        """
        /* 登录态采样：
           - 汇总当前页面是否处于登录态（URL/入口特征）
           - 同时采集账号密码输入框是否仍存在，用于判断提交是否真正生效 */
        """
        is_login_page, reason = await self._is_login_page(page)
        try:
            has_user = await page.locator(u_sel).count() > 0
            has_pass = await page.locator(p_sel).count() > 0
        except Exception:
            has_user = False
            has_pass = False
        return is_login_page, reason, has_user, has_pass

    def _extract_url_query_value(self, *, url: str, key: str) -> str:
        m = re.search(rf"[?&]{re.escape(key)}=([^&#]+)", url or "")
        return m.group(1) if m else ""

    async def _collect_login_payload_state(self, *, page: Any) -> Dict[str, Any]:
        """
        /* 登录报文状态采样：
           - 采集登录前端生成的隐藏字段长度（user_id/mi1）
           - 结合 URL 与页面隐藏域 loginError，定位“真密码错误”与“提交流程错误” */
        """
        try:
            data = await page.evaluate(
                r"""() => {
  const userIdEl = document.querySelector('#user_id');
  const mi1El = document.querySelector('#mi1');
  const loginErrorEl = document.querySelector('#loginError');
  const getLen = (el) => {
    if (!el) return -1;
    const v = String(el.value || '');
    return v.length;
  };
  return {
    user_id_len: getLen(userIdEl),
    mi1_len: getLen(mi1El),
    login_error_field: loginErrorEl ? String(loginErrorEl.value || '') : ''
  };
}"""
            )
        except Exception:
            data = {}

        if not isinstance(data, dict):
            data = {}
        data["url_login_error"] = self._extract_url_query_value(url=page.url or "", key="login_error")
        return data

    async def _wait_login_transition(
        self,
        *,
        page: Any,
        u_sel: str,
        p_sel: str,
        rounds: int = 8,
        interval_ms: int = 350,
    ) -> Tuple[bool, str]:
        """
        /* 登录结果轮询：
           - 避免点击后立即判断导致的竞态误判
           - 在短时间内多次采样页面状态，确认是否已经离开登录态 */
        """
        last_detail = ""
        for idx in range(rounds):
            is_login_page, reason, has_user, has_pass = await self._collect_login_state(page=page, u_sel=u_sel, p_sel=p_sel)
            login_error_code = self._extract_url_query_value(url=page.url or "", key="login_error")
            detail = (
                f"poll={idx + 1}/{rounds},url={page.url},is_login_page={is_login_page},"
                f"reason={reason},has_user={has_user},has_pass={has_pass},login_error={login_error_code or '-'}"
            )
            last_detail = detail
            if not is_login_page:
                if reason == "url_hit_login_success_hint":
                    self._log(f"登录成功强信号命中，url={page.url}")
                return True, detail

            # /* 站点已明确给出登录错误码，提前结束轮询并输出细节 */
            if login_error_code:
                return False, detail

            # 门户页仍未进入登录后态，继续等待直到超时。
            if "Login.action" in (page.url or ""):
                await page.wait_for_timeout(interval_ms)
                continue

            # 已离开 authorLogOn 且登录表单不再可见，判定登录成功。
            if "authorLogOn.action" not in (page.url or "") and not (has_user and has_pass):
                return True, detail

            await page.wait_for_timeout(interval_ms)

        return False, last_detail

    async def _do_login(self, page: Any) -> None:
        """
        /* 登录处理：
           - 使用配置的选择器填写账号/密码并提交
           - 成功后把 storage_state 写到本地，后续复用会话减少重复登录 */
        """
        login_url = str(self.site_cfg.get("login_url") or "")
        if not login_url:
            raise RuntimeError(f"[{self.name}] 未配置 login_url")

        await page.goto(login_url, wait_until="domcontentloaded", timeout=self._timeout("navigation", 25000))
        # /* 登录页竞态处理：
        #    - 站点 onload 会执行 getCookie() 回填账号密码
        #    - 若过早 fill，可能被 onload 覆盖为空/旧值，导致看起来“未填充成功” */
        try:
            await page.wait_for_load_state("load", timeout=self._timeout("selector", 12000))
        except Exception as e:
            self._log(f"等待登录页 load 超时，继续执行填充，error={e}")
        self._log(f"开始登录，login_url={login_url}，current_url={page.url}")

        u_sel = self.selectors.get("username")
        p_sel = self.selectors.get("password")
        if not u_sel or not p_sel:
            raise RuntimeError(f"[{self.name}] selectors.username / selectors.password 未配置")

        # 确认元素存在再填充（更利于定位登录失败原因）
        await page.wait_for_selector(u_sel, timeout=self._timeout("selector", 12000))
        await page.wait_for_selector(p_sel, timeout=self._timeout("selector", 12000))

        creds = self.site_cfg.get("credentials") or {}
        username = str(creds.get("username") or "")
        password = str(creds.get("password") or "")
        if not username or not password:
            raise RuntimeError(f"[{self.name}] credentials.username / credentials.password 为空")

        async def fill_and_verify(*, selector: str, target: str, field_name: str) -> None:
            """
            /* 输入回读校验：
               - 填充后立即回读输入框值，确认未被页面脚本异步覆写
               - 最多重试 2 次，降低偶发竞态导致的登录失败 */
            """
            for attempt in range(1, 3):
                await page.fill(selector, target)
                await page.wait_for_timeout(120)
                try:
                    current = await page.eval_on_selector(selector, "el => String(el.value || '')")
                except Exception:
                    current = ""

                if str(current) == target:
                    if attempt > 1:
                        self._log(f"登录字段回读校验重试成功 field={field_name}，attempt={attempt}")
                    return

                self._log(
                    f"登录字段回读不一致 field={field_name}，attempt={attempt}，"
                    f"expect={target}，actual={current}"
                )

            raise RuntimeError(f"[{self.name}] 登录字段填充后被页面脚本覆写，field={field_name}")

        await fill_and_verify(selector=u_sel, target=username, field_name="username")
        await fill_and_verify(selector=p_sel, target=password, field_name="password")

        submit_sel = self.selectors.get("submit") or "input[type='submit'], button[type='submit']"
        # /* cbkx 登录页需要先执行 login() 生成加密字段，直接 form.submit 可能被判密码错误 */
        allow_raw_form_submit = bool(self.site_cfg.get("allow_raw_form_submit", self.name != "cbkx_whu"))

        submit_steps: List[Tuple[str, Any]] = [
            (
                "invoke_login_js",
                lambda: page.evaluate(
                    """() => {
  if (typeof login === 'function') {
    login();
    return 'login_fn_called';
  }
  const btn = document.querySelector("input[onclick*='login()'],input[value='登录'],button[onclick*='login()']");
  if (btn) {
    btn.click();
    return 'login_button_clicked';
  }
  return 'login_fn_missing';
}"""
                ),
            ),
            (
                "selector_click",
                lambda: page.locator(submit_sel).first.click(timeout=self._timeout("selector", 12000)),
            ),
            (
                "press_enter_password",
                lambda: page.press(p_sel, "Enter"),
            ),
            (
                "press_enter_username",
                lambda: page.press(u_sel, "Enter"),
            ),
        ]
        if allow_raw_form_submit:
            submit_steps.append(
                (
                    "form_submit_js",
                    lambda: page.evaluate(
                        """({ uSel, pSel }) => {
  const userEl = document.querySelector(uSel);
  const passEl = document.querySelector(pSel);
  const form = (passEl && passEl.form) || (userEl && userEl.form) || document.querySelector('form');
  if (!form) return false;
  form.submit();
  return true;
}""",
                        {"uSel": u_sel, "pSel": p_sel},
                    ),
                )
            )
        else:
            self._log("已关闭原始 form.submit 兜底（避免绕过前端 login() 逻辑）")

        submit_selector_count = await page.locator(submit_sel).count()
        self._log(f"登录提交前检查，submit_sel={submit_sel}，submit_selector_count={submit_selector_count}")

        login_ok = False
        login_detail = ""

        for method_name, submit_coro in submit_steps:
            payload_before = await self._collect_login_payload_state(page=page)
            self._log(
                "登录动作前载荷检查 "
                f"method={method_name},user_id_len={payload_before.get('user_id_len')},"
                f"mi1_len={payload_before.get('mi1_len')},"
                f"login_error_field={payload_before.get('login_error_field') or '-'},"
                f"url_login_error={payload_before.get('url_login_error') or '-'}"
            )

            try:
                result = await submit_coro()
                self._log(f"已执行登录提交动作 method={method_name}，result={result}")
            except Exception as e:
                self._log(f"登录提交动作异常 method={method_name}，error={e}")
                continue

            # 提交后先等待页面负载变化，再做状态轮询。
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass

            payload_after = await self._collect_login_payload_state(page=page)
            self._log(
                "登录动作后载荷检查 "
                f"method={method_name},url={page.url},user_id_len={payload_after.get('user_id_len')},"
                f"mi1_len={payload_after.get('mi1_len')},"
                f"login_error_field={payload_after.get('login_error_field') or '-'},"
                f"url_login_error={payload_after.get('url_login_error') or '-'}"
            )

            login_ok, login_detail = await self._wait_login_transition(page=page, u_sel=u_sel, p_sel=p_sel)
            self._log(f"登录提交后检查 method={method_name}，login_ok={login_ok}，detail={login_detail}")
            if login_ok:
                break

        if not login_ok:
            await self._save_debug_artifacts(
                page,
                base_name="__login__",
                query_name="__login__",
                create_url=login_url,
                reason="login_failed",
            )
            login_error_code = self._extract_url_query_value(url=page.url or "", key="login_error")
            if login_error_code == "user_psw_error":
                raise RuntimeError(
                    f"[{self.name}] 登录失败：站点返回 user_psw_error。"
                    "请先人工确认账号密码；若人工可登录，请检查前端 login() 是否被站点策略拦截。"
                )
            raise RuntimeError(f"[{self.name}] 登录疑似失败：{login_detail or f'仍停留在登录页 {page.url}'}")

        # 持久化会话
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        await self.context.storage_state(path=str(self.state_path))
        self._api_spec_cache.clear()
        self._log(f"登录成功并持久化会话，state_path={self.state_path}")

    async def _collect_search_authors_skip_flags(self, *, page: Any) -> Dict[str, bool]:
        """
        /* searchAuthors 跳过页识别：
           - 命中 form#frm 且 onsubmit=next()，表示进入“输入本文作者信息”分支页
           - 若出现“都不是”按钮，说明站点允许继续人工判定，此时不自动取消
           - 仅当存在“取消”按钮且不存在“都不是”按钮时，才触发自动取消 */
        """
        form_sel = "form#frm[name='frm'][onsubmit*='next()']"
        none_btn_sel = "input.my_button[value='都不是'][onclick*='cancel()']"
        cancel_btn_sel = "input.my_button[value='取消'][onclick*='Contribution!create22.action']"

        try:
            has_form = await page.locator(form_sel).count() > 0
        except Exception:
            has_form = False

        try:
            has_none_btn = await page.locator(none_btn_sel).count() > 0
        except Exception:
            has_none_btn = False

        try:
            has_cancel_btn = await page.locator(cancel_btn_sel).count() > 0
        except Exception:
            has_cancel_btn = False

        return {
            "has_form": has_form,
            "has_none_btn": has_none_btn,
            "has_cancel_btn": has_cancel_btn,
            "should_cancel": bool(has_form and has_cancel_btn and not has_none_btn),
        }

    async def _cancel_search_authors_skip_page(
        self,
        *,
        page: Any,
        wait_rounds: int = 3,
        interval_ms: int = 350,
        verbose: bool = True,
    ) -> Tuple[bool, str]:
        """
        /* searchAuthors 自动取消：
           - 点击“添加作者”后页面可能异步切换，短轮询等待目标结构出现
           - 命中跳过条件后点击“取消”，将流程拉回 create22，避免记为失败 */
        """
        cancel_btn_sel = "input.my_button[value='取消'][onclick*='Contribution!create22.action']"

        for idx in range(max(1, int(wait_rounds))):
            flags = await self._collect_search_authors_skip_flags(page=page)
            if verbose:
                self._log(
                    "searchAuthors跳过页检查 "
                    f"poll={idx + 1}/{max(1, int(wait_rounds))},url={page.url},"
                    f"has_form={flags.get('has_form')},has_none_btn={flags.get('has_none_btn')},"
                    f"has_cancel_btn={flags.get('has_cancel_btn')},should_cancel={flags.get('should_cancel')}"
                )

            if flags.get("should_cancel"):
                before_url = page.url or ""
                try:
                    await page.click(cancel_btn_sel, timeout=self._timeout("selector", 12000))
                except Exception as e:
                    if verbose:
                        self._log(f"searchAuthors跳过页点击取消失败，url={before_url}，error={e}")
                    return False, ""

                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=self._timeout("navigation", 12000))
                except Exception:
                    pass

                after_url = page.url or ""
                reason = "命中searchAuthors补录页（含frm且无都不是），已点击取消跳过"
                if verbose:
                    self._log(f"searchAuthors跳过成功，before_url={before_url}，after_url={after_url}")
                return True, reason

            if idx < max(1, int(wait_rounds)) - 1:
                await page.wait_for_timeout(interval_ms)

        return False, ""

    async def _wait_query_outcome(
        self,
        *,
        page: Any,
        dialog_messages: List[str],
        result_marker_sel: str,
        result_table_sel: str,
        timeout_ms: int,
        poll_interval_ms: int,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        /* 查询结果快速判定：
           - 轮询并行观察验证码弹窗、searchAuthors 跳过页、目标结果表签名
           - 仅在命中目标结果表后返回 result_ready，避免把 create22 管理表误当查询结果 */
        """
        deadline = time.monotonic() + max(500, int(timeout_ms)) / 1000.0
        poll_interval = max(120, int(poll_interval_ms))
        last_signature: Dict[str, Any] = {}
        last_reason = ""

        while True:
            # 站点验证码错误通常会通过 alert 给出，优先快速返回重试。
            if dialog_messages and any("验证码" in str(msg) for msg in dialog_messages):
                return "captcha_error", "验证码错误（站点提示）", {}

            skipped, skip_reason = await self._cancel_search_authors_skip_page(
                page=page,
                wait_rounds=1,
                interval_ms=poll_interval,
                verbose=False,
            )
            if skipped:
                return "skip", skip_reason, {}

            page_signature = await self._collect_result_page_signature(
                page=page,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )
            if isinstance(page_signature, dict):
                last_signature = page_signature

            selected_table_signature, select_reason = self._select_result_table_signature(
                page_signature=page_signature,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )
            last_reason = select_reason or last_reason

            if selected_table_signature is not None:
                compact = self._compact_page_signature(page_signature)
                compact["selected_table_signature"] = self._summarize_table_signature(selected_table_signature)
                return "result_ready", select_reason or "target_table_ready", compact

            if time.monotonic() >= deadline:
                compact = self._compact_page_signature(last_signature)
                is_search_authors = bool(last_signature.get("is_search_authors_url"))
                if is_search_authors:
                    return "result_not_target", last_reason or "searchAuthors目标结果表缺失", compact
                return "stale_page", last_reason or "提交后仍未进入searchAuthors", compact

            await page.wait_for_timeout(poll_interval)

    async def _collect_api_query_spec(self, *, page: Any, create_url: str) -> Tuple[Optional[ApiQuerySpec], str]:
        """
        /* 接口入口采集：
           - 从 create22 页面抽取 searchAuthors 动作地址、验证码地址和默认表单字段
           - 采集失败时返回原因，供 auto 模式回退到 UI 流程 */
        """
        cached = self._api_spec_cache.get(create_url)
        if cached is not None and cached.search_action_url and cached.captcha_url:
            return cached, ""

        try:
            await page.goto(create_url, wait_until="domcontentloaded", timeout=self._timeout("navigation", 25000))
        except PlaywrightTimeoutError:
            return None, "接口模式打开查询页超时"

        is_login_page, login_reason = await self._is_login_page(page)
        if is_login_page:
            self._log(f"接口模式检测到未登录，reason={login_reason}，url={page.url}")
            await self._do_login(page)
            try:
                await page.goto(create_url, wait_until="domcontentloaded", timeout=self._timeout("navigation", 25000))
            except PlaywrightTimeoutError:
                return None, "接口模式登录后重新打开查询页超时"

        try:
            data = await page.evaluate(
                r"""() => {
  const formDefaults = {};
  const nodes = Array.from(document.querySelectorAll('input[name],select[name],textarea[name]'));
  for (const node of nodes) {
    if (!node || node.disabled || !node.name) continue;
    const tag = String(node.tagName || '').toLowerCase();
    let value = '';

    if (tag === 'select') {
      value = String(node.value || '');
    } else if (tag === 'textarea') {
      value = String(node.value || '');
    } else {
      const type = String(node.type || '').toLowerCase();
      if (type === 'checkbox' || type === 'radio') {
        if (!node.checked) continue;
        value = String(node.value || 'on');
      } else if (type === 'submit' || type === 'button' || type === 'file') {
        continue;
      } else {
        value = String(node.value || '');
      }
    }

    if (!(node.name in formDefaults)) {
      formDefaults[node.name] = value;
    }
  }

  let checkAction = '';
  try {
    if (typeof check === 'function') {
      const checkText = String(check);
      const m = checkText.match(/Contribution!searchAuthors\.action[^"'\s]*/);
      if (m && m[0]) checkAction = m[0];
    }
  } catch (e) {}

  let scriptAction = '';
  if (!checkAction) {
    const scripts = Array.from(document.querySelectorAll('script')).map((s) => String(s.textContent || ''));
    for (const txt of scripts) {
      const m = txt.match(/Contribution!searchAuthors\.action[^"'\s]*/);
      if (m && m[0]) {
        scriptAction = m[0];
        break;
      }
    }
  }

  const captchaEl = document.querySelector('#randomCodePic');
  const captchaSrc = captchaEl ? String(captchaEl.src || captchaEl.getAttribute('src') || '') : '';

  return {
    page_url: String(location.href || ''),
    check_action: checkAction,
    script_action: scriptAction,
    captcha_src: captchaSrc,
    form_defaults: formDefaults,
  };
}"""
            )
        except Exception as e:
            return None, f"接口模式采集页面结构失败：{e}"

        if not isinstance(data, dict):
            return None, "接口模式采集结果类型异常"

        page_url = str(data.get("page_url") or page.url or create_url)
        search_action_raw = str(data.get("check_action") or data.get("script_action") or "").strip()
        captcha_src_raw = str(data.get("captcha_src") or "").strip()

        page_html = ""
        if not search_action_raw or not captcha_src_raw:
            try:
                page_html = await page.content()
            except Exception:
                page_html = ""

        if not search_action_raw:
            search_action_raw = self._extract_search_action_candidate(page_html)
        if not captcha_src_raw:
            captcha_src_raw = self._extract_captcha_src_candidate(page_html)

        if not search_action_raw:
            return None, "未提取到 searchAuthors 接口地址"

        search_action_url = self._resolve_abs_url(base_url=page_url, target_url=search_action_raw)
        captcha_url = self._resolve_abs_url(
            base_url=page_url,
            target_url=captcha_src_raw or "/Journalx_cbkx/kaptcha.jpg",
        )

        defaults_raw = data.get("form_defaults")
        form_defaults: Dict[str, str] = {}
        if isinstance(defaults_raw, dict):
            for k, v in defaults_raw.items():
                if not isinstance(k, str) or not k:
                    continue
                form_defaults[k] = "" if v is None else str(v)

        form_defaults.setdefault("personSearch.rolsesp", "3")
        form_defaults.setdefault("personSearch.email", "")

        spec = ApiQuerySpec(
            create_url=create_url,
            resolved_create_url=page_url,
            search_action_url=search_action_url,
            captcha_url=captcha_url,
            form_defaults=form_defaults,
        )
        self._api_spec_cache[create_url] = spec
        self._log(
            "接口地址识别完成 "
            f"create={spec.resolved_create_url},captcha={spec.captcha_url},search={spec.search_action_url}"
        )
        return spec, ""

    def _refresh_api_query_spec_from_response(
        self,
        *,
        spec: ApiQuerySpec,
        response_url: str,
        response_html: str,
    ) -> None:
        """
        /* 接口地址动态刷新：
           - 部分页面会在响应脚本中刷新 id/processId
           - 每次响应后尝试更新 search/captcha 地址，减少后续失配 */
        """
        base_url = response_url or spec.resolved_create_url or spec.create_url

        action_raw = self._extract_search_action_candidate(response_html)
        if action_raw:
            spec.search_action_url = self._resolve_abs_url(base_url=base_url, target_url=action_raw)

        captcha_raw = self._extract_captcha_src_candidate(response_html)
        if captcha_raw:
            spec.captcha_url = self._resolve_abs_url(base_url=base_url, target_url=captcha_raw)

    async def _solve_captcha_digits_via_api(
        self,
        *,
        spec: ApiQuerySpec,
        create_url: str,
        base_name: str,
        query_name: str,
        submit_attempt: int,
        expected_digits: int,
        ocr_max_attempts: int,
        refresh_delay_ms: int,
    ) -> Tuple[str, int, str]:
        """
        /* 接口验证码识别：
           - 每次提交仅请求一次 kaptcha 图片并落盘
           - OCR 仅针对本次图片识别，避免重复请求触发验证码刷新 */
        """
        if ddddocr is None or self.ocr_engine is None:
            raise RuntimeError(_build_ddddocr_unavailable_message())

        req_ctx = getattr(self.context, "request", None)
        if req_ctx is None:
            raise RuntimeError("当前 BrowserContext 不支持 request 接口")

        captcha_candidates = self._build_captcha_url_candidates(
            captcha_url=spec.captcha_url,
            base_url=spec.resolved_create_url or spec.create_url,
        )
        if not captcha_candidates:
            raise RuntimeError("验证码接口地址为空")

        img_bytes: bytes | None = None
        used_captcha_url = ""
        last_req_error = ""
        for candidate_url in captcha_candidates:
            captcha_req_url = self._build_captcha_request_url(captcha_url=candidate_url)
            if not captcha_req_url:
                continue

            try:
                resp = await req_ctx.get(
                    captcha_req_url,
                    timeout=self._api_http_timeout_ms(),
                    headers={"Referer": spec.resolved_create_url or spec.create_url},
                )
                status = int(resp.status)
                if status < 200 or status >= 300:
                    last_req_error = f"{candidate_url} -> captcha_status={status}"
                    continue
                body = await resp.body()
                if not body:
                    last_req_error = f"{candidate_url} -> empty_body"
                    continue
                img_bytes = body
                used_captcha_url = candidate_url
                break
            except Exception as e:
                last_req_error = f"{candidate_url} -> {e}"
                continue

        if img_bytes is None:
            raise RuntimeError(f"验证码接口请求失败：{last_req_error or 'all_candidates_failed'}")

        captcha_img_path = ""
        try:
            captcha_img_file = self._persist_api_captcha_image(
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
                submit_attempt=submit_attempt,
                captcha_url=used_captcha_url or spec.captcha_url,
                img_bytes=img_bytes,
            )
            captcha_img_path = str(captcha_img_file)
            img_bytes = captcha_img_file.read_bytes()
        except Exception as e:
            self._log(f"验证码图片落盘失败，使用内存字节继续识别，error={e}")

        max_ocr_attempts = max(1, int(ocr_max_attempts))
        for attempt in range(1, max_ocr_attempts + 1):
            raw = await asyncio.to_thread(self.ocr_engine.classification, img_bytes)
            digits = re.sub(r"\D", "", str(raw or ""))
            if digits and (expected_digits <= 0 or len(digits) == expected_digits):
                if used_captcha_url and used_captcha_url != spec.captcha_url:
                    self._log(f"验证码接口候选命中，from={spec.captcha_url}，selected={used_captcha_url}")
                    spec.captcha_url = used_captcha_url
                return digits, attempt, captcha_img_path

            if attempt < max_ocr_attempts:
                await asyncio.sleep(max(0, refresh_delay_ms) / 1000.0)

        raise RuntimeError("接口验证码识别失败（本轮验证码仅请求一次）")

    async def _query_author_via_api(
        self,
        *,
        page: Any,
        create_url: str,
        base_name: str,
        query_name: str,
    ) -> Tuple[Optional[QueryResult], str]:
        """
        /* 接口优先查询：
           - 复用登录态后直接请求 searchAuthors，减少页面跳转和 DOM 交互
           - 若接口入口无法识别，返回 None 让上层回退 UI 查询 */
        """
        req_ctx = getattr(self.context, "request", None)
        if req_ctx is None:
            return None, "BrowserContext 不支持 request 接口"

        captcha_cfg = dict((self.global_cfg.get("captcha") or {}))
        expected_digits = int(captcha_cfg.get("expected_digits") or 0)
        ocr_max_attempts = int(captcha_cfg.get("ocr_max_attempts") or 6)
        submit_max_attempts = int(captcha_cfg.get("submit_max_attempts") or 4)
        refresh_delay_ms = int(captcha_cfg.get("refresh_delay_ms") or 250)

        sel = self.selectors
        result_table_sel = sel.get("result_table") or "table.list"
        result_marker_sel = self._result_row_marker_selector()

        t0 = time.monotonic()
        submit_attempts = 0
        captcha_attempts_total = 0
        last_error = ""
        last_response_url = ""

        debug: Dict[str, Any] = {
            "result_page_verified": False,
            "query_channel": "api",
        }

        def build_skip_result(skip_reason: str) -> QueryResult:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            debug["skip_reason"] = skip_reason
            debug["final_url"] = last_response_url
            return QueryResult(
                ok=True,
                site=self.name,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
                final_url=last_response_url,
                headers=[],
                rows=[],
                elapsed_ms=elapsed_ms,
                submit_attempts=submit_attempts,
                captcha_attempts=captcha_attempts_total,
                debug=debug,
            )

        for submit_attempt in range(1, submit_max_attempts + 1):
            submit_attempts = submit_attempt
            debug["submit_attempt"] = submit_attempt

            spec, spec_err = await self._collect_api_query_spec(page=page, create_url=create_url)
            if spec is None:
                return None, spec_err or "接口入口采集失败"

            debug["api_search_action_url"] = spec.search_action_url
            debug["api_captcha_url"] = spec.captcha_url

            try:
                captcha_text, captcha_attempts, captcha_img_path = await self._solve_captcha_digits_via_api(
                    spec=spec,
                    create_url=create_url,
                    base_name=base_name,
                    query_name=query_name,
                    submit_attempt=submit_attempt,
                    expected_digits=expected_digits,
                    ocr_max_attempts=ocr_max_attempts,
                    refresh_delay_ms=refresh_delay_ms,
                )
                if captcha_img_path:
                    debug["api_captcha_image_path"] = captcha_img_path
            except Exception as e:
                last_error = f"接口验证码识别失败：{e}"
                continue

            captcha_attempts_total += captcha_attempts

            payload = dict(spec.form_defaults)
            payload["personSearch.name"] = query_name
            payload["personSearch.email"] = str(payload.get("personSearch.email") or "")
            payload["personSearch.rolsesp"] = str(payload.get("personSearch.rolsesp") or "3")
            payload["randomCode"] = captcha_text

            try:
                response = await req_ctx.post(
                    spec.search_action_url,
                    form=payload,
                    timeout=self._api_http_timeout_ms(),
                    headers={"Referer": spec.resolved_create_url or spec.create_url},
                )
            except Exception as e:
                last_error = f"接口提交失败：{e}"
                self._api_spec_cache.pop(create_url, None)
                continue

            response_status = int(response.status)
            response_url = str(response.url or "")
            last_response_url = response_url
            debug["api_response_status"] = response_status
            debug["post_url"] = response_url

            try:
                response_text = await response.text()
            except Exception as e:
                last_error = f"读取接口响应失败：{e}"
                continue

            self._refresh_api_query_spec_from_response(
                spec=spec,
                response_url=response_url,
                response_html=response_text,
            )

            if self._looks_like_login_response(final_url=response_url, html_text=response_text):
                self._api_spec_cache.pop(create_url, None)
                self._log(f"接口响应疑似回到登录页，触发重新登录，url={response_url}")
                await self._do_login(page)
                continue

            sanitized_html = self._sanitize_response_html(response_text)
            html_for_parse = sanitized_html or "<html><body></body></html>"
            try:
                html_file = self._persist_api_response_html(
                    create_url=create_url,
                    base_name=base_name,
                    query_name=query_name,
                    submit_attempt=submit_attempt,
                    response_status=response_status,
                    response_url=response_url,
                    html_text=html_for_parse,
                )
                debug["api_response_html_path"] = str(html_file)
                html_for_parse = html_file.read_text(encoding="utf-8")
            except Exception as e:
                debug["api_response_html_error"] = str(e)

            try:
                await page.set_content(html_for_parse, wait_until="domcontentloaded")
            except Exception as e:
                last_error = f"接口响应解析失败：{e}"
                continue

            skip_flags = await self._collect_search_authors_skip_flags(page=page)
            debug["skip_flags"] = skip_flags
            if skip_flags.get("should_cancel"):
                return build_skip_result("命中searchAuthors补录页（接口判定）"), ""

            page_signature = await self._collect_result_page_signature(
                page=page,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
                override_url=response_url,
            )
            selected_table_signature, selected_reason = self._select_result_table_signature(
                page_signature=page_signature,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )

            debug["post_table_signature"] = self._compact_page_signature(page_signature)
            debug["selected_table_signature"] = (
                self._summarize_table_signature(selected_table_signature)
                if selected_table_signature is not None
                else ""
            )
            debug["selected_table_reason"] = selected_reason
            debug["result_page_verified"] = bool(selected_table_signature is not None)

            if selected_table_signature is None:
                if selected_reason == "hit_create22_management_table":
                    return build_skip_result("命中create22管理表，未产出可用作者结果"), ""
                if self._is_search_authors_url(response_url):
                    last_error = f"已进入searchAuthors但未命中目标结果表：{selected_reason or 'unknown'}"
                else:
                    last_error = f"接口返回非目标页：{selected_reason or 'unknown'}"
                continue

            raw_headers, raw_rows = await self._extract_table_from_signature(
                page=page,
                table_sig=selected_table_signature,
            )
            headers, rows = self._normalize_author_rows(raw_headers=raw_headers, raw_rows=raw_rows)
            if not rows:
                return build_skip_result("结果表中无有效Email，已按规则跳过"), ""

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            return QueryResult(
                ok=True,
                site=self.name,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
                final_url=response_url,
                headers=headers,
                rows=rows,
                elapsed_ms=elapsed_ms,
                submit_attempts=submit_attempts,
                captcha_attempts=captcha_attempts_total,
                debug=debug,
            ), ""

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        debug["last_error"] = last_error
        debug["final_url"] = last_response_url or page.url or ""
        if captcha_attempts_total <= 0 and "验证码接口请求失败" in (last_error or ""):
            return None, last_error or "接口验证码地址不可用"
        return QueryResult(
            ok=False,
            site=self.name,
            create_url=create_url,
            base_name=base_name,
            query_name=query_name,
            final_url=last_response_url or page.url or "",
            headers=[],
            rows=[],
            elapsed_ms=elapsed_ms,
            submit_attempts=submit_attempts,
            captcha_attempts=captcha_attempts_total,
            error=last_error or "未知错误",
            debug=debug,
        ), ""

    async def query_author_on_page(
        self,
        *,
        page: Any,
        create_url: str,
        base_name: str,
        query_name: str,
    ) -> QueryResult:
        query_mode = self._query_mode()
        if query_mode in {"api", "auto"}:
            api_result, api_fallback_reason = await self._query_author_via_api(
                page=page,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
            )
            if api_result is not None:
                return api_result

            if query_mode == "api":
                return QueryResult(
                    ok=False,
                    site=self.name,
                    create_url=create_url,
                    base_name=base_name,
                    query_name=query_name,
                    final_url=page.url or "",
                    headers=[],
                    rows=[],
                    elapsed_ms=0,
                    submit_attempts=0,
                    captcha_attempts=0,
                    error=api_fallback_reason or "接口模式不可用",
                    debug={"query_channel": "api"},
                )

            if api_fallback_reason:
                self._log(f"接口模式不可用，已回退页面模式，reason={api_fallback_reason}")

        captcha_cfg = dict((self.global_cfg.get("captcha") or {}))
        expected_digits = int(captcha_cfg.get("expected_digits") or 0)
        ocr_max_attempts = int(captcha_cfg.get("ocr_max_attempts") or 6)
        submit_max_attempts = int(captcha_cfg.get("submit_max_attempts") or 4)
        refresh_delay_ms = int(captcha_cfg.get("refresh_delay_ms") or 250)
        post_submit_poll_interval_ms = int(captcha_cfg.get("post_submit_poll_interval_ms") or 350)
        post_submit_timeout_ms = int(captcha_cfg.get("post_submit_timeout_ms") or self._timeout("result", 20000))

        sel = self.selectors
        name_sel = sel.get("name_input")
        captcha_input_sel = sel.get("captcha_input")
        captcha_img_sel = sel.get("captcha_image")
        search_btn_sel = sel.get("search_button")
        result_table_sel = sel.get("result_table") or "table.list"
        result_marker_sel = self._result_row_marker_selector()
        if not all([name_sel, captcha_input_sel, captcha_img_sel, search_btn_sel]):
            raise RuntimeError(f"[{self.name}] selectors 未完整配置（需要 name_input/captcha_input/captcha_image/search_button）")

        t0 = time.monotonic()
        submit_attempts = 0
        captcha_attempts_total = 0
        last_error = ""

        debug: Dict[str, Any] = {"result_page_verified": False}
        # 捕获 alert/confirm 对话框（常用于验证码错误提示）
        dialog_messages: List[str] = []

        async def on_dialog(d: Any) -> None:
            dialog_messages.append(str(d.message))
            await d.dismiss()

        page.on("dialog", on_dialog)

        def detach_dialog_listener() -> None:
            try:
                page.remove_listener("dialog", on_dialog)
            except Exception:
                try:
                    page.off("dialog", on_dialog)
                except Exception:
                    pass

        def build_skip_result(skip_reason: str) -> QueryResult:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            debug["skip_reason"] = skip_reason
            debug["final_url"] = page.url or ""
            return QueryResult(
                ok=True,
                site=self.name,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
                final_url=page.url or "",
                headers=[],
                rows=[],
                elapsed_ms=elapsed_ms,
                submit_attempts=submit_attempts,
                captcha_attempts=captcha_attempts_total,
                debug=debug,
            )

        for submit_attempt in range(1, submit_max_attempts + 1):
            submit_attempts = submit_attempt
            dialog_messages.clear()
            debug["submit_attempt"] = submit_attempt

            try:
                await page.goto(create_url, wait_until="domcontentloaded", timeout=self._timeout("navigation", 25000))
                self._log(
                    f"查询尝试 submit_attempt={submit_attempt}，base_name={base_name}，query_name={query_name}，url={page.url}"
                )
            except PlaywrightTimeoutError:
                last_error = "打开查询页超时"
                continue

            # 若被重定向到登录页，则先登录再重试
            is_login_page, login_reason = await self._is_login_page(page)
            if is_login_page:
                self._log(
                    f"查询页识别到未登录，reason={login_reason}，final_url={page.url}，submit_attempt={submit_attempt}"
                )
                await self._do_login(page)
                continue

            try:
                await page.wait_for_selector(name_sel, timeout=self._timeout("selector", 12000))
                await page.wait_for_selector(captcha_input_sel, timeout=self._timeout("selector", 12000))
                await page.wait_for_selector(captcha_img_sel, timeout=self._timeout("selector", 12000))
            except PlaywrightTimeoutError:
                last_error = "页面缺少必要输入框/验证码"
                self._log(
                    f"查询页元素缺失，name_sel={name_sel}，captcha_input_sel={captcha_input_sel}，captcha_img_sel={captcha_img_sel}，final_url={page.url}"
                )
                continue

            pre_signature = await self._collect_result_page_signature(
                page=page,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )
            debug["pre_url"] = str(pre_signature.get("url") or page.url or "")
            debug["pre_table_signature"] = self._compact_page_signature(pre_signature)

            await page.fill(name_sel, query_name)

            # 识别验证码（数字验证码）
            captcha_text, captcha_attempts = await self._solve_captcha_digits(
                page=page,
                captcha_img_sel=captcha_img_sel,
                expected_digits=expected_digits,
                ocr_max_attempts=ocr_max_attempts,
                refresh_delay_ms=refresh_delay_ms,
            )
            captcha_attempts_total += captcha_attempts
            await page.fill(captcha_input_sel, captcha_text)

            # 提交查询
            try:
                await page.click(search_btn_sel, timeout=self._timeout("selector", 12000))
            except PlaywrightTimeoutError:
                last_error = "点击查询按钮超时"
                continue

            skipped, skip_reason = await self._cancel_search_authors_skip_page(page=page)
            if skipped:
                detach_dialog_listener()
                return build_skip_result(skip_reason)

            # 结果页可能不跳转 URL，而是局部刷新；必须命中目标结果页签名才允许提取。
            outcome, outcome_detail, outcome_signature = await self._wait_query_outcome(
                page=page,
                dialog_messages=dialog_messages,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
                timeout_ms=post_submit_timeout_ms,
                poll_interval_ms=post_submit_poll_interval_ms,
            )

            if outcome_signature:
                debug["post_url"] = str(outcome_signature.get("url") or page.url or "")
                debug["post_table_signature"] = outcome_signature
            else:
                debug["post_url"] = page.url or ""

            if outcome == "skip":
                detach_dialog_listener()
                return build_skip_result(outcome_detail or "命中searchAuthors补录页（快速判定）")

            if outcome == "captcha_error":
                last_error = outcome_detail or "验证码错误（站点提示）"
                continue

            if outcome == "stale_page":
                last_error = f"提交后仍停留在非目标页：{outcome_detail or 'stale_page'}"
                continue

            if outcome == "result_not_target":
                if outcome_detail == "hit_create22_management_table":
                    detach_dialog_listener()
                    return build_skip_result("命中create22管理表，未产出可用作者结果")
                last_error = f"已进入searchAuthors但未命中目标结果表：{outcome_detail or 'result_not_target'}"
                continue

            if outcome == "timeout":
                last_error = "等待结果表格超时/未找到结果表格"
                continue

            final_signature = await self._collect_result_page_signature(
                page=page,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )
            selected_table_signature, selected_reason = self._select_result_table_signature(
                page_signature=final_signature,
                result_marker_sel=result_marker_sel,
                result_table_sel=result_table_sel,
            )
            debug["post_url"] = str(final_signature.get("url") or page.url or "")
            debug["post_table_signature"] = self._compact_page_signature(final_signature)
            debug["selected_table_signature"] = (
                self._summarize_table_signature(selected_table_signature)
                if selected_table_signature is not None
                else ""
            )
            debug["selected_table_reason"] = selected_reason
            debug["result_page_verified"] = bool(selected_table_signature is not None)

            if selected_table_signature is None:
                if selected_reason == "hit_create22_management_table":
                    detach_dialog_listener()
                    return build_skip_result("命中create22管理表，未产出可用作者结果")
                last_error = f"结果页二次校验未命中目标表：{selected_reason or 'unknown'}"
                continue

            raw_headers, raw_rows = await self._extract_table_from_signature(
                page=page,
                table_sig=selected_table_signature,
            )

            headers, rows = self._normalize_author_rows(raw_headers=raw_headers, raw_rows=raw_rows)
            if not rows:
                detach_dialog_listener()
                return build_skip_result("结果表中无有效Email，已按规则跳过")

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            detach_dialog_listener()
            return QueryResult(
                ok=True,
                site=self.name,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
                final_url=page.url or "",
                headers=headers,
                rows=rows,
                elapsed_ms=elapsed_ms,
                submit_attempts=submit_attempts,
                captcha_attempts=captcha_attempts_total,
                debug=debug,
            )

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        debug["last_error"] = last_error
        debug["final_url"] = page.url or ""
        await self._save_debug_artifacts(page, base_name, query_name, create_url, reason="query_failed")
        detach_dialog_listener()
        return QueryResult(
            ok=False,
            site=self.name,
            create_url=create_url,
            base_name=base_name,
            query_name=query_name,
            final_url=page.url or "",
            headers=[],
            rows=[],
            elapsed_ms=elapsed_ms,
            submit_attempts=submit_attempts,
            captcha_attempts=captcha_attempts_total,
            error=last_error or "未知错误",
            debug=debug,
        )

    async def query_author(self, *, create_url: str, base_name: str, query_name: str) -> QueryResult:
        page = await self.context.new_page()
        try:
            return await self.query_author_on_page(
                page=page,
                create_url=create_url,
                base_name=base_name,
                query_name=query_name,
            )
        finally:
            await page.close()

    async def _solve_captcha_digits(
        self,
        *,
        page: Any,
        captcha_img_sel: str,
        expected_digits: int,
        ocr_max_attempts: int,
        refresh_delay_ms: int,
    ) -> Tuple[str, int]:
        """
        /* 验证码识别：
           - 站点验证码通常是数字；使用 OCR 读取后仅保留数字
           - 若长度不符合预期，点击验证码图片刷新并重试 */
        """
        if ddddocr is None or self.ocr_engine is None:
            raise RuntimeError(_build_ddddocr_unavailable_message())

        locator = page.locator(captcha_img_sel)
        await locator.wait_for(timeout=self._timeout("selector", 12000))

        for attempt in range(1, ocr_max_attempts + 1):
            img_bytes = await locator.screenshot(type="png")
            raw = await asyncio.to_thread(self.ocr_engine.classification, img_bytes)
            digits = re.sub(r"\D", "", str(raw or ""))
            if digits and (expected_digits <= 0 or len(digits) == expected_digits):
                return digits, attempt

            # 刷新验证码（通常点击图片就会刷新）
            try:
                await locator.click()
            except Exception:
                pass
            await page.wait_for_timeout(refresh_delay_ms)

        raise RuntimeError("验证码识别失败（超过最大重试次数）")

    async def _extract_table_by_marker(
        self,
        *,
        page: Any,
        marker_sel: str,
        fallback_table_sel: str = "",
    ) -> Tuple[List[str], List[List[str]]]:
        """
        /* 结果表提取（基于标记）：
           - 先复用页面签名策略选中目标 table，再按 dom_index 精确提取
           - 若未命中目标表，仅在 searchAuthors URL 下允许回退 result_table 选择器 */
        """
        page_signature = await self._collect_result_page_signature(
            page=page,
            result_marker_sel=marker_sel,
            result_table_sel=fallback_table_sel,
        )
        selected_table_signature, _ = self._select_result_table_signature(
            page_signature=page_signature,
            result_marker_sel=marker_sel,
            result_table_sel=fallback_table_sel,
        )
        if selected_table_signature is not None:
            return await self._extract_table_from_signature(page=page, table_sig=selected_table_signature)

        if fallback_table_sel and self._is_search_authors_url(page.url or ""):
            try:
                return await self._extract_table(page=page, table_sel=fallback_table_sel)
            except Exception:
                return [], []

        return [], []

    async def _extract_table_from_signature(
        self,
        *,
        page: Any,
        table_sig: Dict[str, Any],
    ) -> Tuple[List[str], List[List[str]]]:
        """
        /* 结果表提取（基于页面签名）：
           - 使用签名中的 dom_index 精确定位目标 table，避免 first-match 误提取
           - 仅提取该 table 的表头与数据行，不跨表拼接 */
        """
        try:
            dom_index = int(table_sig.get("dom_index"))
        except Exception:
            dom_index = -1

        data = await page.evaluate(
            r"""({ domIndex }) => {
  const norm = (v) => String(v == null ? '' : v).replace(/\u00a0/g, ' ').replace(/^\s+|\s+$/g, '');
  const tables = Array.from(document.querySelectorAll('table'));
  if (domIndex < 0 || domIndex >= tables.length) return { headers: [], rows: [] };

  const table = tables[domIndex];
  const rows = Array.from(table.querySelectorAll('tr'));
  if (!rows.length) return { headers: [], rows: [] };

  const headerCells = Array.from(rows[0].querySelectorAll('th,td'));
  const headers = headerCells.map(c => norm(c.innerText || c.textContent || ''));
  const dataRows = [];

  for (const r of rows.slice(1)) {
    const cells = Array.from(r.querySelectorAll('th,td'));
    if (!cells.length) continue;
    const values = cells.map(c => {
      let t = norm(c.innerText || c.textContent || '');
      const radio = c.querySelector("input[type='radio'][name='sid']");
      if (radio) {
        const sid = radio.getAttribute('value') || '';
        const checked = !!radio.checked;
        if (!t) t = sid;
        if (checked && sid) t = sid + ' (checked)';
      }
      return t;
    });
    if (values.every(v => !norm(v))) continue;
    dataRows.push(values);
  }

  return { headers, rows: dataRows };
}""",
            {"domIndex": dom_index},
        )
        if not isinstance(data, dict):
            return [], []
        headers = data.get("headers") or []
        rows = data.get("rows") or []
        return list(headers) if isinstance(headers, list) else [], list(rows) if isinstance(rows, list) else []

    async def _extract_table(self, *, page: Any, table_sel: str) -> Tuple[List[str], List[List[str]]]:

        # 用页面内 JS 提取表头与数据行（比抓 HTML 再解析更稳一些）
        data = await page.eval_on_selector(
            table_sel,
            r"""(table) => {
  const norm = (v) => String(v == null ? '' : v).replace(/\u00a0/g, ' ').replace(/^\s+|\s+$/g, '');
  const rows = Array.from(table.querySelectorAll('tr'));
  if (!rows.length) return { headers: [], rows: [] };
  const headerCells = Array.from(rows[0].querySelectorAll('th,td'));
  const headers = headerCells.map(c => norm(c.innerText || c.textContent || ''));
  const data = [];
  for (const r of rows.slice(1)) {
    const cells = Array.from(r.querySelectorAll('th,td'));
    if (!cells.length) continue;
    const values = cells.map(c => {
      let t = norm(c.innerText || c.textContent || '');
      const radio = c.querySelector('input[type=radio][name=sid]');
      if (radio) {
        const sid = radio.getAttribute('value') || '';
        const checked = !!radio.checked;
        if (!t) t = sid;
        if (checked && sid) t = sid + ' (checked)';
      }
      return t;
    });
    // 过滤空行
    if (values.every(v => !norm(v))) continue;
    data.push(values);
  }
  return { headers, rows: data };
}""",
        )
        if not isinstance(data, dict):
            return [], []
        headers = data.get("headers") or []
        rows = data.get("rows") or []
        return list(headers) if isinstance(headers, list) else [], list(rows) if isinstance(rows, list) else []

    async def _save_debug_artifacts(
        self,
        page: Any,
        base_name: str,
        query_name: str,
        create_url: str,
        *,
        reason: str,
    ) -> None:
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        key = f"{stamp}_{self.name}_{_sha1_8(base_name)}_{_sha1_8(query_name)}_{_sha1_8(create_url)}_{reason}"
        try:
            await page.screenshot(path=str(self.debug_dir / f"{key}.png"), full_page=True)
        except Exception:
            pass
        try:
            html = await page.content()
            (self.debug_dir / f"{key}.html").write_text(html, encoding="utf-8")
        except Exception:
            pass


def _is_skipped_result(result: QueryResult) -> bool:
    """
    /* 跳过结果判定：
       - 当前使用 debug.skip_reason 作为跳过标记
       - 仅在任务本身成功时才判定为跳过，避免与失败态混淆 */
    """
    if not result.ok:
        return False
    debug = result.debug if isinstance(result.debug, dict) else {}
    return bool(str(debug.get("skip_reason") or "").strip())


def _normalize_aggregated_author_row(row: Any) -> Optional[List[str]]:
    """
    /* 聚合行标准化：
       - 统一收敛为固定四列（姓名/单位/Email/手机号码）
       - 保持“仅保留包含有效 Email 的记录”规则，避免聚合表混入噪声数据 */
    """
    cells_raw = row if isinstance(row, list) else [row]
    cells = [_normalize_scalar_text(c) for c in cells_raw]
    if not cells:
        return None

    name = cells[0] if len(cells) > 0 else ""
    org = cells[1] if len(cells) > 1 else ""

    email = _extract_email(cells[2] if len(cells) > 2 else "")
    if not email:
        email = _extract_email(" ".join(cells))
    if not email:
        return None

    phone = _extract_phone(cells[3] if len(cells) > 3 else "")
    if not phone:
        phone = _extract_phone(" ".join(cells))

    return [
        name or "-",
        org or "-",
        email,
        phone or "-",
    ]


def _collect_aggregated_author_rows(results: List[QueryResult]) -> List[List[str]]:
    """
    /* 聚合输出：
       - 汇总本次运行中所有成功且非跳过的作者记录
       - 以四列完整值去重，输出单一汇总表，避免重复查询名导致的重复展示 */
    """
    uniq: set[Tuple[str, str, str, str]] = set()
    rows: List[List[str]] = []

    for r in results:
        if not r.ok or _is_skipped_result(r):
            continue
        for row in r.rows:
            normalized = _normalize_aggregated_author_row(row)
            if not normalized:
                continue
            key = (normalized[0], normalized[1], normalized[2], normalized[3])
            if key in uniq:
                continue
            uniq.add(key)
            rows.append(normalized)

    rows.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    return rows


def _build_run_markdown(
    *,
    run_time: str,
    global_cfg: Dict[str, Any],
    results: List[QueryResult],
) -> str:
    """
    生成一次运行的 Markdown 章节（追加写入）。
    """
    lines: List[str] = []
    lines.append(f"## {run_time} 批量作者查询")
    lines.append("")
    lines.append(f"- 并发：`{global_cfg.get('concurrency')}`")
    sim_cfg = global_cfg.get("similar_names") or {}
    lines.append(
        f"- 相似名：`enabled={bool(sim_cfg.get('enabled'))}`，`max_variants={sim_cfg.get('max_variants')}`"
    )
    out_cfg = global_cfg.get("output") or {}
    lines.append(f"- 输出：`{out_cfg.get('user_list_md')}`")
    lines.append(f"- 跳过清单：`{out_cfg.get('skipped_md') or '../skipped_names.md'}`")
    lines.append("")

    total_tasks = len(results)
    skipped_tasks = sum(1 for r in results if _is_skipped_result(r))
    failed_tasks = sum(1 for r in results if not r.ok)
    success_tasks = max(0, total_tasks - skipped_tasks - failed_tasks)
    aggregated_rows = _collect_aggregated_author_rows(results)

    lines.append("### 运行摘要")
    lines.append("")
    lines.append(f"- 总任务数：`{total_tasks}`")
    lines.append(f"- 成功：`{success_tasks}`，跳过：`{skipped_tasks}`，失败：`{failed_tasks}`")
    lines.append(f"- 聚合后记录数：`{len(aggregated_rows)}`")
    lines.append("")

    lines.append("### 聚合作者表")
    lines.append("")
    if aggregated_rows:
        lines.append(_format_md_table(["姓名", "单位", "Email", "手机号码"], aggregated_rows))
    else:
        lines.append("_本次无可聚合的作者记录_")
    lines.append("")

    # 防止文件结尾粘连
    lines.append("")
    return "\n".join(lines)


def _build_realtime_aggregated_markdown(*, run_time: str, rows: List[List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"## {run_time} 聚合作者表")
    lines.append("")
    lines.append("### 聚合作者表")
    lines.append("")
    if rows:
        lines.append(_format_md_table(["姓名", "单位", "Email", "手机号码"], rows))
    else:
        lines.append("_本次无可聚合的作者记录_")
    lines.append("")
    return "\n".join(lines)


def _upsert_marked_block(
    path: Path,
    *,
    start_marker: str,
    end_marker: str,
    block_text: str,
    insert_at_top_when_missing: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join([start_marker, block_text.rstrip(), end_marker, ""])

    if not path.exists():
        path.write_text(payload, encoding="utf-8")
        return

    source = path.read_text(encoding="utf-8")
    start_idx = source.find(start_marker)
    end_idx = source.find(end_marker)
    if start_idx >= 0 and end_idx >= 0 and end_idx >= start_idx:
        end_tail = end_idx + len(end_marker)
        if end_tail < len(source) and source[end_tail : end_tail + 1] == "\n":
            end_tail += 1
        new_text = source[:start_idx] + payload + source[end_tail:]
    else:
        if insert_at_top_when_missing:
            new_text = payload + source
        else:
            if source and not source.endswith("\n"):
                source = source + "\n"
            new_text = source + payload
    path.write_text(new_text, encoding="utf-8")


def _build_failed_markdown(*, run_time: str, failures: List[QueryResult], failed_md_path: str) -> str:
    lines: List[str] = []
    lines.append(f"## {run_time} 失败清单")
    lines.append("")
    lines.append(f"- 输出：`{failed_md_path}`")
    lines.append(f"- 条数：`{len(failures)}`")
    lines.append("")
    for f in failures:
        lines.append(f"- 站点：`{f.site}`，原名：`{f.base_name}`，查询名：`{f.query_name}`")
        lines.append(f"  - create_url：`{f.create_url}`")
        if f.final_url:
            lines.append(f"  - final_url：`{f.final_url}`")
        lines.append(f"  - error：`{f.error}`")
    lines.append("")
    return "\n".join(lines)


def _build_skipped_markdown(*, run_time: str, skipped: List[QueryResult], skipped_md_path: str) -> str:
    lines: List[str] = []
    lines.append(f"## {run_time} 跳过清单")
    lines.append("")
    lines.append(f"- 输出：`{skipped_md_path}`")
    lines.append(f"- 条数：`{len(skipped)}`")
    lines.append("")
    for s in skipped:
        skip_reason = str((s.debug or {}).get("skip_reason") or "命中跳过策略")
        lines.append(f"- 站点：`{s.site}`，原名：`{s.base_name}`，查询名：`{s.query_name}`")
        lines.append(f"  - create_url：`{s.create_url}`")
        if s.final_url:
            lines.append(f"  - final_url：`{s.final_url}`")
        lines.append(f"  - skip_reason：`{skip_reason}`")
    lines.append("")
    return "\n".join(lines)


def _append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        # 保障追加时章节从新行开始（避免读入整个文件）
        try:
            with path.open("rb") as f:
                f.seek(-1, os.SEEK_END)
                last = f.read(1)
            if last != b"\n":
                with path.open("ab") as f:
                    f.write(b"\n")
        except Exception:
            pass
    with path.open("a", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")


def _append_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


async def _run(cfg_path: Path, *, names_override: Optional[Path], concurrency_override: Optional[int]) -> int:
    cfg_dir = cfg_path.parent.resolve()
    cfg = _load_yaml(cfg_path)

    global_cfg: Dict[str, Any] = dict(cfg.get("global") or {})
    if concurrency_override is not None:
        global_cfg["concurrency"] = int(concurrency_override)

    inputs = dict(cfg.get("inputs") or {})
    names_file = _resolve_path(cfg_dir, str(names_override or inputs.get("names_file") or "./names.txt"))
    names = _load_names(names_file)
    if not names:
        print(f"未读取到任何姓名（names_file={names_file}）", file=sys.stderr)
        return 2

    sim_cfg = dict(global_cfg.get("similar_names") or {})
    sim_enabled = bool(sim_cfg.get("enabled", True))
    sim_max = int(sim_cfg.get("max_variants") or 10)
    replace_map = sim_cfg.get("replace_map") or {}
    if not isinstance(replace_map, dict):
        replace_map = {}

    out_cfg = dict(global_cfg.get("output") or {})
    user_list_md = _resolve_path(cfg_dir, str(out_cfg.get("user_list_md") or "../userList.md"))
    failed_md = _resolve_path(cfg_dir, str(out_cfg.get("failed_md") or "../failed_names.md"))
    skipped_md = _resolve_path(cfg_dir, str(out_cfg.get("skipped_md") or "../skipped_names.md"))
    jsonl_path = _resolve_path(cfg_dir, str(out_cfg.get("jsonl") or "../userList.jsonl"))
    write_jsonl = bool(out_cfg.get("write_jsonl", True))

    sites = cfg.get("sites") or []
    if not isinstance(sites, list) or not sites:
        raise RuntimeError("未配置 sites")

    if async_playwright is None:
        raise RuntimeError(f"playwright 不可用：{_playwright_import_error}")  # type: ignore[name-defined]
    if ddddocr is None:
        raise RuntimeError(_build_ddddocr_unavailable_message())

    headless = bool(global_cfg.get("headless", True))
    concurrency = int(global_cfg.get("concurrency") or 3)
    site_concurrency_cfg = dict(global_cfg.get("site_concurrency") or {})
    default_site_concurrency = int(global_cfg.get("site_worker_concurrency") or concurrency or 1)
    query_cfg = dict(global_cfg.get("query") or {})
    name_poll_interval_ms = int(query_cfg.get("name_poll_interval_ms") or 200)
    if name_poll_interval_ms < 0:
        name_poll_interval_ms = 0
    if default_site_concurrency <= 0:
        default_site_concurrency = 1
    browser_cfg = dict(global_cfg.get("browser") or {})

    state_dir = SCRIPT_DIR / ".state"
    debug_dir = SCRIPT_DIR / ".debug"

    # OCR 引擎全局复用（加载模型较慢）
    try:
        ocr_engine = ddddocr.DdddOcr(show_ad=False)
    except Exception as e:
        raise RuntimeError(_build_ddddocr_unavailable_message(e)) from e

    run_time = _now_str()
    results: List[QueryResult] = []

    async with async_playwright() as p:
        browser = await _launch_browser_with_local_chrome_preferred(
            playwright=p,
            headless=headless,
            browser_cfg=browser_cfg,
            cfg_dir=cfg_dir,
        )
        try:
            sessions: List[SiteSession] = []
            session_by_site: Dict[str, SiteSession] = {}
            for s in sites:
                if not isinstance(s, dict):
                    continue
                site_name = str(s.get("name") or "site")
                site_state = state_dir / f"{site_name}.storage_state.json"
                storage_state = str(site_state) if site_state.exists() else None
                context = await browser.new_context(storage_state=storage_state)
                sess = SiteSession(
                    site_cfg=s,
                    global_cfg=global_cfg,
                    context=context,
                    ocr_engine=ocr_engine,
                    state_path=site_state,
                    debug_dir=debug_dir,
                )
                await sess.ensure_login()
                sessions.append(sess)
                session_by_site[sess.name] = sess

            jobs_by_site: Dict[str, List[QueryJob]] = {sess.name: [] for sess in sessions}
            replace_map_for_sim = {
                str(k): list(v) for k, v in (replace_map or {}).items() if isinstance(v, list)
            }
            total_job_count = 0

            for sess in sessions:
                create_urls = sess.site_cfg.get("create_urls") or []
                for base_name in names:
                    variants = _generate_similar_names(
                        base_name=base_name,
                        enabled=sim_enabled,
                        max_variants=sim_max,
                        replace_map=replace_map_for_sim,
                    )
                    for query_name in variants:
                        for cu in create_urls:
                            jobs_by_site[sess.name].append(
                                QueryJob(
                                    site=sess.name,
                                    create_url=str(cu),
                                    base_name=base_name,
                                    query_name=query_name,
                                )
                            )
                            total_job_count += 1

            if total_job_count <= 0:
                print("未生成任何任务（请检查 sites.create_urls）", file=sys.stderr)
                return 2

            write_lock = asyncio.Lock()
            aggregated_keys: set[Tuple[str, str, str, str]] = set()
            aggregated_rows: List[List[str]] = []
            run_block_key = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
            # /* userList.md 聚合块策略：
            #    - 同一轮运行：使用同一组标记，实时更新始终覆盖该运行对应表格
            #    - 不同轮运行：标记带 run_block_key，每次重跑都会新增一个新表格块 */
            realtime_block_start = f"<!-- AUTO_TOOLS_REALTIME_AGGREGATED_START:{run_block_key} -->"
            realtime_block_end = f"<!-- AUTO_TOOLS_REALTIME_AGGREGATED_END:{run_block_key} -->"

            def collect_new_aggregated_rows(result: QueryResult) -> bool:
                if (not result.ok) or _is_skipped_result(result):
                    return False

                changed = False
                for row in result.rows:
                    normalized = _normalize_aggregated_author_row(row)
                    if not normalized:
                        continue
                    key = (normalized[0], normalized[1], normalized[2], normalized[3])
                    if key in aggregated_keys:
                        continue
                    aggregated_keys.add(key)
                    aggregated_rows.append(normalized)
                    changed = True
                return changed

            async def flush_realtime_aggregated_table() -> None:
                async with write_lock:
                    # /* 聚合表实时更新：
                    #    - worker 并发返回成功结果时，只允许单写线程更新聚合区块
                    #    - 用标记区块替换，确保文件中只保留一份最新聚合作者表 */
                    ordered_rows = sorted(aggregated_rows, key=lambda x: (x[0], x[1], x[2], x[3]))
                    block_text = _build_realtime_aggregated_markdown(run_time=run_time, rows=ordered_rows)
                    _upsert_marked_block(
                        user_list_md,
                        start_marker=realtime_block_start,
                        end_marker=realtime_block_end,
                        block_text=block_text,
                        insert_at_top_when_missing=True,
                    )

            async def consume_result(result: QueryResult) -> None:
                results.append(result)
                status_text = "SKIP" if _is_skipped_result(result) else ("OK" if result.ok else "FAIL")
                print(
                    f"[{result.site}] {result.base_name} -> {result.query_name} | {status_text} | rows={len(result.rows)} | {result.elapsed_ms}ms"
                )
                if collect_new_aggregated_rows(result):
                    await flush_realtime_aggregated_table()

            await flush_realtime_aggregated_table()

            worker_tasks: List[asyncio.Task[None]] = []

            for site_name, jobs in jobs_by_site.items():
                sess = session_by_site.get(site_name)
                if sess is None:
                    continue

                raw_limit = site_concurrency_cfg.get(site_name, default_site_concurrency)
                try:
                    worker_count = int(raw_limit)
                except Exception:
                    worker_count = default_site_concurrency
                if worker_count <= 0:
                    worker_count = 1

                queue: asyncio.Queue[Optional[QueryJob]] = asyncio.Queue(maxsize=max(worker_count * 2, 1))

                async def site_worker(
                    *,
                    site_sess: SiteSession,
                    site_queue: asyncio.Queue[Optional[QueryJob]],
                ) -> None:
                    page = await site_sess.context.new_page()
                    try:
                        while True:
                            job = await site_queue.get()
                            if job is None:
                                site_queue.task_done()
                                break

                            try:
                                result = await site_sess.query_author_on_page(
                                    page=page,
                                    create_url=job.create_url,
                                    base_name=job.base_name,
                                    query_name=job.query_name,
                                )
                            except Exception as e:
                                result = QueryResult(
                                    ok=False,
                                    site=site_sess.name,
                                    create_url=job.create_url,
                                    base_name=job.base_name,
                                    query_name=job.query_name,
                                    final_url=page.url or "",
                                    headers=[],
                                    rows=[],
                                    elapsed_ms=0,
                                    submit_attempts=0,
                                    captcha_attempts=0,
                                    error=str(e),
                                )
                                try:
                                    await page.close()
                                except Exception:
                                    pass
                                page = await site_sess.context.new_page()

                            await consume_result(result)
                            site_queue.task_done()
                            if name_poll_interval_ms > 0:
                                await asyncio.sleep(name_poll_interval_ms / 1000.0)
                    finally:
                        try:
                            await page.close()
                        except Exception:
                            pass

                for _ in range(worker_count):
                    worker_tasks.append(
                        asyncio.create_task(
                            site_worker(site_sess=sess, site_queue=queue)
                        )
                    )

                for job in jobs:
                    await queue.put(job)
                for _ in range(worker_count):
                    await queue.put(None)

            await asyncio.gather(*worker_tasks)
        finally:
            await browser.close()

    # 输出：失败/跳过清单 + 可选 JSONL
    failures = [r for r in results if not r.ok]
    skipped = [r for r in results if _is_skipped_result(r)]
    if failures:
        failed_text = _build_failed_markdown(run_time=run_time, failures=failures, failed_md_path=str(failed_md))
        _append_text(failed_md, failed_text)
    if skipped:
        skipped_text = _build_skipped_markdown(run_time=run_time, skipped=skipped, skipped_md_path=str(skipped_md))
        _append_text(skipped_md, skipped_text)

    if write_jsonl:
        jsonl_records: List[Dict[str, Any]] = []
        for r in results:
            debug = r.debug if isinstance(r.debug, dict) else {}
            is_skipped = _is_skipped_result(r)
            skip_reason = str(debug.get("skip_reason") or "")
            jsonl_records.append(
                {
                    "run_time": run_time,
                    "site": r.site,
                    "create_url": r.create_url,
                    "base_name": r.base_name,
                    "query_name": r.query_name,
                    "ok": r.ok,
                    "skipped": is_skipped,
                    "skip_reason": skip_reason,
                    "final_url": r.final_url,
                    "headers": r.headers,
                    "rows": r.rows,
                    "elapsed_ms": r.elapsed_ms,
                    "submit_attempts": r.submit_attempts,
                    "captcha_attempts": r.captcha_attempts,
                    "error": r.error,
                }
            )
        _append_jsonl(jsonl_path, jsonl_records)

    print(f"聚合作者表实时更新写入: {user_list_md}")
    if failures:
        print(f"失败清单已追加写入: {failed_md}")
    if skipped:
        print(f"跳过清单已追加写入: {skipped_md}")
    if write_jsonl:
        print(f"结构化日志已追加写入: {jsonl_path}")

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="批量作者查询（含验证码识别、并发、MD 追加导出）")
    parser.add_argument(
        "--config",
        default=str(SCRIPT_DIR / "config.yaml"),
        help="配置文件路径（默认 autoTools/author_query/config.yaml）",
    )
    parser.add_argument("--names", default="", help="可选：覆盖配置中的 names_file")
    parser.add_argument("--concurrency", type=int, default=0, help="可选：覆盖并发数（>0 生效）")
    args = parser.parse_args(argv)

    cfg_path = Path(args.config).expanduser().resolve()
    names_override = Path(args.names).expanduser().resolve() if args.names else None
    concurrency_override = int(args.concurrency) if int(args.concurrency or 0) > 0 else None

    try:
        return asyncio.run(_run(cfg_path, names_override=names_override, concurrency_override=concurrency_override))
    except KeyboardInterrupt:
        print("用户中断", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"运行失败: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
