#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import mimetypes
import os
import random
import re
import shutil
import smtplib
import subprocess
import sys
import tempfile
import time
from builtins import TimeoutError as BuiltinTimeoutError
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, TextIO
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
import socket

import yaml

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
AI_KEY_PLACEHOLDERS = {"", "sk-xxxxxx", "your_api_key", "change_me", "placeholder"}
REPORT_FIELDNAMES = [
    "timestamp",
    "status",
    "recipient_email",
    "recipient_name",
    "sender_id",
    "sender_email",
    "template_id",
    "subject",
    "body",
    "ai_used",
    "error",
]


class ConfigError(ValueError):
    pass


class SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return ""


@dataclass(frozen=True)
class SenderConfig:
    sender_id: str
    from_name: str
    email: str
    password: str
    smtp_host: str
    smtp_port: int
    use_ssl: bool
    use_tls: bool


@dataclass(frozen=True)
class TemplateConfig:
    template_id: str
    subject_template: str
    prompt_template: str
    fallback_body_template: str
    content_subtype: str
    attachment_paths: tuple[Path, ...]
    inline_image_paths: tuple[Path, ...]


@dataclass(frozen=True)
class RuntimeConfig:
    batch_size: int
    batch_interval_seconds: float
    batch_interval_jitter_seconds: float
    per_email_delay_seconds: float
    per_email_delay_jitter_seconds: float
    smtp_timeout_seconds: int
    smtp_retry_count: int
    smtp_retry_backoff_seconds: float
    failure_pause_threshold: int
    failure_pause_seconds: float
    random_seed: Optional[int]
    dry_run: bool
    report_dir: Path
    skip_invalid_recipient_email: bool


@dataclass(frozen=True)
class RecipientConfig:
    file_path: Path
    email_field: str
    template_id_field: str


@dataclass(frozen=True)
class AIConfig:
    enabled: bool
    use_local_codex: bool
    local_codex_home: Optional[str]
    provider: str
    reasoning_effort: str
    api_style: str
    base_url: str
    api_key: str
    model: str
    system_prompt: str
    temperature: float
    max_tokens: Optional[int]
    timeout_seconds: int
    retries: int
    retry_backoff_seconds: float


@dataclass(frozen=True)
class AppConfig:
    runtime: RuntimeConfig
    recipients: RecipientConfig
    senders: List[SenderConfig]
    templates: Dict[str, TemplateConfig]
    template_order: List[str]
    template_selection: str
    default_template_id: str
    ai: AIConfig


class AITextClient(Protocol):
    provider_name: str

    def generate(self, prompt: str) -> str:
        ...


class TemplateSelector:
    def __init__(
        self,
        templates: Dict[str, TemplateConfig],
        template_order: List[str],
        default_template_id: str,
        selection: str,
        template_id_field: str,
        rng: random.Random,
    ) -> None:
        self.templates = templates
        self.template_order = template_order
        self.default_template_id = default_template_id
        self.selection = selection
        self.template_id_field = template_id_field
        self.rng = rng

    def pick(self, index: int, recipient: Dict[str, str]) -> TemplateConfig:
        _ = index
        _ = recipient
        template_id = self.rng.choice(self.template_order)
        return self.templates[template_id]


class OpenAICompatibleClient:
    provider_name = "openai_compatible"

    def __init__(self, cfg: AIConfig):
        self.cfg = cfg

    def generate(self, prompt: str) -> str:
        last_error: Optional[Exception] = None
        attempts = self.cfg.retries + 1
        request_variants = list(self._iter_request_variants(prompt))

        for attempt in range(1, attempts + 1):
            for url, payload in request_variants:
                try:
                    response = self._post_json(url, payload)
                    text = self._extract_text(response)
                    if not text:
                        raise RuntimeError("AI response was empty")
                    return text
                except Exception as exc:  # pragma: no cover - network path
                    last_error = exc

            if attempt < attempts:
                time.sleep(self.cfg.retry_backoff_seconds * attempt)

        raise RuntimeError(
            f"AI generate failed after {attempts} attempts and {len(request_variants)} request variants: {last_error}"
        )

    def _iter_request_variants(self, prompt: str) -> Iterable[tuple[str, Dict[str, Any]]]:
        if self.cfg.api_style == "responses":
            payloads = self._build_responses_payload_variants(prompt)
            urls = self._build_endpoint_variants("responses")
        else:
            payloads = self._build_chat_completions_payload_variants(prompt)
            urls = self._build_endpoint_variants("chat/completions")

        for url in urls:
            for payload in payloads:
                yield url, payload

    def _build_endpoint_variants(self, endpoint: str) -> List[str]:
        base_url = self.cfg.base_url.rstrip("/")
        parsed = urllib_parse.urlsplit(base_url)
        path = parsed.path.rstrip("/")
        candidates: List[str] = []

        def add_candidate(url: str) -> None:
            if url and url not in candidates:
                candidates.append(url)

        if path.endswith(f"/{endpoint}"):
            add_candidate(base_url)
            return candidates

        add_candidate(f"{base_url}/{endpoint}")

        if path in {"", "/"}:
            add_candidate(f"{base_url}/v1/{endpoint}")
        elif path != "/v1" and not path.endswith("/v1"):
            add_candidate(f"{base_url}/v1/{endpoint}")

        return candidates

    def _build_chat_completions_payload_variants(self, prompt: str) -> List[Dict[str, Any]]:
        messages: List[Dict[str, str]] = []
        if self.cfg.system_prompt:
            messages.append({"role": "system", "content": self.cfg.system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload: Dict[str, Any] = {
            "model": self.cfg.model,
            "messages": messages,
        }
        if self.cfg.temperature >= 0:
            payload["temperature"] = self.cfg.temperature
        if self.cfg.max_tokens is not None:
            payload["max_tokens"] = self.cfg.max_tokens

        variants = [payload]
        if "temperature" in payload:
            variants.append({k: v for k, v in payload.items() if k != "temperature"})
        return variants

    def _build_responses_payload_variants(self, prompt: str) -> List[Dict[str, Any]]:
        combined_prompt = prompt
        if self.cfg.system_prompt:
            combined_prompt = f"{self.cfg.system_prompt.rstrip()}\n\n{prompt}".strip()

        payload_simple: Dict[str, Any] = {
            "model": self.cfg.model,
            "input": prompt,
        }
        if self.cfg.system_prompt:
            payload_simple["instructions"] = self.cfg.system_prompt
        if self.cfg.temperature >= 0:
            payload_simple["temperature"] = self.cfg.temperature
        if self.cfg.max_tokens is not None:
            payload_simple["max_output_tokens"] = self.cfg.max_tokens

        payload_structured: Dict[str, Any] = {
            "model": self.cfg.model,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            ],
        }
        if self.cfg.system_prompt:
            payload_structured["instructions"] = self.cfg.system_prompt
        if self.cfg.temperature >= 0:
            payload_structured["temperature"] = self.cfg.temperature
        if self.cfg.max_tokens is not None:
            payload_structured["max_output_tokens"] = self.cfg.max_tokens

        payload_simple_combined: Dict[str, Any] = {
            "model": self.cfg.model,
            "input": combined_prompt,
        }
        if self.cfg.temperature >= 0:
            payload_simple_combined["temperature"] = self.cfg.temperature
        if self.cfg.max_tokens is not None:
            payload_simple_combined["max_output_tokens"] = self.cfg.max_tokens

        payload_structured_combined: Dict[str, Any] = {
            "model": self.cfg.model,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": combined_prompt}],
                }
            ],
        }
        if self.cfg.temperature >= 0:
            payload_structured_combined["temperature"] = self.cfg.temperature
        if self.cfg.max_tokens is not None:
            payload_structured_combined["max_output_tokens"] = self.cfg.max_tokens

        variants = [
            payload_simple,
            payload_structured,
            payload_simple_combined,
            payload_structured_combined,
        ]
        variants.extend({k: v for k, v in item.items() if k != "temperature"} for item in variants)

        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in variants:
            key = json.dumps(item, ensure_ascii=False, sort_keys=True)
            if key not in seen:
                seen.add(key)
                deduped.append(item)
        return deduped

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.cfg.api_key}",
            "Content-Type": "application/json",
        }
        request = urllib_request.Request(url, data=body, headers=headers, method="POST")

        try:
            with urllib_request.urlopen(request, timeout=self.cfg.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="ignore")
            short_err_body = err_body[:500].replace("\n", " ")
            raise RuntimeError(f"AI API HTTP {exc.code}: {short_err_body}") from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"AI API network error: {exc.reason}") from exc

        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError("AI API returned non-JSON object")
        return parsed

    def _extract_text(self, payload: Dict[str, Any]) -> str:
        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            message = first.get("message") if isinstance(first, dict) else {}
            if isinstance(message, dict):
                content = message.get("content")
                extracted = _extract_content_text(content)
                if extracted:
                    return extracted

        output = payload.get("output")
        if isinstance(output, list):
            pieces: List[str] = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                text = _extract_content_text(content)
                if text:
                    pieces.append(text)
            merged = "\n".join(x.strip() for x in pieces if x.strip())
            if merged:
                return merged

        return ""


class CodexCLIClient:
    provider_name = "codex_cli"

    def __init__(self, cfg: AIConfig):
        self.cfg = cfg

    def generate(self, prompt: str) -> str:
        codex_bin = shutil.which("codex")
        if not codex_bin:
            raise RuntimeError("Local Codex CLI not found in PATH")

        final_prompt = self._build_prompt(prompt)
        output_path: Optional[Path] = None

        try:
            with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
                output_path = Path(f.name)

            command = [
                codex_bin,
                "exec",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "--color",
                "never",
                "--ephemeral",
                "-c",
                f"model_reasoning_effort={json.dumps(self.cfg.reasoning_effort)}",
                "-m",
                self.cfg.model,
                "--output-last-message",
                str(output_path),
                final_prompt,
            ]

            log(
                f"AI request start provider=codex_cli model={self.cfg.model} "
                f"reasoning_effort={self.cfg.reasoning_effort} timeout={self.cfg.timeout_seconds}s"
            )
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.cfg.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            if output_path is not None:
                try:
                    output_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise RuntimeError(
                f"Local Codex CLI timeout after {self.cfg.timeout_seconds}s"
            ) from exc

        if completed.returncode != 0:
            if output_path is not None:
                try:
                    output_path.unlink(missing_ok=True)
                except Exception:
                    pass
            details = (completed.stderr or completed.stdout or "").strip().replace("\n", " ")
            raise RuntimeError(f"Local Codex CLI failed: {details[:500]}")

        if not output_path or not output_path.exists():
            raise RuntimeError("Local Codex CLI did not produce output file")

        try:
            text = output_path.read_text(encoding="utf-8").strip()
        finally:
            try:
                output_path.unlink(missing_ok=True)
            except Exception:
                pass

        cleaned = _cleanup_generated_text(text)
        if not cleaned:
            raise RuntimeError("Local Codex CLI returned empty content")
        return cleaned

    def _build_prompt(self, prompt: str) -> str:
        parts = [
            "你是一名专业的企业商务邮件助手。",
            "请严格遵循下方输出格式要求，不要输出解释、分析、代码块或任何额外说明。",
        ]
        if self.cfg.system_prompt:
            parts.append(self.cfg.system_prompt)
        parts.append(prompt)
        return "\n\n".join(part.strip() for part in parts if part and part.strip())


def _extract_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        texts: List[str] = []
        for item in content:
            if isinstance(item, str):
                texts.append(item)
            elif isinstance(item, dict):
                value = item.get("text")
                if isinstance(value, str):
                    texts.append(value)
                elif isinstance(value, dict):
                    maybe = value.get("value")
                    if isinstance(maybe, str):
                        texts.append(maybe)
        return "\n".join(x.strip() for x in texts if x and x.strip())
    return ""


def _cleanup_generated_text(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 3:
            cleaned = "\n".join(lines[1:-1]).strip()
    return cleaned


def _build_subject_body_prompt(prompt: str, subject_hint: str) -> str:
    return (
        "请基于下面的邮件写作要求，同时生成一个邮件标题和一封邮件正文。\n\n"
        "原始写作要求：\n"
        f"{prompt}\n\n"
        "标题要求：\n"
        "1) 18-32字；\n"
        "2) 真诚、亲切、专业，不夸张，不营销腔；\n"
        "3) 让医生愿意点开并继续阅读；\n"
        f"4) 可参考但不要照抄这个备选标题：{subject_hint}\n\n"
        "输出格式要求（严格遵守）：\n"
        "第一行必须以 `SUBJECT:` 开头，后面直接写邮件标题。\n"
        "第二行必须是 `BODY:`\n"
        "从第三行开始只写邮件正文。\n"
        "不要添加任何解释、前言、后记、代码块或多余标记。"
    )


def _parse_generated_subject_body(text: str) -> tuple[str, str]:
    cleaned = _cleanup_generated_text(text)
    subject_match = re.search(r"(?mi)^\s*(?:SUBJECT|标题|主题)\s*[:：]\s*(.+?)\s*$", cleaned)
    if not subject_match:
        raise RuntimeError("AI response missing SUBJECT line")

    body_marker = re.search(r"(?mi)^\s*(?:BODY|正文)\s*[:：]?\s*$", cleaned)
    if not body_marker:
        inline_body = re.search(r"(?mis)^\s*(?:BODY|正文)\s*[:：]\s*(.+)$", cleaned)
        if inline_body:
            body = inline_body.group(1).strip()
        else:
            raise RuntimeError("AI response missing BODY section")
    else:
        body = cleaned[body_marker.end() :].strip()

    subject = subject_match.group(1).strip()
    if not subject:
        raise RuntimeError("AI response subject was empty")
    if not body:
        raise RuntimeError("AI response body was empty")
    return subject, body


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def _expand_env_vars(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in os.environ:
            raise ConfigError(f"Environment variable is not set: {key}")
        return os.environ[key]

    return ENV_VAR_PATTERN.sub(repl, value)


def _deep_expand_env(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _deep_expand_env(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_deep_expand_env(item) for item in data]
    if isinstance(data, str):
        return _expand_env_vars(data)
    return data


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a YAML dictionary")

    expanded = _deep_expand_env(raw)
    if not isinstance(expanded, dict):
        raise ConfigError("Expanded config root must be a dictionary")

    return expanded


def to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def to_int(value: Any, name: str, *, minimum: Optional[int] = None) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise ConfigError(f"{name} must be an integer") from exc

    if minimum is not None and parsed < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return parsed


def to_float(value: Any, name: str, *, minimum: Optional[float] = None) -> float:
    try:
        parsed = float(value)
    except Exception as exc:
        raise ConfigError(f"{name} must be a number") from exc

    if minimum is not None and parsed < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return parsed


def resolve_path(config_dir: Path, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    return (config_dir / candidate).resolve()


def _is_ai_key_placeholder(value: str) -> bool:
    return value.strip().lower() in AI_KEY_PLACEHOLDERS


def _is_empty_or_default(value: str, defaults: set[str]) -> bool:
    return value.strip() == "" or value.strip().lower() in {x.lower() for x in defaults}


def _parse_template_file_paths(
    config_dir: Path,
    template_item: Dict[str, Any],
    template_index: int,
    field_name: str,
) -> tuple[Path, ...]:
    raw_paths = template_item.get(field_name, [])
    if raw_paths is None:
        path_items: List[Any] = []
    elif isinstance(raw_paths, str):
        path_items = [raw_paths]
    elif isinstance(raw_paths, list):
        path_items = raw_paths
    else:
        raise ConfigError(f"templates.items[{template_index}].{field_name} must be a list or string")

    parsed_paths: List[Path] = []
    for path_index, raw_path in enumerate(path_items):
        path_value = str(raw_path or "").strip()
        if not path_value:
            raise ConfigError(
                f"templates.items[{template_index}].{field_name}[{path_index}] cannot be empty"
            )

        resolved_path = resolve_path(config_dir, path_value)
        if not resolved_path.exists():
            raise ConfigError(
                f"templates.items[{template_index}].{field_name}[{path_index}] not found: {resolved_path}"
            )
        if not resolved_path.is_file():
            raise ConfigError(
                f"templates.items[{template_index}].{field_name}[{path_index}] is not a file: {resolved_path}"
            )
        parsed_paths.append(resolved_path)

    return tuple(parsed_paths)


def load_local_codex_ai_defaults(codex_home: Optional[str]) -> Dict[str, str]:
    if tomllib is None:
        raise ConfigError("Python >= 3.11 is required to parse local codex config (tomllib missing)")

    base_home = (
        Path(codex_home).expanduser()
        if codex_home and codex_home.strip()
        else Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    )
    config_path = base_home / "config.toml"
    auth_path = base_home / "auth.json"

    if not config_path.exists():
        raise ConfigError(f"Local codex config.toml not found: {config_path}")
    if not auth_path.exists():
        raise ConfigError(f"Local codex auth.json not found: {auth_path}")

    try:
        parsed_cfg = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ConfigError(f"Failed to parse local codex config: {exc}") from exc

    if not isinstance(parsed_cfg, dict):
        raise ConfigError("Local codex config is invalid (root is not object)")

    provider_name = parsed_cfg.get("model_provider")
    model = str(parsed_cfg.get("model") or "").strip()
    providers = parsed_cfg.get("model_providers") if isinstance(parsed_cfg.get("model_providers"), dict) else {}
    provider_cfg = providers.get(provider_name) if isinstance(provider_name, str) else {}
    if not isinstance(provider_cfg, dict):
        provider_cfg = {}

    base_url = str(provider_cfg.get("base_url") or "").strip()
    wire_api = str(provider_cfg.get("wire_api") or "").strip()

    try:
        parsed_auth = json.loads(auth_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ConfigError(f"Failed to parse local codex auth.json: {exc}") from exc

    if not isinstance(parsed_auth, dict):
        raise ConfigError("Local codex auth.json is invalid (root is not object)")

    api_key = str(parsed_auth.get("OPENAI_API_KEY") or "").strip()

    if not base_url:
        raise ConfigError("Local codex config missing model_providers.<provider>.base_url")
    if not model:
        raise ConfigError("Local codex config missing model")
    if wire_api not in {"responses", "chat_completions"}:
        wire_api = "chat_completions"
    if not api_key:
        raise ConfigError("Local codex auth.json missing OPENAI_API_KEY")

    return {
        "base_url": base_url,
        "model": model,
        "api_style": wire_api,
        "api_key": api_key,
    }


def build_ai_client(ai: AIConfig) -> AITextClient:
    if ai.provider == "codex_cli":
        return CodexCLIClient(ai)
    return OpenAICompatibleClient(ai)


def parse_config(raw: Dict[str, Any], config_dir: Path) -> AppConfig:
    runtime_raw = raw.get("runtime") or {}
    recipients_raw = raw.get("recipients") or {}
    ai_raw = raw.get("ai") or {}
    templates_raw = raw.get("templates") or {}

    runtime = RuntimeConfig(
        batch_size=to_int(runtime_raw.get("batch_size", 10), "runtime.batch_size", minimum=1),
        batch_interval_seconds=to_float(
            runtime_raw.get("batch_interval_seconds", 10),
            "runtime.batch_interval_seconds",
            minimum=0,
        ),
        batch_interval_jitter_seconds=to_float(
            runtime_raw.get("batch_interval_jitter_seconds", 2),
            "runtime.batch_interval_jitter_seconds",
            minimum=0,
        ),
        per_email_delay_seconds=to_float(
            runtime_raw.get("per_email_delay_seconds", 1.5),
            "runtime.per_email_delay_seconds",
            minimum=0,
        ),
        per_email_delay_jitter_seconds=to_float(
            runtime_raw.get("per_email_delay_jitter_seconds", 1.5),
            "runtime.per_email_delay_jitter_seconds",
            minimum=0,
        ),
        smtp_timeout_seconds=to_int(
            runtime_raw.get("smtp_timeout_seconds", 20),
            "runtime.smtp_timeout_seconds",
            minimum=1,
        ),
        smtp_retry_count=to_int(
            runtime_raw.get("smtp_retry_count", 2),
            "runtime.smtp_retry_count",
            minimum=0,
        ),
        smtp_retry_backoff_seconds=to_float(
            runtime_raw.get("smtp_retry_backoff_seconds", 3),
            "runtime.smtp_retry_backoff_seconds",
            minimum=0,
        ),
        failure_pause_threshold=to_int(
            runtime_raw.get("failure_pause_threshold", 3),
            "runtime.failure_pause_threshold",
            minimum=1,
        ),
        failure_pause_seconds=to_float(
            runtime_raw.get("failure_pause_seconds", 120),
            "runtime.failure_pause_seconds",
            minimum=0,
        ),
        random_seed=(
            to_int(runtime_raw["random_seed"], "runtime.random_seed")
            if runtime_raw.get("random_seed") not in (None, "")
            else None
        ),
        dry_run=to_bool(runtime_raw.get("dry_run", False), default=False),
        report_dir=resolve_path(config_dir, str(runtime_raw.get("report_dir", "./reports"))),
        skip_invalid_recipient_email=to_bool(
            runtime_raw.get("skip_invalid_recipient_email", True), default=True
        ),
    )

    recipients_file = str(recipients_raw.get("file") or "").strip()
    if not recipients_file:
        raise ConfigError("recipients.file is required")

    recipients = RecipientConfig(
        file_path=resolve_path(config_dir, recipients_file),
        email_field=str(recipients_raw.get("email_field", "email")).strip() or "email",
        template_id_field=str(recipients_raw.get("template_id_field", "template_id")).strip()
        or "template_id",
    )

    senders_raw = raw.get("senders")
    if not isinstance(senders_raw, list) or not senders_raw:
        raise ConfigError("senders must be a non-empty list")

    senders: List[SenderConfig] = []
    for idx, item in enumerate(senders_raw):
        if not isinstance(item, dict):
            raise ConfigError(f"senders[{idx}] must be an object")

        sender_id = str(item.get("id", f"sender_{idx + 1}")).strip() or f"sender_{idx + 1}"
        email = str(item.get("email", "")).strip()
        password = str(item.get("password", "")).strip()
        smtp_host = str(item.get("smtp_host", "")).strip()

        if not email:
            raise ConfigError(f"senders[{idx}].email is required")
        if not password:
            raise ConfigError(f"senders[{idx}].password is required")
        if not smtp_host:
            raise ConfigError(f"senders[{idx}].smtp_host is required")

        if not EMAIL_RE.match(email):
            raise ConfigError(f"senders[{idx}].email is invalid: {email}")

        use_ssl = to_bool(item.get("use_ssl", True), default=True)
        use_tls = to_bool(item.get("use_tls", False), default=False)
        if use_ssl and use_tls:
            raise ConfigError(f"senders[{idx}] cannot set both use_ssl=true and use_tls=true")

        sender = SenderConfig(
            sender_id=sender_id,
            from_name=str(item.get("from_name", sender_id)).strip(),
            email=email,
            password=password,
            smtp_host=smtp_host,
            smtp_port=to_int(item.get("smtp_port", 465), f"senders[{idx}].smtp_port", minimum=1),
            use_ssl=use_ssl,
            use_tls=use_tls,
        )
        senders.append(sender)

    template_items = templates_raw.get("items")
    if not isinstance(template_items, list) or not template_items:
        raise ConfigError("templates.items must be a non-empty list")

    templates: Dict[str, TemplateConfig] = {}
    template_order: List[str] = []
    for idx, item in enumerate(template_items):
        if not isinstance(item, dict):
            raise ConfigError(f"templates.items[{idx}] must be an object")

        template_id = str(item.get("id", "")).strip()
        if not template_id:
            raise ConfigError(f"templates.items[{idx}].id is required")
        if template_id in templates:
            raise ConfigError(f"Duplicate template id: {template_id}")

        subject_template = str(item.get("subject_template", "")).strip()
        prompt_template = str(item.get("prompt_template", "")).rstrip()

        if not subject_template:
            raise ConfigError(f"templates.items[{idx}].subject_template is required")
        if not prompt_template:
            raise ConfigError(f"templates.items[{idx}].prompt_template is required")

        content_subtype = str(item.get("content_subtype", "plain")).strip().lower()
        if content_subtype not in {"plain", "html"}:
            raise ConfigError(
                f"templates.items[{idx}].content_subtype must be plain or html, got {content_subtype}"
            )

        attachment_paths = _parse_template_file_paths(
            config_dir=config_dir,
            template_item=item,
            template_index=idx,
            field_name="attachment_paths",
        )
        inline_image_paths = _parse_template_file_paths(
            config_dir=config_dir,
            template_item=item,
            template_index=idx,
            field_name="inline_image_paths",
        )

        templates[template_id] = TemplateConfig(
            template_id=template_id,
            subject_template=subject_template,
            prompt_template=prompt_template,
            fallback_body_template=str(item.get("fallback_body_template", "")).rstrip(),
            content_subtype=content_subtype,
            attachment_paths=attachment_paths,
            inline_image_paths=inline_image_paths,
        )
        template_order.append(template_id)

    default_template_id = str(templates_raw.get("default_template_id", template_order[0])).strip()
    if default_template_id not in templates:
        raise ConfigError(f"templates.default_template_id not found: {default_template_id}")

    template_selection = str(templates_raw.get("selection", "by_recipient_field")).strip()
    if template_selection not in {"by_recipient_field", "round_robin"}:
        raise ConfigError("templates.selection must be by_recipient_field or round_robin")

    ai_enabled = to_bool(ai_raw.get("enabled", True), default=True)
    ai_use_local_codex = to_bool(ai_raw.get("use_local_codex", False), default=False)
    ai_local_codex_home = str(ai_raw.get("local_codex_home", "")).strip() or None
    ai_provider = str(ai_raw.get("provider", "openai_compatible")).strip() or "openai_compatible"
    ai_reasoning_effort = str(ai_raw.get("reasoning_effort", "low")).strip().lower() or "low"
    ai_api_style = str(ai_raw.get("api_style", "chat_completions")).strip() or "chat_completions"
    ai_base_url = str(ai_raw.get("base_url", "https://api.openai.com/v1")).strip() or "https://api.openai.com/v1"
    ai_api_key = str(ai_raw.get("api_key", "")).strip()
    ai_model = str(ai_raw.get("model", "gpt-4.1-mini")).strip() or "gpt-4.1-mini"

    if ai_use_local_codex:
        codex_defaults = load_local_codex_ai_defaults(ai_local_codex_home)

        if _is_empty_or_default(ai_base_url, {"https://api.openai.com/v1"}):
            ai_base_url = codex_defaults["base_url"]
        if _is_empty_or_default(ai_model, {"gpt-4.1-mini"}):
            ai_model = codex_defaults["model"]
        if _is_empty_or_default(ai_api_style, {"chat_completions"}):
            ai_api_style = codex_defaults["api_style"]
        if _is_ai_key_placeholder(ai_api_key):
            ai_api_key = codex_defaults["api_key"]

    ai = AIConfig(
        enabled=ai_enabled,
        use_local_codex=ai_use_local_codex,
        local_codex_home=ai_local_codex_home,
        provider=ai_provider,
        reasoning_effort=ai_reasoning_effort,
        api_style=ai_api_style,
        base_url=ai_base_url,
        api_key=ai_api_key,
        model=ai_model,
        system_prompt=str(ai_raw.get("system_prompt", "")).rstrip(),
        temperature=to_float(ai_raw.get("temperature", 0.7), "ai.temperature", minimum=0),
        max_tokens=(
            to_int(ai_raw["max_tokens"], "ai.max_tokens", minimum=1)
            if ai_raw.get("max_tokens") not in (None, "")
            else None
        ),
        timeout_seconds=to_int(ai_raw.get("timeout_seconds", 30), "ai.timeout_seconds", minimum=1),
        retries=to_int(ai_raw.get("retries", 2), "ai.retries", minimum=0),
        retry_backoff_seconds=to_float(
            ai_raw.get("retry_backoff_seconds", 2), "ai.retry_backoff_seconds", minimum=0
        ),
    )

    if ai.enabled:
        if ai.provider not in {"openai_compatible", "codex_cli"}:
            raise ConfigError("ai.provider must be openai_compatible or codex_cli")
        if ai.reasoning_effort not in {"low", "medium", "high", "xhigh"}:
            raise ConfigError("ai.reasoning_effort must be low / medium / high / xhigh")
        if ai.api_style not in {"chat_completions", "responses"}:
            raise ConfigError("ai.api_style must be chat_completions or responses")
        if ai.provider == "openai_compatible" and not ai.api_key:
            raise ConfigError("ai.api_key is required when ai.enabled=true")

    return AppConfig(
        runtime=runtime,
        recipients=recipients,
        senders=senders,
        templates=templates,
        template_order=template_order,
        template_selection=template_selection,
        default_template_id=default_template_id,
        ai=ai,
    )


def normalize_row(row: Dict[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in row.items():
        clean_key = (str(key) if key is not None else "").strip()
        if not clean_key:
            continue
        clean_value = "" if value is None else str(value).strip()
        normalized[clean_key] = clean_value
    return normalized


def load_recipients(cfg: AppConfig) -> List[Dict[str, str]]:
    path = cfg.recipients.file_path
    if not path.exists():
        raise FileNotFoundError(f"Recipients file not found: {path}")

    suffix = path.suffix.lower()
    rows: List[Dict[str, str]] = []
    skipped_invalid = 0

    if suffix in {".csv", ".tsv"}:
        delimiter = "," if suffix == ".csv" else "\t"
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if not reader.fieldnames:
                raise ConfigError("Recipients CSV/TSV must contain header row")

            for raw_row in reader:
                row = normalize_row(raw_row)
                email = row.get(cfg.recipients.email_field, "")
                if not email:
                    continue
                if not EMAIL_RE.match(email):
                    if cfg.runtime.skip_invalid_recipient_email:
                        skipped_invalid += 1
                        continue
                    raise ConfigError(f"Invalid recipient email: {email}")
                rows.append(row)
    else:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                value = line.strip()
                if not value or value.startswith("#"):
                    continue

                email = value.split(",", 1)[0].strip()
                if not EMAIL_RE.match(email):
                    if cfg.runtime.skip_invalid_recipient_email:
                        skipped_invalid += 1
                        continue
                    raise ConfigError(f"Invalid recipient email: {email}")

                rows.append({cfg.recipients.email_field: email})

    if skipped_invalid:
        log(f"Skipped {skipped_invalid} recipients due to invalid email format")

    return rows


def render_template(text: str, context: Dict[str, str]) -> str:
    str_context = {k: "" if v is None else str(v) for k, v in context.items()}
    return text.format_map(SafeDict(str_context)).strip()


def build_context(
    recipient: Dict[str, str], sender: SenderConfig, index: int, total: int
) -> Dict[str, str]:
    context: Dict[str, str] = dict(recipient)
    context.setdefault("name", recipient.get("name", ""))
    context["sender_id"] = sender.sender_id
    context["sender_name"] = sender.from_name
    context["sender_email"] = sender.email
    context["send_index"] = str(index)
    context["total_count"] = str(total)
    context["today"] = datetime.now().strftime("%Y-%m-%d")
    return context


def create_subject_and_body(
    template: TemplateConfig,
    context: Dict[str, str],
    ai_client: Optional[AITextClient],
) -> tuple[str, str, bool]:
    prompt = render_template(template.prompt_template, context)
    fallback_subject = render_template(template.subject_template, context) or "(No Subject)"
    if ai_client is not None:
        try:
            log(
                f"AI generate start provider={ai_client.provider_name} recipient={context.get('email', '')} "
                f"template={template.template_id}"
            )
            generated = ai_client.generate(_build_subject_body_prompt(prompt, fallback_subject))
            subject, body = _parse_generated_subject_body(generated)
            if subject.strip() and body.strip():
                return subject.strip(), body.strip(), True
        except Exception as exc:
            log(
                f"AI failed for recipient={context.get('email', '')}, "
                f"template={template.template_id}, error={exc}"
            )

    if template.fallback_body_template:
        return fallback_subject, render_template(template.fallback_body_template, context), False

    return fallback_subject, prompt, False


def _guess_mime_parts(file_path: Path) -> tuple[str, str]:
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type and "/" in mime_type:
        return tuple(mime_type.split("/", 1))  # type: ignore[return-value]
    return "application", "octet-stream"


def _normalize_html_body(body: str) -> str:
    if re.search(r"<[A-Za-z][^>]*>", body):
        return body
    normalized = body.replace("\r\n", "\n").replace("\r", "\n")
    return html.escape(normalized).replace("\n", "<br>\n")


def send_email(
    sender: SenderConfig,
    to_email: str,
    subject: str,
    body: str,
    content_subtype: str,
    smtp_timeout_seconds: int,
    attachment_paths: Iterable[Path] = (),
    inline_image_paths: Iterable[Path] = (),
) -> None:
    message = EmailMessage()
    message["From"] = formataddr((sender.from_name, sender.email)) if sender.from_name else sender.email
    message["To"] = to_email
    message["Subject"] = subject

    if content_subtype == "html":
        html_body = _normalize_html_body(body)
        message.set_content("This is an HTML email. Please use an HTML-compatible mail client.")
        message.add_alternative(html_body, subtype="html")
    else:
        message.set_content(body)

    for attachment_path in attachment_paths:
        maintype, subtype = _guess_mime_parts(attachment_path)
        message.add_attachment(
            attachment_path.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=attachment_path.name,
        )
    for inline_image_path in inline_image_paths:
        maintype, subtype = _guess_mime_parts(inline_image_path)
        message.add_attachment(
            inline_image_path.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=inline_image_path.name,
        )

    if sender.use_ssl:
        with smtplib.SMTP_SSL(
            sender.smtp_host, sender.smtp_port, timeout=smtp_timeout_seconds
        ) as smtp:
            smtp.login(sender.email, sender.password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(sender.smtp_host, sender.smtp_port, timeout=smtp_timeout_seconds) as smtp:
        smtp.ehlo()
        if sender.use_tls:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(sender.email, sender.password)
        smtp.send_message(message)


def is_transient_smtp_error(exc: Exception) -> bool:
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        try:
            codes = [int(v[0]) for v in exc.recipients.values() if isinstance(v, tuple) and v]
        except Exception:
            codes = []
        if codes:
            return all(400 <= code < 500 for code in codes)
        return False

    if isinstance(
        exc,
        (
            smtplib.SMTPServerDisconnected,
            smtplib.SMTPConnectError,
            smtplib.SMTPHeloError,
            smtplib.SMTPDataError,
            socket.timeout,
            BuiltinTimeoutError,
        ),
    ):
        return True

    if isinstance(exc, smtplib.SMTPResponseException):
        return 400 <= int(exc.smtp_code) < 500

    text = str(exc).lower()
    transient_keywords = (
        "timed out",
        "temporarily",
        "try again later",
        "rate limit",
        "too many",
        "connection reset",
        "broken pipe",
    )
    return any(keyword in text for keyword in transient_keywords)


def compute_wait_seconds(base: float, jitter: float, rng: random.Random) -> float:
    if base < 0:
        base = 0.0
    if jitter <= 0:
        return base
    value = base + rng.uniform(-jitter, jitter)
    return max(0.0, value)


def create_report_writer(report_dir: Path) -> tuple[Path, TextIO, csv.DictWriter]:
    report_dir.mkdir(parents=True, exist_ok=True)
    file_path = report_dir / f"send_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    report_file = file_path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(report_file, fieldnames=REPORT_FIELDNAMES)
    writer.writeheader()
    report_file.flush()
    return file_path, report_file, writer


def write_report(report_dir: Path, rows: Iterable[Dict[str, str]]) -> Path:
    file_path, report_file, writer = create_report_writer(report_dir)
    try:
        for row in rows:
            writer.writerow(row)
        report_file.flush()
    finally:
        report_file.close()
    return file_path


def run(config: AppConfig, *, dry_run_override: bool, start_index: int, limit: int, no_wait: bool) -> int:
    recipients = load_recipients(config)
    if start_index:
        recipients = recipients[start_index:]
    if limit > 0:
        recipients = recipients[:limit]

    if not recipients:
        log("No recipients to process")
        return 0

    rng = random.Random(config.runtime.random_seed)
    ai_client = build_ai_client(config.ai) if config.ai.enabled else None
    selector = TemplateSelector(
        templates=config.templates,
        template_order=config.template_order,
        default_template_id=config.default_template_id,
        selection=config.template_selection,
        template_id_field=config.recipients.template_id_field,
        rng=rng,
    )

    dry_run = config.runtime.dry_run or dry_run_override
    total = len(recipients)

    sent_count = 0
    failed_count = 0
    dry_run_count = 0
    consecutive_failures = 0
    report_path, report_file, report_writer = create_report_writer(config.runtime.report_dir)

    log(
        f"Start sending: total={total}, senders={len(config.senders)}, "
        f"batch_size={config.runtime.batch_size}, "
        f"batch_interval={config.runtime.batch_interval_seconds}s±{config.runtime.batch_interval_jitter_seconds}s, "
        f"per_email_delay={config.runtime.per_email_delay_seconds}s±{config.runtime.per_email_delay_jitter_seconds}s, "
        f"smtp_retry={config.runtime.smtp_retry_count}, dry_run={dry_run}"
    )
    if config.ai.enabled and config.ai.use_local_codex:
        log(
            f"AI source=local_codex provider={config.ai.provider} model={config.ai.model} "
            f"reasoning_effort={config.ai.reasoning_effort} "
            f"api_style={config.ai.api_style} base_url={config.ai.base_url}"
        )
    try:
        for idx, recipient in enumerate(recipients):
            recipient_email = recipient.get(config.recipients.email_field, "").strip()
            recipient_name = recipient.get("name", "").strip()
            sender = config.senders[idx % len(config.senders)]

            status = "FAILED"
            error_message = ""
            ai_used = "false"
            template_id = ""
            subject = ""
            body = ""

            try:
                template = selector.pick(idx, recipient)
                template_id = template.template_id

                context = build_context(recipient, sender, idx + 1, total)
                subject, body, used_ai = create_subject_and_body(template, context, ai_client)
                ai_used = "true" if used_ai else "false"

                if not dry_run:
                    max_attempts = config.runtime.smtp_retry_count + 1
                    send_error: Optional[Exception] = None
                    for send_attempt in range(1, max_attempts + 1):
                        try:
                            send_email(
                                sender=sender,
                                to_email=recipient_email,
                                subject=subject,
                                body=body,
                                content_subtype=template.content_subtype,
                                smtp_timeout_seconds=config.runtime.smtp_timeout_seconds,
                                attachment_paths=template.attachment_paths,
                                inline_image_paths=template.inline_image_paths,
                            )
                            send_error = None
                            break
                        except Exception as exc:
                            send_error = exc
                            transient = is_transient_smtp_error(exc)
                            has_next_attempt = send_attempt < max_attempts
                            if transient and has_next_attempt:
                                retry_wait = (
                                    config.runtime.smtp_retry_backoff_seconds * send_attempt
                                ) + rng.uniform(0, 1.5)
                                log(
                                    f"[{idx + 1}/{total}] SMTP retry {send_attempt}/{config.runtime.smtp_retry_count} "
                                    f"recipient={recipient_email} wait={retry_wait:.2f}s error={exc}"
                                )
                                if not no_wait and retry_wait > 0:
                                    time.sleep(retry_wait)
                                continue
                            raise send_error

                    status = "SENT"
                    sent_count += 1
                    consecutive_failures = 0
                    log(
                        f"[{idx + 1}/{total}] SENT recipient={recipient_email} "
                        f"sender={sender.email} template={template.template_id}"
                    )
                else:
                    status = "DRY_RUN"
                    dry_run_count += 1
                    consecutive_failures = 0
                    log(
                        f"[{idx + 1}/{total}] DRY_RUN recipient={recipient_email} "
                        f"sender={sender.email} template={template.template_id}"
                    )
            except Exception as exc:
                status = "FAILED"
                failed_count += 1
                consecutive_failures += 1
                error_message = str(exc)
                log(f"[{idx + 1}/{total}] FAILED recipient={recipient_email} error={error_message}")

            report_writer.writerow(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status": status,
                    "recipient_email": recipient_email,
                    "recipient_name": recipient_name,
                    "sender_id": sender.sender_id,
                    "sender_email": sender.email,
                    "template_id": template_id,
                    "subject": subject,
                    "body": body,
                    "ai_used": ai_used,
                    "error": error_message,
                }
            )
            report_file.flush()

            reached_batch_end = (idx + 1) % config.runtime.batch_size == 0
            has_more = (idx + 1) < total
            if (
                has_more
                and not no_wait
                and consecutive_failures >= config.runtime.failure_pause_threshold
                and config.runtime.failure_pause_seconds > 0
            ):
                cooldown = compute_wait_seconds(
                    config.runtime.failure_pause_seconds,
                    config.runtime.per_email_delay_jitter_seconds,
                    rng,
                )
                log(
                    f"Consecutive failures={consecutive_failures}, activate cooldown {cooldown:.2f}s "
                    f"before continuing"
                )
                time.sleep(cooldown)
                consecutive_failures = 0

            if has_more and not no_wait:
                if reached_batch_end:
                    wait_seconds = compute_wait_seconds(
                        config.runtime.batch_interval_seconds,
                        config.runtime.batch_interval_jitter_seconds,
                        rng,
                    )
                    log(f"Batch complete, sleep {wait_seconds:.2f}s before next batch")
                    time.sleep(wait_seconds)
                else:
                    wait_seconds = compute_wait_seconds(
                        config.runtime.per_email_delay_seconds,
                        config.runtime.per_email_delay_jitter_seconds,
                        rng,
                    )
                    if wait_seconds > 0:
                        log(f"Inter-email delay {wait_seconds:.2f}s")
                        time.sleep(wait_seconds)
    finally:
        report_file.close()

    log(
        f"Finished. sent={sent_count}, failed={failed_count}, dry_run={dry_run_count}, "
        f"report={report_path}"
    )

    return 0 if failed_count == 0 else 2


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch email sender with AI-generated body")
    parser.add_argument(
        "--config",
        default="email_sender/config.yaml",
        help="Path to YAML config (default: email_sender/config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not send email, only simulate and produce report",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Start from recipient index (0-based)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process first N recipients after --start-index (0 = all)",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip sleeping between batches",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.start_index < 0:
        raise SystemExit("--start-index must be >= 0")
    if args.limit < 0:
        raise SystemExit("--limit must be >= 0")

    config_path = Path(args.config).resolve()
    config_dir = config_path.parent

    try:
        raw = load_yaml(config_path)
        config = parse_config(raw, config_dir)
        return run(
            config,
            dry_run_override=args.dry_run,
            start_index=args.start_index,
            limit=args.limit,
            no_wait=args.no_wait,
        )
    except (ConfigError, FileNotFoundError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("[ERROR] Interrupted by user", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
