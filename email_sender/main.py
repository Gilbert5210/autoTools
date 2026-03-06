#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import smtplib
import sys
import time
from builtins import TimeoutError as BuiltinTimeoutError
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib import error as urllib_error
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


class TemplateSelector:
    def __init__(
        self,
        templates: Dict[str, TemplateConfig],
        template_order: List[str],
        default_template_id: str,
        selection: str,
        template_id_field: str,
    ) -> None:
        self.templates = templates
        self.template_order = template_order
        self.default_template_id = default_template_id
        self.selection = selection
        self.template_id_field = template_id_field

    def pick(self, index: int, recipient: Dict[str, str]) -> TemplateConfig:
        if self.selection == "round_robin":
            template_id = self.template_order[index % len(self.template_order)]
            return self.templates[template_id]

        recipient_template_id = (recipient.get(self.template_id_field) or "").strip()
        if recipient_template_id and recipient_template_id in self.templates:
            return self.templates[recipient_template_id]

        if self.default_template_id in self.templates:
            return self.templates[self.default_template_id]

        return self.templates[self.template_order[0]]


class OpenAICompatibleClient:
    def __init__(self, cfg: AIConfig):
        self.cfg = cfg

    def generate(self, prompt: str) -> str:
        last_error: Optional[Exception] = None
        attempts = self.cfg.retries + 1

        for attempt in range(1, attempts + 1):
            try:
                if self.cfg.api_style == "responses":
                    payload = self._build_responses_payload(prompt)
                    response = self._post_json(f"{self.cfg.base_url.rstrip('/')}/responses", payload)
                else:
                    payload = self._build_chat_completions_payload(prompt)
                    response = self._post_json(
                        f"{self.cfg.base_url.rstrip('/')}/chat/completions", payload
                    )

                text = self._extract_text(response)
                if not text:
                    raise RuntimeError("AI response was empty")
                return text
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                if attempt < attempts:
                    time.sleep(self.cfg.retry_backoff_seconds * attempt)

        raise RuntimeError(f"AI generate failed after {attempts} attempts: {last_error}")

    def _build_chat_completions_payload(self, prompt: str) -> Dict[str, Any]:
        messages: List[Dict[str, str]] = []
        if self.cfg.system_prompt:
            messages.append({"role": "system", "content": self.cfg.system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload: Dict[str, Any] = {
            "model": self.cfg.model,
            "messages": messages,
            "temperature": self.cfg.temperature,
        }
        if self.cfg.max_tokens is not None:
            payload["max_tokens"] = self.cfg.max_tokens
        return payload

    def _build_responses_payload(self, prompt: str) -> Dict[str, Any]:
        input_items: List[Dict[str, str]] = []
        if self.cfg.system_prompt:
            input_items.append({"role": "system", "content": self.cfg.system_prompt})
        input_items.append({"role": "user", "content": prompt})

        payload: Dict[str, Any] = {
            "model": self.cfg.model,
            "input": input_items,
            "temperature": self.cfg.temperature,
        }
        if self.cfg.max_tokens is not None:
            payload["max_output_tokens"] = self.cfg.max_tokens
        return payload

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

        templates[template_id] = TemplateConfig(
            template_id=template_id,
            subject_template=subject_template,
            prompt_template=prompt_template,
            fallback_body_template=str(item.get("fallback_body_template", "")).rstrip(),
            content_subtype=content_subtype,
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
        if ai.provider != "openai_compatible":
            raise ConfigError("Only ai.provider=openai_compatible is supported")
        if ai.api_style not in {"chat_completions", "responses"}:
            raise ConfigError("ai.api_style must be chat_completions or responses")
        if not ai.api_key:
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


def create_body(
    template: TemplateConfig,
    context: Dict[str, str],
    ai_client: Optional[OpenAICompatibleClient],
) -> tuple[str, bool]:
    prompt = render_template(template.prompt_template, context)
    if ai_client is not None:
        try:
            generated = ai_client.generate(prompt)
            if generated.strip():
                return generated.strip(), True
        except Exception as exc:
            log(
                f"AI failed for recipient={context.get('email', '')}, "
                f"template={template.template_id}, error={exc}"
            )

    if template.fallback_body_template:
        return render_template(template.fallback_body_template, context), False

    return prompt, False


def send_email(
    sender: SenderConfig,
    to_email: str,
    subject: str,
    body: str,
    content_subtype: str,
    smtp_timeout_seconds: int,
) -> None:
    message = EmailMessage()
    message["From"] = formataddr((sender.from_name, sender.email)) if sender.from_name else sender.email
    message["To"] = to_email
    message["Subject"] = subject

    if content_subtype == "html":
        message.set_content("This is an HTML email. Please use an HTML-compatible mail client.")
        message.add_alternative(body, subtype="html")
    else:
        message.set_content(body)

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


def write_report(report_dir: Path, rows: Iterable[Dict[str, str]]) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    file_path = report_dir / f"send_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    fieldnames = [
        "timestamp",
        "status",
        "recipient_email",
        "recipient_name",
        "sender_id",
        "sender_email",
        "template_id",
        "subject",
        "ai_used",
        "error",
    ]

    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

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

    ai_client = OpenAICompatibleClient(config.ai) if config.ai.enabled else None
    selector = TemplateSelector(
        templates=config.templates,
        template_order=config.template_order,
        default_template_id=config.default_template_id,
        selection=config.template_selection,
        template_id_field=config.recipients.template_id_field,
    )

    dry_run = config.runtime.dry_run or dry_run_override
    total = len(recipients)
    rng = random.Random(config.runtime.random_seed)

    sent_count = 0
    failed_count = 0
    dry_run_count = 0
    consecutive_failures = 0
    report_rows: List[Dict[str, str]] = []

    log(
        f"Start sending: total={total}, senders={len(config.senders)}, "
        f"batch_size={config.runtime.batch_size}, "
        f"batch_interval={config.runtime.batch_interval_seconds}s±{config.runtime.batch_interval_jitter_seconds}s, "
        f"per_email_delay={config.runtime.per_email_delay_seconds}s±{config.runtime.per_email_delay_jitter_seconds}s, "
        f"smtp_retry={config.runtime.smtp_retry_count}, dry_run={dry_run}"
    )
    if config.ai.enabled and config.ai.use_local_codex:
        log(
            f"AI source=local_codex model={config.ai.model} api_style={config.ai.api_style} "
            f"base_url={config.ai.base_url}"
        )

    for idx, recipient in enumerate(recipients):
        recipient_email = recipient.get(config.recipients.email_field, "").strip()
        recipient_name = recipient.get("name", "").strip()
        sender = config.senders[idx % len(config.senders)]

        status = "FAILED"
        error_message = ""
        ai_used = "false"
        template_id = ""
        subject = ""

        try:
            template = selector.pick(idx, recipient)
            template_id = template.template_id

            context = build_context(recipient, sender, idx + 1, total)
            subject = render_template(template.subject_template, context) or "(No Subject)"
            body, used_ai = create_body(template, context, ai_client)
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

        report_rows.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "status": status,
                "recipient_email": recipient_email,
                "recipient_name": recipient_name,
                "sender_id": sender.sender_id,
                "sender_email": sender.email,
                "template_id": template_id,
                "subject": subject,
                "ai_used": ai_used,
                "error": error_message,
            }
        )

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

    report_path = write_report(config.runtime.report_dir, report_rows)
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
