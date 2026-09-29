"""Load Playwright cookies without baking credentials into crawler images."""

import json
import os
from pathlib import Path


class CookieConfigurationError(RuntimeError):
    """Raised when configured crawler cookies are missing or malformed."""


def env_flag(name, default=False):
    """Read a strict boolean environment flag."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise CookieConfigurationError(
        f"{name} must be one of true/false, 1/0, yes/no, or on/off"
    )


def _normalize_same_site(value):
    if value is None:
        return None

    normalized = str(value).strip().lower()
    mapping = {
        "strict": "Strict",
        "lax": "Lax",
        "none": "None",
        "no_restriction": "None",
        "unspecified": "Lax",
    }
    if normalized not in mapping:
        raise CookieConfigurationError("cookie sameSite value is invalid")
    return mapping[normalized]


def _decode_cookie_payload(raw_payload, source_label):
    try:
        payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError) as exc:
        raise CookieConfigurationError(
            f"{source_label} must contain valid JSON"
        ) from exc

    if isinstance(payload, dict):
        payload = payload.get("cookies")

    if not isinstance(payload, list) or not payload:
        raise CookieConfigurationError(
            f"{source_label} must contain a non-empty cookie list"
        )

    cookies = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise CookieConfigurationError(
                f"{source_label} cookie at index {index} must be an object"
            )

        cookie = dict(item)
        name = cookie.get("name")
        if not isinstance(name, str) or not name.strip():
            raise CookieConfigurationError(
                f"{source_label} cookie at index {index} is missing name"
            )

        if "value" not in cookie or not isinstance(cookie["value"], str):
            raise CookieConfigurationError(
                f"{source_label} cookie at index {index} is missing string value"
            )

        domain = cookie.get("domain")
        url = cookie.get("url")
        has_domain = isinstance(domain, str) and bool(domain.strip())
        has_url = isinstance(url, str) and url.startswith(("http://", "https://"))
        if not has_domain and not has_url:
            raise CookieConfigurationError(
                f"{source_label} cookie at index {index} needs domain or URL"
            )

        if has_domain:
            cookie.setdefault("path", "/")

        if "sameSite" in cookie:
            cookie["sameSite"] = _normalize_same_site(cookie["sameSite"])

        cookies.append(cookie)

    return cookies


def load_playwright_cookies(env_var, file_path, required=False):
    """Return ``(cookies, source)`` using environment-first resolution.

    ECS should inject the complete JSON cookie list through ``env_var`` from
    AWS Secrets Manager. Local development can continue using ``file_path``.
    Cookie values are never included in exceptions or log messages.
    """
    environment_payload = os.getenv(env_var)
    if environment_payload is not None:
        cookies = _decode_cookie_payload(environment_payload, env_var)
        return cookies, "environment"

    cookie_path = Path(file_path)
    if cookie_path.is_file():
        try:
            file_payload = cookie_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CookieConfigurationError("cookie file could not be read") from exc

        cookies = _decode_cookie_payload(file_payload, "cookie file")
        return cookies, "file"

    if required:
        raise CookieConfigurationError(
            f"{env_var} is required and no local cookie file is available"
        )

    return [], "none"
