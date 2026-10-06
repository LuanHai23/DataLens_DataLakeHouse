import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "jobs" / "crawlers" / "cookie_loader.py"


def load_cookie_module():
    spec = importlib.util.spec_from_file_location("crawler_cookie_loader", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def valid_cookie(name="session", value="value"):
    return {
        "name": name,
        "value": value,
        "domain": ".example.test",
        "path": "/",
        "sameSite": "lax",
    }


def test_environment_cookie_secret_takes_precedence(monkeypatch, tmp_path):
    module = load_cookie_module()
    cookie_file = tmp_path / "cookies.json"
    cookie_file.write_text(json.dumps([valid_cookie("file-cookie")]), encoding="utf-8")
    monkeypatch.setenv(
        "TEST_COOKIES_JSON",
        json.dumps([valid_cookie("environment-cookie")]),
    )

    cookies, source = module.load_playwright_cookies(
        "TEST_COOKIES_JSON",
        cookie_file,
        required=True,
    )

    assert source == "environment"
    assert cookies[0]["name"] == "environment-cookie"
    assert cookies[0]["sameSite"] == "Lax"


def test_local_cookie_file_is_supported(monkeypatch, tmp_path):
    module = load_cookie_module()
    monkeypatch.delenv("TEST_COOKIES_JSON", raising=False)
    cookie_file = tmp_path / "cookies.json"
    cookie_file.write_text(json.dumps([valid_cookie()]), encoding="utf-8")

    cookies, source = module.load_playwright_cookies(
        "TEST_COOKIES_JSON",
        cookie_file,
    )

    assert source == "file"
    assert len(cookies) == 1


def test_missing_optional_cookie_source_returns_empty(monkeypatch, tmp_path):
    module = load_cookie_module()
    monkeypatch.delenv("TEST_COOKIES_JSON", raising=False)

    cookies, source = module.load_playwright_cookies(
        "TEST_COOKIES_JSON",
        tmp_path / "missing.json",
    )

    assert cookies == []
    assert source == "none"


def test_missing_required_cookie_source_fails(monkeypatch, tmp_path):
    module = load_cookie_module()
    monkeypatch.delenv("TEST_COOKIES_JSON", raising=False)

    with pytest.raises(module.CookieConfigurationError, match="is required"):
        module.load_playwright_cookies(
            "TEST_COOKIES_JSON",
            tmp_path / "missing.json",
            required=True,
        )


def test_invalid_secret_does_not_fallback_or_leak(monkeypatch, tmp_path):
    module = load_cookie_module()
    secret_value = "not-json-sensitive-cookie-value"
    monkeypatch.setenv("TEST_COOKIES_JSON", secret_value)
    cookie_file = tmp_path / "cookies.json"
    cookie_file.write_text(json.dumps([valid_cookie()]), encoding="utf-8")

    with pytest.raises(module.CookieConfigurationError) as error:
        module.load_playwright_cookies("TEST_COOKIES_JSON", cookie_file)

    assert "valid JSON" in str(error.value)
    assert secret_value not in str(error.value)


@pytest.mark.parametrize("value", ["true", "1", "YES", "on"])
def test_required_flag_accepts_true_values(monkeypatch, value):
    module = load_cookie_module()
    monkeypatch.setenv("COOKIE_REQUIRED", value)

    assert module.env_flag("COOKIE_REQUIRED") is True


def test_crawlers_use_environment_first_cookie_loader():
    topcv_source = (
        ROOT / "jobs" / "crawlers" / "crawler_topcv.py"
    ).read_text(encoding="utf-8")
    itviec_source = (
        ROOT / "jobs" / "crawlers" / "crawler_itviec.py"
    ).read_text(encoding="utf-8")

    assert "TOPCV_COOKIES_JSON" in topcv_source
    assert "TOPCV_COOKIES_REQUIRED" in topcv_source
    assert "ITVIEC_COOKIES_JSON" in itviec_source
    assert "ITVIEC_COOKIES_REQUIRED" in itviec_source
    assert "load_playwright_cookies" in topcv_source
    assert "load_playwright_cookies" in itviec_source
    assert "TOPCV_COOKIE_APPLY_FAILED" in topcv_source
    assert "ITVIEC_COOKIE_APPLY_FAILED" in itviec_source
    assert "with open(cookies_path)" not in topcv_source
    assert "COOKIES_FILE.read_text()" not in itviec_source
