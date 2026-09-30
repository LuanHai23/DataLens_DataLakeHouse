import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
CRAWLER_PATH = ROOT / "jobs" / "crawlers" / "crawler_topcv.py"
CLASSIFIER_PATH = (
    ROOT / "jobs" / "crawlers" / "topcv_page_classifier.py"
)


def load_classifier():
    spec = importlib.util.spec_from_file_location(
        "topcv_page_classifier",
        CLASSIFIER_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def classify(**overrides):
    values = {
        "status_code": 200,
        "final_url": "https://www.topcv.vn/tim-viec-lam-data",
        "title": "TopCV jobs",
        "body_text": "",
        "job_card_count": 0,
        "job_link_count": 0,
    }
    values.update(overrides)
    return load_classifier().classify_topcv_page(**values)


def test_topcv_runtime_uses_camoufox_without_spoofed_chrome_user_agent():
    source = CRAWLER_PATH.read_text(encoding="utf-8")

    assert "from camoufox.sync_api import Camoufox" in source
    assert "TOPCV_BROWSER_ENGINE=camoufox" in source
    assert "headless=True" in source
    assert "geoip=True" in source
    assert 'os="windows"' in source
    assert "page.context.add_cookies(cookies)" in source
    assert "sync_playwright" not in source
    assert "playwright_stealth" not in source
    assert "Chrome/121" not in source
    assert "inner_html()" not in source


def test_topcv_runtime_logs_safe_failure_classification():
    source = CRAWLER_PATH.read_text(encoding="utf-8")

    assert "TOPCV_HTTP_STATUS=" in source
    assert "TOPCV_FINAL_HOST=" in source
    assert "TOPCV_SELECTOR_COUNTS=" in source
    assert "TOPCV_PAGE_CLASSIFICATION=" in source
    assert "TOPCV_APPLIED_COOKIE_COUNT=" in source
    assert "print(body_text)" not in source


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"job_card_count": 50}, "jobs_visible"),
        (
            {"title": "Attention Required! | Cloudflare"},
            "cloudflare_block",
        ),
        (
            {"body_text": "Sorry, you have been blocked"},
            "cloudflare_block",
        ),
        ({"status_code": 429}, "rate_limited"),
        ({"status_code": 403}, "access_denied"),
        (
            {"body_text": "Verify you are human"},
            "anti_bot_challenge",
        ),
        (
            {"final_url": "https://www.topcv.vn/dang-nhap"},
            "login_redirect",
        ),
        ({"job_link_count": 8}, "selector_drift"),
        ({}, "empty_unknown"),
    ],
)
def test_topcv_page_classification(overrides, expected):
    assert classify(**overrides) == expected
