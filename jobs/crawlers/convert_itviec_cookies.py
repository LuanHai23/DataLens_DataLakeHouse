import json
from pathlib import Path

RAW_COOKIE_PATH = Path("C:/Users/PC (newgear)/Desktop/VNJobs_API_DataLakeHouse/jobs/crawlers/json_cookies/itviec_cookies.json")
OUTPUT_COOKIE_PATH = Path("C:/Users/PC (newgear)/Desktop/VNJobs_API_DataLakeHouse/jobs/crawlers/json_cookies/itviec_cookies_playwright.json")


def normalize_same_site(value):
    if not value:
        return "Lax"

    value = str(value).lower()

    if value == "strict":
        return "Strict"

    if value == "none" or value == "no_restriction":
        return "None"

    if value == "lax":
        return "Lax"

    # Chrome sometimes exports "unspecified"
    return "Lax"


def convert_cookie(cookie):
    domain = cookie.get("domain")

    if not domain:
        raise ValueError(f"Cookie missing domain: {cookie}")

    converted = {
        "name": cookie["name"],
        "value": cookie["value"],
        "domain": domain,
        "path": cookie.get("path", "/"),
        "httpOnly": bool(cookie.get("httpOnly", False)),
        "secure": bool(cookie.get("secure", False)),
        "sameSite": normalize_same_site(cookie.get("sameSite")),
    }
    # Chrome export uses expirationDate
    # Playwright uses expires
    expires = cookie.get("expirationDate") or cookie.get("expires")

    if expires:
        converted["expires"] = int(float(expires))

    return converted


def main():
    if not RAW_COOKIE_PATH.exists():
        raise FileNotFoundError(f"Raw cookie file not found: {RAW_COOKIE_PATH}")

    with RAW_COOKIE_PATH.open("r", encoding="utf-8") as f:
        raw_cookies = json.load(f)

    playwright_cookies = []

    for cookie in raw_cookies:
        name = cookie.get("name")

        if not name or "value" not in cookie:
            continue

        playwright_cookies.append(convert_cookie(cookie))

    OUTPUT_COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_COOKIE_PATH.open("w", encoding="utf-8") as f:
        json.dump(playwright_cookies, f, ensure_ascii=False, indent=2)

    print(f"Converted {len(playwright_cookies)} cookies")
    print(f"Saved to: {OUTPUT_COOKIE_PATH}")


if __name__ == "__main__":
    main()