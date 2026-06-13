import json
import os

import re
import time
import random
import re
from pathlib import Path
from datetime import datetime
from camoufox.sync_api import Camoufox
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
KEYWORDS     = ["data-engineer", "data-science", "big-data", "data analyst"]
MAX_PAGES    = 5
OUTPUT       = "itviec_jobs.json"
MINIO_PATH   = "s3a://data-lake/itviec/{date}/itviec_jobs.json"
BASE_DIR = Path(__file__).resolve().parent
COOKIES_FILE = BASE_DIR / "json_cookies" / "itviec_cookies_playwright.json"
DEBUG_CARD   = False

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

def jitter(a=2.0, b=5.0):
    time.sleep(random.uniform(a, b))


def wait_for_jobs(page):
    for sel in [".job-card", '[data-controller="job-card"]', ".job_content"]:
        try:
            page.wait_for_selector(sel, timeout=3000)
            return sel
        except Exception:
            continue
    return None


def parse_card(card, keyword):
    def txt(*sels):
        for s in sels:
            try:
                el = card.query_selector(s)
                if el:
                    t = el.inner_text().strip()
                    if t: return t
            except: pass
        return ""

    # Title & URL
    h3 = card.query_selector("h3[data-url]")
    title   = h3.inner_text().strip() if h3 else ""
    raw_url = (h3.get_attribute("data-url") or "") if h3 else ""
    # strip lab_feature param
    url = re.sub(r'\?.*', '', raw_url)

    # Company
    company_el = card.query_selector("span.ims-2 a, .employer-name a, [class*='employer'] a")
    company = company_el.inner_text().strip() if company_el else ""

    # Location
    loc_el = card.query_selector("div[title].text-truncate, div[title].text-nowrap")
    location = loc_el.get_attribute("title") if loc_el else ""

    # Work type (At office / Remote / Hybrid) 
    work_type = ""
    for el in card.query_selector_all("div.text-rich-grey.flex-shrink-0"):
        t = el.inner_text().strip()
        if t in ("At office", "Remote", "Hybrid", "At Office"):
            work_type = t
            break

    # Salary
    salary_el = card.query_selector("div.salary, .salary-from-to, [class*='salary']")
    salary = salary_el.inner_text().strip() if salary_el else "Negotiable"
    if "sign in" in salary.lower():
        salary = "Login to view"

    # Tags/Skills 
    tags = []
    for t in card.query_selector_all("[data-responsive-tag-list-target='tag']"):
        txt_val = t.inner_text().strip()
        if txt_val and txt_val not in tags:
            tags.append(txt_val)

    # Posted date 
    posted = txt(".small-text.text-dark-grey", "[class*='posted']", "span.small-text")

    if not title:
        return None

    return {
        "keyword":    keyword,
        "title":      title,
        "url":        url,
        "company":    company,
        "location":   location,
        "work_type":  work_type,
        "salary":     salary,
        "tags":       tags,
        "posted":     posted,
        "scraped_at": datetime.now().isoformat(),
    }


def scrape_keyword(page, keyword):
    all_jobs = []
    print(f"\n[Keyword: {keyword}]")

    for page_num in range(1, MAX_PAGES + 1):
        url = f"https://itviec.com/it-jobs/{keyword}?page={page_num}"
        print(f" Page {page_num}...", end=" ", flush=True)

        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        jitter(2, 4)

        if "chờ một chút" in page.title().lower() or "just a moment" in page.title().lower():
            print(f"CF block! title={page.title()}")
            break

        matched_sel = wait_for_jobs(page)
        if not matched_sel:
            print(f"0 jobs — không tìm thấy selector (title: {page.title()})")
            break

        cards = page.query_selector_all(matched_sel)
        if not cards:
            print("0 cards — hết trang.")
            break

        # Debug: in HTML card đầu tiên nếu cần
        if DEBUG_CARD and page_num == 1:
            print(f"\n{'='*60}")
            print("DEBUG innerHTML card[0]:")
            print(cards[0].inner_html())
            print(f"{'='*60}\n")

        jobs = []
        for card in cards:
            result = parse_card(card, keyword)
            if result:
                jobs.append(result)

        if not jobs:
            print(f"0 jobs — parse thất bại ({len(cards)} cards found)")
            if cards:
                print("DEBUG card[0]:", cards[0].inner_html()[:1500])
            break

        all_jobs.extend(jobs)
        print(f"{len(jobs)} jobs (tổng: {len(all_jobs)})")
        jitter(1.5, 3.5)

    return all_jobs


def main():
    print("ITviec Scraper — Camoufox + Full Info")
    print("=" * 50)

    all_jobs = []

    with Camoufox(headless=True, geoip=True, locale=["vi-VN", "en-US"], os="windows") as browser:
        page = browser.new_page()

        # Load cookies trước khi goto bất kỳ trang 
        if COOKIES_FILE.exists():
            cookies = json.loads(COOKIES_FILE.read_text())
            # Camoufox dùng page.context
            try:
                page.context.add_cookies(cookies)
                print(f"Loaded {len(cookies)} cookies từ {COOKIES_FILE}")
            except Exception as e:
                print(f"Lỗi load cookies: {e}")
        else:
            print(f"Không có {COOKIES_FILE} — salary sẽ bị ẩn")

        print("Warm-up homepage")
        page.goto("https://itviec.com", wait_until="domcontentloaded", timeout=30_000)
        jitter(3, 5)

        # Kiểm tra đã login chưa
        if page.query_selector("a[href='/sign_in']"):
            print("Chưa login — salary sẽ bị ẩn. Kiểm tra lại cookies!")
        else:
            print("Đã login thành công!")

        for keyword in KEYWORDS:
            jobs = scrape_keyword(page, keyword)
            all_jobs.extend(jobs)
            jitter(3, 6)

    # Deduplicate theo URL
    seen, unique = set(), []
    for j in all_jobs:
        key = j["url"] or j["title"]
        if key not in seen:
            seen.add(key)
            unique.append(j)

    print(f"\nTotal unique jobs: {len(unique)}")

    output = {
        "source":     "itviec",
        "scraped_at": datetime.now().isoformat(),
        "total":      len(unique),
        "keywords":   KEYWORDS,
        "jobs":       unique,
    }
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"💾 Saved local → {OUTPUT}")

    # Upload lên MinIO để Bronze layer đọc được
    try:
        import boto3
        from botocore.client import Config
        from datetime import date

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )
        bucket  = "data-lake"
        key     = f"itviec/{date.today().isoformat()}/itviec_jobs.json"

        # Tạo bucket nếu chưa có
        try:
            s3.head_bucket(Bucket=bucket)
        except:
            s3.create_bucket(Bucket=bucket)

        s3.upload_file(OUTPUT, bucket, key)
        print(f"Uploaded → s3://{bucket}/{key}")
    except Exception as e:
        print(f"MinIO upload failed: {e} — file vẫn được lưu local")

    if unique:
        print("\nSample (job đầu tiên)")
        print(json.dumps(unique[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()