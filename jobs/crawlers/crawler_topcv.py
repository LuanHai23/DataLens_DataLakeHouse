import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from camoufox.sync_api import Camoufox

from base_crawler import BaseCrawler
from cookie_loader import env_flag, load_playwright_cookies
from s3_ingestion import MiniOIngestion
from topcv_page_classifier import classify_topcv_page

BASE_DIR = Path(__file__).resolve().parent
COOKIES_FILE = BASE_DIR / "json_cookies" / "topcv_cookies_playwright_v1.json"

class JobHunterCrawler_TOPCV:
    def __init__(self):
        self.minio = MiniOIngestion()

    def _get_page_content(self, page):
        print("Đang cuộn trang để kích hoạt Lazy Load...")
        for _ in range(5):
            page.mouse.wheel(0, 1000)
            time.sleep(1)

    def _diagnose_page(self, page, response):
        list_count = page.locator(".job-list-search-result").count()
        card_count = page.locator("div.job-item-search-result").count()
        job_link_count = page.locator('a[href*="/viec-lam/"]').count()

        try:
            body_text = page.locator("body").inner_text(timeout=5000)
        except Exception:
            body_text = ""

        status_code = response.status if response else 0
        classification = classify_topcv_page(
            status_code=status_code,
            final_url=page.url,
            title=page.title(),
            body_text=body_text,
            job_card_count=card_count,
            job_link_count=job_link_count,
        )

        print(f"TOPCV_HTTP_STATUS={status_code}")
        print(f"TOPCV_FINAL_HOST={urlparse(page.url).netloc}")
        print(
            "TOPCV_SELECTOR_COUNTS="
            f"list:{list_count},cards:{card_count},job_links:{job_link_count}"
        )
        print(f"TOPCV_PAGE_CLASSIFICATION={classification}")
        return classification

    def crawl_topcv(self, page, crawler):
        print("\nĐang crawl TopCV")
        response = page.goto(
            "https://www.topcv.vn/tim-viec-lam-data-kcr257cb261?type_keyword=0&sba=1&category_family=r257~b261&saturday_status=0",
            wait_until="domcontentloaded",
            timeout=60000,
        )

        self._get_page_content(page)

        try:
            page.wait_for_selector(".job-list-search-result", timeout=10000)
        except Exception:
            print(" Không thấy list job TopCV")

        page_classification = self._diagnose_page(page, response)
        job_cards = page.locator("div.job-item-search-result").all()
        print(f"Tìm thấy {len(job_cards)} jobs TopCV")

        jobs = []
        for card in job_cards:
            try:
                # Title & URL — <h3 class="title"><a href="..."><span title="...">
                title_el = card.locator("h3.title a").first
                if title_el.count() == 0:
                    continue
                title   = title_el.get_attribute("title") or title_el.inner_text().strip()
                raw_url = title_el.get_attribute("href") or ""
                url     = raw_url if raw_url.startswith("http") else "https://www.topcv.vn" + raw_url
                # Strip tracking params
                import re
                url = re.sub(r'\?.*', '', url)

                # Company — <a class="company ..."><span class="company-name">
                company = ""
                company_el = card.locator("a.company span.company-name").first
                if company_el.count() > 0:
                    company = company_el.get_attribute("title") or company_el.inner_text().strip()

                # Salary — <label class="title-salary"> hoặc <label class="salary"><span>
                salary = "Thoả thuận"
                salary_el = card.locator("label.title-salary").first
                if salary_el.count() > 0:
                    salary = salary_el.inner_text().strip()
                    # bỏ icon text, chỉ lấy text thật
                    import re as _re
                    salary = _re.sub(r'\s+', ' ', salary).strip()

                # Location — <label class="address"><span class="city-text">
                location = ""
                loc_el = card.locator("label.address span.city-text").first
                if loc_el.count() > 0:
                    location = loc_el.inner_text().strip()

                # Tags <div class="tag"><a class="item-tag">
                tags = []
                for tag_el in card.locator("div.tag a.item-tag").all():
                    t = tag_el.inner_text().strip()
                    if t and t not in tags:
                        tags.append(t)

                # Posted <label class="address mobile-hidden label-update">
                posted = ""
                posted_el = card.locator("label.address.mobile-hidden.label-update").first
                if posted_el.count() > 0:
                    posted = posted_el.inner_text().strip()
                    posted = re.sub(r'\s+', ' ', posted).strip()

                print(f"   - {title} | {company} | {salary} | {location}")
                jobs.append({
                    "title":      title,
                    "url":        url,
                    "company":    company,
                    "salary":     salary,
                    "location":   location,
                    "keyword":    "data",
                    "work_type":  "At office",
                    "tags":       tags,
                    "posted":     posted,
                    "crawled_at": str(datetime.now()),
                })
            except Exception as e:
                print(f"⚠️ {e}")
                continue

        print(f"Tổng số job có được: {len(jobs)}")

        if not jobs:
            failure_classification = (
                "parser_drift" if job_cards else page_classification
            )
            raise RuntimeError(
                "TOPCV_EMPTY_RESULT:"
                f"{failure_classification}:"
                "no jobs parsed; nothing uploaded"
            )

        print(f"Đang upload {len(jobs)} jobs lên object storage...")
        self.minio.upload_jobs("topcv", jobs)

    def run_topcv(self):
        try:
            with Camoufox(
                headless=True,
                geoip=True,
                locale=["vi-VN", "en-US"],
                os="windows",
            ) as browser:
                print("TOPCV_BROWSER_ENGINE=camoufox")
                page = browser.new_page()
                cookies, cookie_source = load_playwright_cookies(
                    "TOPCV_COOKIES_JSON",
                    COOKIES_FILE,
                    required=env_flag("TOPCV_COOKIES_REQUIRED", False),
                )
                if cookies:
                    try:
                        page.context.add_cookies(cookies)
                    except Exception:
                        raise RuntimeError("TOPCV_COOKIE_APPLY_FAILED") from None
                    print(
                        f"🍪 Loaded {len(cookies)} TopCV cookies "
                        f"from {cookie_source}"
                    )
                    print(
                        "TOPCV_APPLIED_COOKIE_COUNT="
                        f"{len(page.context.cookies())}"
                    )
                else:
                    print("⚠️ No TopCV cookies configured — crawl may be blocked!")

                self.crawl_topcv(page, BaseCrawler("topcv"))
                print("\n✅ Đã crawl xong TopCV")
                page.close()

        except Exception as e:
            print(f"❌ Lỗi crawl TopCV: {e}")
            raise

if __name__ == "__main__":
    hunter = JobHunterCrawler_TOPCV()
    hunter.run_topcv()
