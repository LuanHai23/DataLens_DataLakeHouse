from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
import time
import subprocess
import os
from datetime import datetime
from base_crawler import BaseCrawler
from s3_ingestion import MiniOIngestion
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
COOKIES_FILE = BASE_DIR / "json_cookies" / "topcv_cookies_playwright_v1.json"

class JobHunterCrawler_TOPCV:
    def __init__(self):
        self.xvfb = None

        try:
            self.minio = MiniOIngestion()
        except Exception as e:
            print(f"⚠️  MinIO init failed: {e}")
            print("   Crawler vẫn chạy, chỉ skip upload MinIO.")
            self.minio = None

    def _start_virtual_display(self):
        print("🖥️  Starting virtual display (Xvfb)...")
        self.xvfb = subprocess.Popen(
            ["Xvfb", ":99", "-screen", "0", "1920x1080x24"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        os.environ["DISPLAY"] = ":99"
        time.sleep(2)
        print("Virtual display ready.")

    def _stop_virtual_display(self):
        if self.xvfb:
            self.xvfb.terminate()
            print("Virtual display stopped.")

    def _get_page_content(self, page):
        print("Đang cuộn trang để kích hoạt Lazy Load...")
        for _ in range(5):
            page.mouse.wheel(0, 1000)
            time.sleep(1)

    def crawl_topcv(self, page, crawler):
        print("\nĐang crawl TopCV") 
        page.goto( 
            "https://www.topcv.vn/tim-viec-lam-data-kcr257cb261?type_keyword=0&sba=1&category_family=r257~b261&saturday_status=0", 
            timeout=60000 
        )
        
        self._get_page_content(page)

        try:
            page.wait_for_selector(".job-list-search-result", timeout=10000)
        except:
            print(" Không thấy list job TopCV")

        job_cards = page.locator("div.job-item-search-result").all()
        print(f"Tìm thấy {len(job_cards)} jobs TopCV")

        # Debug: in HTML card đầu tiên
        if job_cards:
            print("=" * 60)
            print("DEBUG innerHTML card[0]:")
            print(job_cards[0].inner_html())
            print("=" * 60)

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

                # Tags — <div class="tag"><a class="item-tag">
                tags = []
                for tag_el in card.locator("div.tag a.item-tag").all():
                    t = tag_el.inner_text().strip()
                    if t and t not in tags:
                        tags.append(t)

                # Posted — <label class="address mobile-hidden label-update">
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
                print(f"   ⚠️  {e}")
                continue

        print(f"Tổng số job có được: {len(jobs)}")

        if jobs:
            if self.minio:
                print(f"Đang upload {len(jobs)} jobs lên MinIO...")
                self.minio.upload_jobs("topcv", jobs)
            else:
                print("⚠️  Skip MinIO upload — chỉ lưu local vì MinIO chưa connect được.")
        else:
            print("List job rỗng! Code crawl có vấn đề rồi.")

    def run_topcv(self):
        self._start_virtual_display()

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(
                    headless=False,
                    args=[
                        "--no-sandbox", "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage", "--disable-gpu",
                        "--start-maximized",
                    ]
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="vi-VN",
                )

                cookies_path = COOKIES_FILE

                if os.path.exists(cookies_path):
                    with open(cookies_path) as f:
                        cookies = json.load(f)
                    context.add_cookies(cookies)
                    print(f"🍪 Loaded {len(cookies)} TopCV cookies")
                else:
                    print("⚠️  No TopCV cookies — salary may be hidden!")

                page = context.new_page()
                stealth_sync(page)

                self.crawl_topcv(page, BaseCrawler("topcv"))
                print("\n✅ Đã crawl xong TopCV")
                page.close()
                browser.close()

            except Exception as e:
                print(f"❌ Lỗi crawl TopCV: {e}")
            finally:
                self._stop_virtual_display()

if __name__ == "__main__":
    hunter = JobHunterCrawler_TOPCV()
    hunter.run_topcv()