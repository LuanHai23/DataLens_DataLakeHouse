import discord
from discord.ext import commands
import os
from trino.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN     = os.environ.get("DISCORD_TOKEN")
TRINO_HOST        = os.environ.get("TRINO_HOST")
TRINO_PORT        = int(os.environ.get("TRINO_PORT"))
TRINO_USER        = os.environ.get("TRINO_USER")
TRINO_HTTP_SCHEME = os.environ.get("TRINO_HTTP_SCHEME")
TRINO_CATALOG     = os.environ.get("TRINO_CATALOG")
TRINO_SCHEMA      = os.environ.get("TRINO_SCHEMA")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


def query_trino(sql: str) -> list[dict] | None:
    try:
        print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}")
        conn = connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG, schema=TRINO_SCHEMA, http_scheme=TRINO_HTTP_SCHEME,
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close(); conn.close()
        print(f"Query returned {len(rows)} rows")
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Trino query failed: {e}")
        return None



KEYWORD_COLORS = {
    "data-engineer":  0xE53935,
    "data-analyst":   0x1E88E5,
    "data-scientist": 0x43A047,
    "big-data":       0xFB8C00,
}

def make_job_embed(row: dict) -> discord.Embed:
    keyword = row.get("keyword", "") or ""
    color   = KEYWORD_COLORS.get(keyword.lower(), 0x5865F2)

    embed = discord.Embed(
        title = row.get("title", "N/A"),
        url = row.get("url", ""),
        color = color,
    )

    # Company
    company = row.get("company", "")
    if company:
        embed.set_author(name=f"🏢 {company}")

    # Salary — dùng trực tiếp từ gold
    salary = row.get("salary", "")
    if not salary or salary in ("Login to view", "Negotiable", "Hidden", ""):
        salary_str = "🔒 You'll love it"
    else:
        salary_str = salary
    embed.add_field(name="💰 Salary", value=f"**{salary_str}**", inline=True)

    # Work type + Location
    work_type = row.get("work_type", "")
    location  = row.get("location", "")
    parts = []
    if work_type: parts.append(f"🏢 {work_type}")
    if location:  parts.append(f"📍 {location}")
    if parts:
        embed.add_field(name="", value="  •  ".join(parts), inline=False)

    # Tags
    tags = row.get("tags", [])
    if isinstance(tags, str):
        import json as _json
        try: tags = _json.loads(tags)
        except: tags = [t.strip() for t in tags.split(",") if t.strip()]
    if tags:
        embed.add_field(name="🛠 Skills", value="  ".join(f"`{t}`" for t in tags[:6]), inline=False)

    # Footer
    posted  = row.get("posted", "")
    scraped = row.get("scraped_at", "")[:10] if row.get("scraped_at") else ""
    footer  = []
    if posted:  footer.append(f"🕐 {posted}")
    if keyword: footer.append(f"#{keyword}")
    if scraped: footer.append(f"Crawled {scraped}")
    embed.set_footer(text="  •  ".join(footer))

    return embed


# ---------------------------------------------------------------------------
# COMMANDS
# ---------------------------------------------------------------------------
@bot.event
async def on_ready():
    print(f"✅ Bot {bot.user} đã online!")


@bot.command(name="job_de")
async def job_de(ctx, limit: int = 5):
    """!job_de [số lượng] — Hiện job Data Engineer mới nhất"""
    await ctx.send(f"🔍 Đang lấy **{limit}** job Data Engineer mới nhất...")

    query = f"""
        SELECT keyword, title, url, company, location, work_type, salary, tags, posted, report_date
        FROM demo.gold.itviec_jobs
        ORDER BY report_date DESC
        LIMIT {limit}
    """
    data = query_trino(query)

    if data is None:
        await ctx.send("❌ Lỗi kết nối Trino. Kiểm tra lại!")
        return
    if len(data) == 0:
        await ctx.send("😔 Table trống. Chạy `load_itviec_to_iceberg.py` để load data trước!")
        return

    await ctx.send(f"**🚀 TOP {len(data)} DATA ENGINEER JOBS**")
    for row in data:
        await ctx.send(embed=make_job_embed(row))


@bot.command(name="jobs")
async def jobs(ctx, keyword: str = "data-engineer", limit: int = 5):
    """!jobs [keyword] [số lượng] — Tìm job theo keyword"""
    await ctx.send(f"🔍 Đang tìm **{limit}** job `{keyword}`...")

    query = f"""
        SELECT keyword, title, url, company, location, work_type, salary, tags, posted, report_date
        FROM demo.gold.itviec_jobs
        ORDER BY report_date DESC
        LIMIT {limit}
    """
    data = query_trino(query)

    if not data:
        await ctx.send(f"😔 Không tìm thấy job nào cho `{keyword}`.")
        return

    await ctx.send(f"**🚀 {len(data)} JOBS: {keyword.upper()}**")
    for row in data:
        await ctx.send(embed=make_job_embed(row))


@bot.command(name="report")
async def daily_report(ctx):
    await ctx.send("🔍 Đang tổng hợp số liệu mới nhất từ Data Lakehouse...")
    query = """
        SELECT location_std, currency, avg_min_salary, avg_max_salary, total_jobs
        FROM demo.gold.market_summary
    """
    data = query_trino(query)
    if not data:
        await ctx.send("⚠️ Hiện chưa có dữ liệu nào trong kho Gold.")
        return

    msg = "**📊 BÁO CÁO THỊ TRƯỜNG IT MỚI NHẤT**\n" + "=" * 40 + "\n\n"
    for row in data:
        location = row.get("location_std", "Unknown")
        currency = row.get("currency", "VND")
        total_jobs = row.get("total_jobs", 0)
        avg_min = float(row["avg_min_salary"]) if row.get("avg_min_salary") else 0
        avg_max = float(row["avg_max_salary"]) if row.get("avg_max_salary") else 0
        icon = "🏙️" if "Ho Chi Minh" in location else "🏯" if "Ha Noi" in location else "📍"
        msg += f"{icon} **{location}** ({currency})\n"
        msg += f" • Số job tuyển: **{total_jobs}**\n"
        msg += f" • Lương TB (Min): **{avg_min:,.0f}**\n"
        msg += f" • Lương TB (Max): **{avg_max:,.0f}**\n\n"
    await ctx.send(msg)


@bot.command(name="alert")
async def high_salary_alert(ctx):
    await ctx.send("🤑 Đang check hàng VIP")
    query = """
        SELECT title, min_salary, max_salary, currency, source, url, location_std
        FROM demo.gold.daily_alerts
        WHERE report_date = (SELECT MAX(report_date) FROM demo.gold.daily_alerts)
        ORDER BY min_salary DESC
        LIMIT 5
    """
    data = query_trino(query)
    if not data:
        await ctx.send("😔 Hôm nay móm, không có kèo nào thơm cả")
        return

    embeds = []
    for row in data:
        min_sal  = float(row["min_salary"]) if row.get("min_salary") else 0
        max_sal  = float(row["max_salary"]) if row.get("max_salary") else 0
        currency = row.get("currency", "VND")
        salary_str = f"{min_sal:,.0f} - {max_sal:,.0f} {currency}" if max_sal > 0 and max_sal != min_sal else f"Từ {min_sal:,.0f} {currency}"
        embed = discord.Embed(title=row.get("title", "Job giấu tên"), url=row.get("url", "#"), color=0x2ecc71)
        embed.set_author(name=f"Nguồn: {str(row.get('source','Unknown')).upper()}")
        embed.add_field(name="💰 Mức lương", value=f"**{salary_str}**", inline=False)
        embed.add_field(name="📍 Địa điểm", value=row.get("location_std", "Remote"), inline=True)
        embed.set_footer(text="Data Engineer Fresher Project")
        embeds.append(embed)

    await ctx.send(content="**🚨 TOP 5 JOB LƯƠNG KHỦNG NHẤT HÔM NAY 🚨**", embeds=embeds)


@bot.command(name="sources")
async def source_stats(ctx):
    await ctx.send("📊 Đang đếm số lượng job theo nguồn")
    query = """
        SELECT source, jobs_count
        FROM demo.gold.source_stats
        WHERE report_date = (SELECT MAX(report_date) FROM demo.gold.source_stats)
        ORDER BY jobs_count DESC
    """
    data = query_trino(query)
    if not data:
        await ctx.send("⚠️ Chưa có data tổng hợp nguồn.")
        return
    msg = "**📈 TỈ TRỌNG NGUỒN TUYỂN DỤNG**\n" + "=" * 40 + "\n\n"
    for row in data:
        msg += f"• **{row.get('source','Unknown').upper()}**: {row.get('jobs_count', 0)} job\n"
    await ctx.send(msg)


@bot.command(name="ping")
async def ping(ctx):
    await ctx.send("🏓 Pong! Bot còn sống.")

    # Test Trino
    data = query_trino("SELECT 1")
    if data is None:
        await ctx.send("❌ Trino không kết nối được!")
        return
    await ctx.send("✅ Trino OK!")

    # Check table tồn tại không
    tables = query_trino("SHOW TABLES IN demo.gold")
    if tables:
        names = [t[list(t.keys())[0]] for t in tables]
        await ctx.send(f"📋 Tables in demo.gold: `{'`, `'.join(names)}`")
    else:
        await ctx.send("⚠️ demo.gold trống hoặc không tồn tại!")

    # Count rows
    count = query_trino("SELECT COUNT(*) as cnt FROM demo.gold.itviec_jobs")
    if count:
        await ctx.send(f"📊 demo.gold.itviec_jobs: **{count[0]['cnt']}** rows")
    else:
        await ctx.send("❌ demo.gold.itviec_jobs không tồn tại!")


@bot.command(name="help")
async def help_command(ctx):
    msg = """
    **🤖 Job Hunter Bot (Lakehouse Edition)**

    `!job_de [n]`        — Top N job Data Engineer mới nhất (default: 5)
    `!jobs [keyword] [n]` — Tìm job theo keyword (data-analyst, big-data...)
    `!report`            — Báo cáo thị trường chung (lương TB theo khu vực)
    `!alert`             — Top 5 job lương khởi điểm cao nhất
    `!sources`           — Thống kê lượng job theo nguồn crawl
    `!help`              — Hiện danh sách lệnh
    """.strip()
    await ctx.send(msg)

if __name__ == "__main__":
    print("🚀 Đang khởi động Bot...")
    if not DISCORD_TOKEN:
        print("❌ LỖI: Chưa có DISCORD_TOKEN!")
    else:
        try:
            bot.run(DISCORD_TOKEN)
        except Exception as e:
            print(f"❌ Lỗi Bot: {e}")