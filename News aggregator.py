
"""
News Aggregator CLI - single-file implementation.

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl python-dateutil

Usage examples:
    # Fetch latest from NewsAPI (requires NEWSAPI_KEY env var) or fallback scrapers:
    python news_aggregator.py fetch --source all --limit 50

    # View stored articles (filters available)
    python news_aggregator.py view --keyword cloud --start 2025-01-01 --end 2025-11-13

    # Export to excel
    python news_aggregator.py export --format excel --out news.xlsx

    # Run dedup (removes duplicates by url/title)
    python news_aggregator.py dedupe

    # List known scraping sources
    python news_aggregator.py list-sources
"""

import os
import sys
import argparse
import sqlite3
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
from dateutil import parser as dateparser

# Optional pandas for Excel
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except Exception:
    PANDAS_AVAILABLE = False

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_PATH = "news.db"

# === Database helpers ===
def init_db(conn):
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            url TEXT,
            source TEXT,
            published_at TEXT,
            summary TEXT,
            fetched_at TEXT
        );
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_url ON articles(url);")
    conn.commit()

def insert_article(conn, article):
    """
    article: dict with keys title, url, source, published_at, summary
    Deduplicate by URL first, then title.
    """
    c = conn.cursor()
    # quick dedupe by URL
    if article.get("url"):
        c.execute("SELECT id FROM articles WHERE url = ?", (article["url"],))
        if c.fetchone():
            logging.debug("Duplicate url skipped: %s", article["url"])
            return False
    # dedupe by title
    c.execute("SELECT id FROM articles WHERE title = ?", (article["title"],))
    if c.fetchone():
        logging.debug("Duplicate title skipped: %s", article["title"])
        return False

    c.execute("""
        INSERT INTO articles (title, url, source, published_at, summary, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        article.get("title"),
        article.get("url"),
        article.get("source"),
        article.get("published_at"),
        article.get("summary"),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()
    logging.debug("Inserted: %s", article.get("title"))
    return True

# === NewsAPI fetcher ===
def fetch_from_newsapi(api_key, q=None, sources=None, page_size=20, max_pages=1):
    logging.info("Fetching from NewsAPI...")
    base = "https://newsapi.org/v2/top-headlines"
    headers = {"Authorization": api_key}
    results = []
    for page in range(1, max_pages+1):
        params = {"pageSize": page_size, "page": page}
        if q: params["q"] = q
        if sources and sources != "all": params["sources"] = sources
        else:
            params["language"] = "en"
        resp = requests.get(base, params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            logging.error("NewsAPI error: %s - %s", resp.status_code, resp.text[:200])
            break
        j = resp.json()
        for a in j.get("articles", []):
            results.append({
                "title": a.get("title") or "",
                "url": a.get("url"),
                "source": (a.get("source") or {}).get("name"),
                "published_at": a.get("publishedAt"),
                "summary": a.get("description") or ""
            })
        if page * page_size >= j.get("totalResults", 0):
            break
    logging.info("NewsAPI fetched %d articles", len(results))
    return results

# === Simple scrapers (fallback) ===
def scrape_bbc(limit=20):
    logging.info("Scraping BBC front page...")
    url = "https://www.bbc.com"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logging.error("BBC scrape failed: %s", e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    # BBC uses many classes; try common patterns: h3 titles with links
    for h in soup.select("a[href] h3")[:limit*3]:
        title = h.get_text(strip=True)
        a = h.find_parent("a")
        if not a: continue
        link = a.get("href")
        if link and link.startswith("/"):
            link = "https://www.bbc.com" + link
        items.append({"title": title, "url": link, "source": "BBC", "published_at": None, "summary": ""})
        if len(items) >= limit:
            break
    logging.info("BBC scraped %d items", len(items))
    return items

def scrape_cnn(limit=20):
    logging.info("Scraping CNN front page...")
    url = "https://edition.cnn.com"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logging.error("CNN scrape failed: %s", e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    # CNN often has .cd__headline
    for a in soup.select("h3 a, span.cd__headline a, a[href].container__link")[:limit*3]:
        title = a.get_text(strip=True)
        link = a.get("href")
        if not link:
            continue
        if link.startswith("/"):
            link = "https://edition.cnn.com" + link
        items.append({"title": title, "url": link, "source": "CNN", "published_at": None, "summary": ""})
        if len(items) >= limit:
            break
    logging.info("CNN scraped %d items", len(items))
    return items

# === Query and export functions ===
def query_articles(conn, source=None, keyword=None, start_date=None, end_date=None, limit=100):
    c = conn.cursor()
    q = "SELECT id, title, url, source, published_at, summary, fetched_at FROM articles WHERE 1=1"
    params = []
    if source:
        q += " AND source LIKE ?"
        params.append(f"%{source}%")
    if keyword:
        q += " AND (title LIKE ? OR summary LIKE ? OR url LIKE ?)"
        like = f"%{keyword}%"
        params.extend([like, like, like])
    if start_date:
        # compare ISO stored strings
        q += " AND (published_at IS NOT NULL AND published_at >= ?)"
        params.append(start_date)
    if end_date:
        q += " AND (published_at IS NOT NULL AND published_at <= ?)"
        params.append(end_date)
    q += " ORDER BY published_at DESC NULLS LAST, fetched_at DESC LIMIT ?"
    params.append(limit)
    c.execute(q, params)
    rows = c.fetchall()
    cols = ["id", "title", "url", "source", "published_at", "summary", "fetched_at"]
    return [dict(zip(cols, r)) for r in rows]

def export_articles(conn, out_path="export.csv", fmt="csv", **filters):
    rows = query_articles(conn, **filters, limit=1000000)
    if not rows:
        logging.warning("No articles match the filters. Nothing to export.")
        return False
    if fmt == "csv":
        keys = ["id", "title", "url", "source", "published_at", "summary", "fetched_at"]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        logging.info("Exported %d rows to %s", len(rows), out_path)
        return True
    elif fmt in ("excel", "xlsx"):
        if not PANDAS_AVAILABLE:
            logging.error("pandas required for Excel export. Install pandas & openpyxl.")
            return False
        df = pd.DataFrame(rows)
        df.to_excel(out_path, index=False)
        logging.info("Exported %d rows to %s", len(rows), out_path)
        return True
    else:
        logging.error("Unsupported export format: %s", fmt)
        return False

# === Dedup utility ===
def dedupe_db(conn):
    """
    Remove duplicate rows by same URL or same title, keep earliest id.
    """
    c = conn.cursor()
    # remove duplicates by url (keep min id)
    c.execute("""
        DELETE FROM articles WHERE id NOT IN (
            SELECT MIN(id) FROM articles GROUP BY url HAVING url IS NOT NULL
        ) AND url IS NOT NULL;
    """)
    # remove duplicates by title if url is null or multiple with different URLs
    c.execute("""
        DELETE FROM articles WHERE id NOT IN (
            SELECT MIN(id) FROM articles GROUP BY title HAVING title IS NOT NULL
        ) AND title IS NOT NULL;
    """)
    conn.commit()
    logging.info("Deduplication complete.")

# === Helpers ===
def parse_date(s):
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        # return ISO-like string for comparisons
        return dt.isoformat()
    except Exception:
        return None

# === CLI main ===
def main():
    parser = argparse.ArgumentParser(description="News Aggregator CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # fetch
    pfetch = sub.add_parser("fetch", help="Fetch news and store in DB")
    pfetch.add_argument("--source", default="all", help="newsapi source or 'bbc','cnn','all'")
    pfetch.add_argument("--keyword", default=None, help="keyword for NewsAPI q")
    pfetch.add_argument("--limit", type=int, default=50, help="max items to fetch per source")
    pfetch.add_argument("--pages", type=int, default=1, help="pages for NewsAPI pagination")
    pfetch.add_argument("--newsapi-key", default=os.getenv("NEWSAPI_KEY"), help="NewsAPI key (or set NEWSAPI_KEY)")

    # view
    pview = sub.add_parser("view", help="View stored articles")
    pview.add_argument("--source", default=None)
    pview.add_argument("--keyword", default=None)
    pview.add_argument("--start", default=None, help="start date (YYYY-MM-DD or ISO)")
    pview.add_argument("--end", default=None, help="end date (YYYY-MM-DD or ISO)")
    pview.add_argument("--limit", type=int, default=50)

    # export
    pexport = sub.add_parser("export", help="Export stored articles")
    pexport.add_argument("--format", choices=["csv", "excel"], default="csv")
    pexport.add_argument("--out", default="export.csv")
    pexport.add_argument("--source", default=None)
    pexport.add_argument("--keyword", default=None)
    pexport.add_argument("--start", default=None)
    pexport.add_argument("--end", default=None)

    # dedupe
    pdup = sub.add_parser("dedupe", help="Run DB deduplication")

    # list sources
    psource = sub.add_parser("list-sources", help="List built-in scraping sources")

    # clear DB
    pclear = sub.add_parser("clear", help="Clear all articles (use with caution)")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if args.cmd == "fetch":
        articles = []
        # prefer NewsAPI if key present
        if args.newsapi_key:
            try:
                articles = fetch_from_newsapi(args.newsapi_key, q=args.keyword, sources=(None if args.source=="all" else args.source), page_size=args.limit, max_pages=args.pages)
            except Exception as e:
                logging.error("NewsAPI fetch failed: %s", e)
                articles = []
        # If no articles or no key, fallback to scrapers depending on source
        if not articles:
            if args.source in ("all", "bbc"):
                articles += scrape_bbc(limit=args.limit)
            if args.source in ("all", "cnn"):
                articles += scrape_cnn(limit=args.limit)
            # potentially dedupe scraped list by URL/title before insert
            seen = set()
            unique = []
            for a in articles:
                key = (a.get("url") or "") + "|" + (a.get("title") or "")
                if key in seen:
                    continue
                seen.add(key)
                unique.append(a)
            articles = unique

        # store to DB
        count = 0
        for a in articles:
            # ensure title exists
            if not a.get("title"):
                continue
            insert_ok = insert_article(conn, a)
            if insert_ok:
                count += 1
        logging.info("Stored %d new articles.", count)
        conn.close()
        return

    if args.cmd == "view":
        start = parse_date(args.start) if args.start else None
        end = parse_date(args.end) if args.end else None
        rows = query_articles(conn, source=args.source, keyword=args.keyword, start_date=start, end_date=end, limit=args.limit)
        if not rows:
            logging.info("No articles found.")
        else:
            for r in rows:
                pub = r["published_at"] or r["fetched_at"] or "-"
                print(f"[{r['id']}] {r['source']} | {pub}\n{r['title']}\n{r['url']}\n")
        conn.close()
        return

    if args.cmd == "export":
        start = parse_date(args.start) if args.start else None
        end = parse_date(args.end) if args.end else None
        success = export_articles(conn, out_path=args.out, fmt=args.format, source=args.source, keyword=args.keyword, start_date=start, end_date=end)
        if success:
            logging.info("Export completed.")
        conn.close()
        return

    if args.cmd == "dedupe":
        dedupe_db(conn)
        conn.close()
        return

    if args.cmd == "list-sources":
        print("Built-in scrapers: bbc, cnn")
        print("External: NewsAPI (set NEWSAPI_KEY env var)")
        conn.close()
        return

    if args.cmd == "clear":
        confirm = input("Are you sure you want to DELETE ALL articles? Type YES to confirm: ")
        if confirm == "YES":
            c = conn.cursor()
            c.execute("DELETE FROM articles;")
            conn.commit()
            logging.warning("All articles cleared.")
        else:
            logging.info("Aborted.")
        conn.close()
        return

if __name__ == "__main__":
    main()
