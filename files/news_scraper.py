"""
=============================================================
 Casablanca Crime Scraper — scraper/news_scraper.py
 Scrapes 20 Moroccan news websites for سرقة articles
 then filters those mentioning الدار البيضاء or its neighborhoods
=============================================================
"""

import cloudscraper
from bs4 import BeautifulSoup
import time
import random
import json
import csv
import os
import re
from datetime import datetime
from logger import get_logger
from helpers import clean_text, contains_casablanca, extract_neighborhood

logger = get_logger("scraper")

# ─────────────────────────────────────────────
# CASABLANCA NEIGHBORHOODS — used as filter
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# CITY IDENTIFIERS — to confirm article is about Casablanca
# Use these ONLY for city-level confirmation, NOT as neighborhoods
# ─────────────────────────────────────────────────────────────
CASABLANCA_CITY = [
    "الدار البيضاء",
    "الدارالبيضاء",
    "البيضاء",
    "Casablanca",
    "Casa",
]

# ─────────────────────────────────────────────────────────────
# OFFICIAL ARRONDISSEMENTS (16 + 1) — Administrative divisions
# Source: Commune de Casablanca official + Wikipedia
# ─────────────────────────────────────────────────────────────
CASABLANCA_ARRONDISSEMENTS = [
    # Préfecture Casablanca-Anfa
    "أنفا",                    # Anfa
    "المعاريف",                # Maarif
    "سيدي بليوط",             # Sidi Belyout
    "بورغون",                 # Bourgogne

    # Préfecture Ain Chock
    "عين الشق",               # Ain Chock
    "سيدي عثمان",             # Sidi Othmane

    # Préfecture Ain Sebaa - Hay Mohammadi
    "عين السبع",               # Ain Sebaa
    "الحي المحمدي",           # Hay Mohammadi

    # Préfecture Ben M'sick
    "بن مسيك",                # Ben M'sick
    "سباتة",                  # Sbata

    # Préfecture Sidi Bernoussi
    "سيدي البرنوصي",          # Sidi Bernoussi
    "الصخور السوداء",         # Roches Noires / Hay Taddart

    # Préfecture Al Fida - Mers Sultan
    "الفداء",                  # Al Fida
    "درب السلطان",            # Derb Sultan / Mers Sultan
    "مرس السلطان",            # Mers Sultan (alternate)

    # Préfecture Hay Hassani
    "الحي الحسني",            # Hay Hassani

    # Préfecture Moulay Rachid
    "مولاي رشيد",             # Moulay Rachid
    "سيدي مومن",              # Sidi Moumen
]

# ─────────────────────────────────────────────────────────────
# POPULAR NEIGHBORHOODS & QUARTIERS
# Well-known areas frequently mentioned in news articles
# ─────────────────────────────────────────────────────────────
CASABLANCA_NEIGHBORHOODS = [
    # Rich / Central neighborhoods
    "غوتييه",                 # Gauthier
    "راسين",                  # Racine
    "بلفدير",                 # Belvedere / Belvedère
    "الكاليفورنيا",           # California
    "النخيل",                 # Hay Nakhil / Palmier
    "عين دياب",               # Ain Diab
    "برنامج",                 # CIL
    "سيدي معروف",             # Sidi Maarouf
    "بوسيجور",                # Bousejour
    "الإنارة",                # Hay Inara
    "الهناء",                 # Hay Hanaa
    "السلام",                 # Hay Salam
    "الرحمة",                 # Hay Rahma
    "التضامن",                # Hay Tadamoun
    "الفتح",                  # Hay Fath
    "الحسين",                 # Hay Houssein
    "الفاروق",                # Hay Farouq
    "النصر",                  # Hay Nasr
    "الولفة",                 # Oulfa
    "مبروكة",                 # Mabrouka
    "الحبوس",                 # Habous / Nouvelle Médina

    # Popular / Working class neighborhoods
    "درب غلف",                # Derb Ghallef
    "درب الكبير",             # Derb Kébir
    "درب كوبا",               # Derb Cuba
    "سيدي أبي عثمان",         # Sidi Abou Othmane
    "الكاريان",               # Carrières Centrales / Kariane
    "بن مسيك",                # Ben Msick
    "سيدي مؤمن",              # Sidi Moumen (alternate spelling)
    "حي الفرح",               # Hay Farah
    "عين الذياب",             # Ain Diab (alternate)
    "بوسكورة",                # Bouskoura
    "تيط مليل",               # Tit Mellil
    "عين الحرودة",            # Ain Harrouda
    "لسان المدينة",           # Lissasfa zone
    "ابن امسيك",              # Ibn Msik
    "الإدريسية",              # Hay Idrissia
    "المدينة القديمة",        # Médina (old city)
    "عين البرجة",             # Ain Borja
    "الحي الجديد",            # Hay Jadid
    "المارشان",               # Marchan
    "قصبة الأمين",            # Kasbah Amine
    "غبيلة",                  # Ghbila
    "سيدي بنور",              # Sidi Bennour area

    # French names (appear in French-language articles)
    "Ain Chock",
    "Ain Sebaa",
    "Hay Mohammadi",
    "Sidi Bernoussi",
    "Anfa",
    "Maarif",
    "Roches Noires",
    "Moulay Rachid",
    "Sidi Moumen",
    "Derb Sultan",
    "Hay Hassani",
    "Bourgogne",
    "Gauthier",
    "Racine",
    "Belvedere",
    "Californie",
    "Palmier",
    "Ain Diab",
    "Bouskoura",
    "Habous",
    "Ben Msick",
    "Sbata",
    "Oulfa",
    "Sidi Belyout",
    "Sidi Maarouf",
]

# ─────────────────────────────────────────────────────────────
# FULL MERGED LIST — use this in your scraper filter
# ─────────────────────────────────────────────────────────────
ALL_CASABLANCA_KEYWORDS = (
    CASABLANCA_CITY +
    CASABLANCA_ARRONDISSEMENTS +
    CASABLANCA_NEIGHBORHOODS
)

# ─────────────────────────────────────────────
# 20 NEWS SOURCES CONFIGURATION
# ─────────────────────────────────────────────
SOURCES = [
    # ── TIER 1 ─────────────────────────────
    {
        "name": "hespress",
        "base_url": "https://hespress.com",
        "search_url": "https://hespress.com/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://hespress.com/faits-divers/page/{page}",
        "article_selector": "div.card.overlay, div.card",
        "title_selector": "h3.card-title, h2.card-title",
        "link_selector": "a.stretched-link, h3.card-title a, h2.card-title a",
        "content_selector": "div.post-content, div.article-content, div.single-post",
        "date_selector": "small.text-muted.time, span.date-card, time",
        "max_pages": 50,
        "language": "ar",
        "tier": 1
    },
    {
        "name": "al3omk",
        "base_url": "https://al3omk.com",
        "search_url": "https://al3omk.com/?s=سرقة&paged={page}",
        "category_url": "https://al3omk.com/category/%d8%ac%d8%b1%d9%8a%d9%85%d8%a9-%d9%88%d9%82%d8%b6%d8%a7%d8%a1/page/{page}",
        "article_selector": "article.vcard, article, div.td-module-container",
        "title_selector": "h3.vcard-title a, h3.entry-title a, h2.entry-title a",
        "link_selector": "h3.vcard-title a, h3.entry-title a, h2.entry-title a",
        "content_selector": "div.vcard-content, div.entry-content, div.td-post-content",
        "date_selector": "time, span.td-post-date time",
        "max_pages": 50,
        "language": "ar",
        "tier": 1
    },
    {
        "name": "alaoual",
        "base_url": "https://alaoual.com",
        "search_url": "https://alaoual.com/?s=سرقة&paged={page}",
        "category_url": "https://alaoual.com/accidents/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content, div.post-content",
        "date_selector": "time, span.date",
        "max_pages": 40,
        "language": "ar",
        "tier": 1
    },
    {
        "name": "aldar",
        "base_url": "https://aldar.ma",
        "search_url": "https://aldar.ma/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://aldar.ma/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 40,
        "language": "ar",
        "tier": 1
    },
    {
        "name": "kech24",
        "base_url": "https://kech24.com",
        "search_url": "https://kech24.com/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://kech24.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 30,
        "language": "ar",
        "tier": 1
    },
    {
        "name": "mamlakapress",
        "base_url": "https://mamlakapress.com",
        "search_url": "https://mamlakapress.com/?s=سرقة&paged={page}",
        "category_url": "https://mamlakapress.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 30,
        "language": "ar",
        "tier": 1
    },
    # ── TIER 2 ─────────────────────────────
    {
        "name": "goud",
        "base_url": "https://www.goud.ma",
        "search_url": "https://www.goud.ma/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://www.goud.ma/faits-divers/page/{page}",
        "article_selector": "article.card, article, div.card",
        "title_selector": "h2.card-title a, h3.card-title a, h2 a, h3 a",
        "link_selector": "a.stretched-link, h2.card-title a, h2 a",
        "content_selector": "div.entry-content, div.post-content, div.article-content",
        "date_selector": "time, small.text-muted, span.date",
        "max_pages": 30,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "alyaoum24",
        "base_url": "https://alyaoum24.com",
        "search_url": "https://alyaoum24.com/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://alyaoum24.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 30,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "hibapress",
        "base_url": "https://ar.hibapress.com",
        "search_url": "https://ar.hibapress.com/search?q=سرقة+الدار+البيضاء&page={page}",
        "category_url": "https://ar.hibapress.com/section-19-{page}.html",
        "article_selector": "article, article.main-featured-post, div.post-item",
        "title_selector": "h2.post-title a, h3.post-title a, h2 a, h3 a",
        "link_selector": "h2.post-title a, h3.post-title a, h2 a, h3 a",
        "content_selector": "div.article-details, div.post-content, div.entry-content",
        "date_selector": "time, span.date, span.post-date",
        "max_pages": 30,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "anwarpress",
        "base_url": "https://anwarpress.com",
        "search_url": "https://anwarpress.com/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://anwarpress.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 25,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "chouf",
        "base_url": "https://chouf.com",
        "search_url": "https://chouf.com/?s=سرقة&paged={page}",
        "category_url": "https://chouf.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 25,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "barlamane",
        "base_url": "https://barlamane.com",
        "search_url": "https://barlamane.com/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://barlamane.com/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 25,
        "language": "ar",
        "tier": 2
    },
    {
        "name": "ahdath",
        "base_url": "https://ahdath.info",
        "search_url": "https://ahdath.info/?s=سرقة&paged={page}",
        "category_url": "https://ahdath.info/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 25,
        "language": "ar",
        "tier": 2
    },
    # ── TIER 3 — French ────────────────────
    {
        "name": "le360",
        "base_url": "https://le360.ma",
        "search_url": "https://le360.ma/search?q=vol+Casablanca&page={page}",
        "category_url": "https://le360.ma/tag/faits-divers?page={page}",
        "article_selector": "article, div.article-item",
        "title_selector": "h2 a, h3 a, h2.title a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.article-body, div.content",
        "date_selector": "time, span.date",
        "max_pages": 20,
        "language": "fr",
        "tier": 3
    },
    {
        "name": "h24info",
        "base_url": "https://h24info.ma",
        "search_url": "https://h24info.ma/?s=vol+Casablanca&paged={page}",
        "category_url": "https://h24info.ma/category/faits-divers/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 20,
        "language": "fr",
        "tier": 3
    },
    {
        "name": "yabiladi",
        "base_url": "https://www.yabiladi.com",
        "search_url": "https://www.yabiladi.com/articles/list/10/societe/{page}.html",
        "category_url": "https://www.yabiladi.com/articles/list/10/societe/{page}.html",
        "article_selector": "div.article, article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.article-body",
        "date_selector": "span.date, time",
        "max_pages": 20,
        "language": "fr",
        "tier": 3
    },
    {
        "name": "medias24",
        "base_url": "https://medias24.com",
        "search_url": "https://medias24.com/?s=vol+Casablanca&paged={page}",
        "category_url": "https://medias24.com/category/maroc/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 15,
        "language": "fr",
        "tier": 3
    },
    {
        "name": "telquel",
        "base_url": "https://telquel.ma",
        "search_url": "https://telquel.ma/?s=vol+Casablanca&paged={page}",
        "category_url": "https://telquel.ma/category/maroc/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 15,
        "language": "fr",
        "tier": 3
    },
    {
        "name": "assabah",
        "base_url": "https://assabah.press.ma",
        "search_url": "https://assabah.press.ma/?s=سرقة+الدار+البيضاء&paged={page}",
        "category_url": "https://assabah.press.ma/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 20,
        "language": "ar",
        "tier": 3
    },
    {
        "name": "almassae",
        "base_url": "https://almassae.press.ma",
        "search_url": "https://almassae.press.ma/?s=سرقة&paged={page}",
        "category_url": "https://almassae.press.ma/category/حوادث/page/{page}",
        "article_selector": "article",
        "title_selector": "h2 a, h3 a",
        "link_selector": "h2 a, h3 a",
        "content_selector": "div.entry-content",
        "date_selector": "time",
        "max_pages": 20,
        "language": "ar",
        "tier": 3
    },
]

# ─────────────────────────────────────────────
# HEADERS — rotate to avoid blocks
# ─────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

def get_headers(referer: str = None):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers


# ─────────────────────────────────────────────
# CORE SCRAPING FUNCTIONS
# ─────────────────────────────────────────────

# Reuse a single cloudscraper session (handles Cloudflare JS challenges)
_scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})

def fetch_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    """Fetch a page with retry logic and random delays."""
    # derive base domain for Referer header (helps avoid 400 on Arabic query strings)
    from urllib.parse import urlparse
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}/"
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.5, 4.0))  # polite delay
            response = _scraper.get(
                url,
                headers=get_headers(referer=base),
                timeout=20,
                allow_redirects=True
            )
            if response.status_code == 200:
                response.encoding = response.apparent_encoding
                return BeautifulSoup(response.text, "lxml")
            elif response.status_code == 404:
                logger.warning(f"404 on {url} — stopping pagination")
                return None
            elif response.status_code == 429:
                logger.warning(f"Rate limited on {url} — waiting 30s")
                time.sleep(30)
            else:
                logger.warning(f"Status {response.status_code} on {url}")
        except Exception as e:
            logger.error(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            time.sleep(5 * (attempt + 1))
    return None


def scrape_article_content(url: str, source_config: dict) -> str:
    """Fetch full article content from its URL."""
    soup = fetch_page(url)
    if not soup:
        return ""
    for selector in source_config["content_selector"].split(", "):
        content_div = soup.select_one(selector)
        if content_div:
            return clean_text(content_div.get_text(separator=" ", strip=True))
    # fallback: get all paragraph text
    paragraphs = soup.find_all("p")
    return clean_text(" ".join(p.get_text() for p in paragraphs[:10]))


def scrape_source(source: dict, mode: str = "search") -> list[dict]:
    """
    Scrape a single news source.
    mode: 'search' uses search_url, 'category' uses category_url
    """
    articles = []
    url_template = source["search_url"] if mode == "search" else source["category_url"]
    source_name = source["name"]

    logger.info(f"[{source_name}] Starting scrape — mode={mode}, max_pages={source['max_pages']}")

    for page in range(1, source["max_pages"] + 1):
        url = url_template.format(page=page)
        logger.info(f"  [{source_name}] Page {page}: {url}")

        soup = fetch_page(url)
        if not soup:
            logger.info(f"  [{source_name}] No response on page {page} — stopping")
            break

        # Find all article elements
        article_elements = []
        for selector in source["article_selector"].split(", "):
            found = soup.select(selector)
            if found:
                article_elements.extend(found)
                break

        page_articles = []

        if not article_elements:
            # ── SMART FALLBACK: harvest links by heuristics ──
            logger.info(f"  [{source_name}] Selectors found nothing, trying heuristic link harvest...")
            import re as _re
            article_links = []
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                text = a.get_text(strip=True)
                # Arabic text, >15 chars, looks like an article URL
                if (
                    len(text) > 15
                    and _re.search(r'[\u0600-\u06FF]', text)
                    and _re.search(r'(/\d{4}/|/\d{5,}|\.html|-\d{4,})', href)
                    and not _re.search(r'(category|tag|page|author|search)', href)
                ):
                    article_links.append(a)
            if article_links:
                logger.info(f"  [{source_name}] Heuristic found {len(article_links)} links")
                for a in article_links[:30]:
                    href = a.get('href', '')
                    if not href.startswith('http'):
                        href = source['base_url'].rstrip('/') + '/' + href.lstrip('/')
                    text = clean_text(a.get_text())
                    if not contains_casablanca(text, CASABLANCA_KEYWORDS):
                        continue
                    neighborhood = extract_neighborhood(text, CASABLANCA_KEYWORDS)
                    page_articles.append({
                        "source": source_name,
                        "language": source["language"],
                        "tier": source["tier"],
                        "title": text,
                        "url": href,
                        "date": "",
                        "snippet": text,
                        "neighborhood_detected": neighborhood,
                        "full_content": "",
                        "scraped_at": datetime.now().isoformat(),
                    })
            else:
                logger.info(f"  [{source_name}] No articles found on page {page} — stopping")
                break
        else:
            for elem in article_elements:
                try:
                    # Extract title
                    title = ""
                    for sel in source["title_selector"].split(", "):
                        title_tag = elem.select_one(sel)
                        if title_tag:
                            title = clean_text(title_tag.get_text())
                            break

                    # Extract link — handle stretched-link pattern
                    link = ""
                    for sel in source["link_selector"].split(", "):
                        link_tag = elem.select_one(sel)
                        if link_tag and link_tag.get("href"):
                            href = link_tag["href"]
                            if href.startswith("http"):
                                link = href
                            else:
                                link = source["base_url"].rstrip("/") + "/" + href.lstrip("/")
                            # stretched-link has no text: get title from sibling h2/h3
                            if not title and not link_tag.get_text(strip=True):
                                for htag in ['h2', 'h3', 'h4']:
                                    sibling = elem.select_one(f'{htag}')
                                    if sibling:
                                        title = clean_text(sibling.get_text())
                                        break
                            break

                    # Extract date
                    date = ""
                    for sel in source["date_selector"].split(", "):
                        date_tag = elem.select_one(sel)
                        if date_tag:
                            date = (date_tag.get("datetime") or
                                    date_tag.get("content") or
                                    clean_text(date_tag.get_text()))
                            break

                    # Extract snippet
                    snippet = clean_text(elem.get_text(separator=" ", strip=True))[:500]

                    if not title or not link:
                        continue

                    # ── KEY FILTER: must mention Casablanca ──
                    combined_text = title + " " + snippet
                    if not contains_casablanca(combined_text, CASABLANCA_KEYWORDS):
                        continue

                    neighborhood = extract_neighborhood(combined_text, CASABLANCA_KEYWORDS)

                    article = {
                        "source": source_name,
                        "language": source["language"],
                        "tier": source["tier"],
                        "title": title,
                        "url": link,
                        "date": date,
                        "snippet": snippet,
                        "neighborhood_detected": neighborhood,
                        "full_content": "",   # filled in enrichment step
                        "scraped_at": datetime.now().isoformat(),
                    }
                    page_articles.append(article)

                except Exception as e:
                    logger.error(f"  [{source_name}] Error parsing article: {e}")
                    continue

        logger.info(f"  [{source_name}] Page {page}: {len(page_articles)} Casablanca articles found")
        articles.extend(page_articles)

        # stop if no results 2 pages in a row
        if len(page_articles) == 0 and page > 2:
            break

    logger.info(f"[{source_name}] DONE — Total: {len(articles)} articles")
    return articles


def enrich_articles(articles: list[dict], sources_map: dict) -> list[dict]:
    """Fetch full content for each article."""
    logger.info(f"Enriching {len(articles)} articles with full content...")
    for i, article in enumerate(articles):
        if article.get("full_content"):
            continue
        source_config = sources_map.get(article["source"])
        if source_config and article["url"]:
            logger.info(f"  Enriching {i+1}/{len(articles)}: {article['url'][:60]}")
            article["full_content"] = scrape_article_content(article["url"], source_config)
            # Re-check neighborhood with full content
            if article["full_content"]:
                article["neighborhood_detected"] = extract_neighborhood(
                    article["title"] + " " + article["full_content"],
                    CASABLANCA_KEYWORDS
                )
    return articles


def save_results(articles: list[dict], output_dir: str = "output"):
    """Save results to JSON and CSV."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = os.path.join(output_dir, f"casablanca_crimes_{timestamp}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    # CSV
    csv_path = os.path.join(output_dir, f"casablanca_crimes_{timestamp}.csv")
    if articles:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=articles[0].keys())
            writer.writeheader()
            writer.writerows(articles)

    logger.info(f"Saved {len(articles)} articles → {json_path}")
    logger.info(f"Saved CSV → {csv_path}")
    return json_path, csv_path
