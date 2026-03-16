import cloudscraper
from bs4 import BeautifulSoup, NavigableString
import json
import csv
import time
import random
import logging
from datetime import datetime
import os
import re

# ====================== CONFIG ======================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
]

NEIGHBORHOOD_KEYWORDS = [
    "الدار البيضاء", "البيضاء", "عين الشق", "سيدي البرنوصي", "عين السبع", "مولاي رشيد",
    "الحي المحمدي", "الحي الحسني", "أنفا", "سيدي عثمان", "الصخور السوداء", "سيدي مومن",
    "بوسكورة", "درب السلطان", "المعاريف", "التضامن", "Casablanca"
]

# pagination_style: 'wp'  → example.com/?s=kw&paged=N   (WordPress default)
#                  'tag' → example.com/tag/kw/page/N/   (tag archive)
#                  'page'→ example.com/search/kw/page/N/ (custom)
SITES_CONFIG = [
    {"name": "hespress",    "base": "https://hespress.com",      "search_template": "https://hespress.com/?s={keyword}",             "pagination_style": "wp"},
    {"name": "al3omk",     "base": "https://al3omk.com",        "search_template": "https://al3omk.com/tag/{keyword}/",             "pagination_style": "tag"},
    {"name": "alaoual",    "base": "https://alaoual.com",       "search_template": "https://alaoual.com/?s={keyword}",              "pagination_style": "wp"},
    {"name": "aldar",      "base": "https://aldar.ma",          "search_template": "https://aldar.ma/?s={keyword}",                 "pagination_style": "wp"},
    {"name": "kech24",     "base": "https://kech24.com",        "search_template": "https://kech24.com/?s={keyword}",               "pagination_style": "wp"},
    {"name": "mamlakapress","base": "https://mamlakapress.com",  "search_template": "https://mamlakapress.com/?s={keyword}",         "pagination_style": "wp"},
    {"name": "goud",       "base": "https://goud.ma",           "search_template": "https://goud.ma/?s={keyword}",                  "pagination_style": "wp"},
    {"name": "alyaoum24",  "base": "https://alyaoum24.com",     "search_template": "https://alyaoum24.com/?s={keyword}",            "pagination_style": "wp"},
    {"name": "hibapress",  "base": "https://hibapress.com",     "search_template": "https://hibapress.com/?s={keyword}",            "pagination_style": "wp"},
    {"name": "anwarpress", "base": "https://anwarpress.com",    "search_template": "https://anwarpress.com/?s={keyword}",           "pagination_style": "wp"},
    {"name": "chouf",      "base": "https://chouftv.ma",        "search_template": "https://chouftv.ma/?s={keyword}",               "pagination_style": "wp"},
    {"name": "barlamane",  "base": "https://barlamane.com",     "search_template": "https://barlamane.com/?s={keyword}",            "pagination_style": "wp"},
    {"name": "ahdath",     "base": "https://ahdath.info",       "search_template": "https://ahdath.info/?s={keyword}",              "pagination_style": "wp"},
    {"name": "le360",      "base": "https://le360.ma",          "search_template": "https://le360.ma/?s={keyword}",                 "pagination_style": "wp"},
    {"name": "h24info",    "base": "https://h24info.ma",        "search_template": "https://h24info.ma/?s={keyword}",               "pagination_style": "wp"},
    {"name": "yabiladi",   "base": "https://yabiladi.com",      "search_template": "https://yabiladi.com/?s={keyword}",             "pagination_style": "wp"},
    {"name": "medias24",   "base": "https://medias24.com",      "search_template": "https://medias24.com/?s={keyword}",             "pagination_style": "wp"},
    {"name": "telquel",    "base": "https://telquel.ma",        "search_template": "https://telquel.ma/?s={keyword}",               "pagination_style": "wp"},
    {"name": "assabah",    "base": "https://assabah.ma",        "search_template": "https://assabah.ma/?s={keyword}",               "pagination_style": "wp"},
    {"name": "almassae",   "base": "https://almassae.com",      "search_template": "https://almassae.com/?s={keyword}",             "pagination_style": "wp"},
]

OUTPUT_DIR = "output"
JSONL_FILE = os.path.join(OUTPUT_DIR, "casablanca_theft_articles.jsonl")
CSV_FILE   = os.path.join(OUTPUT_DIR, "casablanca_theft_articles.csv")
LOG_FILE   = os.path.join(OUTPUT_DIR, "scraper.log")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()]
)

# ====================== SCRAPER SESSION ======================
_scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False}
)

# ====================== HELPERS ======================
def get_random_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def safe_request(url, retries=3):
    for attempt in range(retries):
        try:
            headers = get_random_headers()
            r = _scraper.get(url, headers=headers, timeout=20)
            if r.status_code == 429:
                wait = 30 + random.uniform(0, 10)
                logging.warning(f"Rate limited (429) on {url} → sleeping {wait:.1f}s")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                logging.warning(f"404 on {url} → stopping pagination for this source")
                return None
            r.raise_for_status()
            return r
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            logging.error(f"Request failed {url} (attempt {attempt+1}/{retries}): {e} → retry in {wait:.1f}s")
            time.sleep(wait)
    return None

# ─────────────────────────────────────────────────────────────
# CITY IDENTIFIERS — for city-level confirmation only
# ─────────────────────────────────────────────────────────────
CASABLANCA_CITY = [
    "الدار البيضاء", "الدارالبيضاء", "البيضاء", "Casablanca", "Casa",
]

# ─────────────────────────────────────────────────────────────
# OFFICIAL ARRONDISSEMENTS (administrative divisions)
# ─────────────────────────────────────────────────────────────
CASABLANCA_ARRONDISSEMENTS = [
    "أنفا", "المعاريف", "سيدي بليوط", "بورغون",
    "عين الشق", "سيدي عثمان",
    "عين السبع", "الحي المحمدي",
    "بن مسيك", "سباتة",
    "سيدي البرنوصي", "الصخور السوداء",
    "الفداء", "درب السلطان", "مرس السلطان",
    "الحي الحسني",
    "مولاي رشيد", "سيدي مومن",
]

# ─────────────────────────────────────────────────────────────
# POPULAR NEIGHBORHOODS & QUARTIERS
# ─────────────────────────────────────────────────────────────
# NOTE: only put names that are UNIQUE to Casablanca here.
# Generic Arabic words (السلام, الرحمة, الفتح…) exist in every Moroccan city
# and are handled as WEAK matches below — they only count alongside a strong kw.
CASABLANCA_NEIGHBORHOODS = [
    "غوتييه", "راسين", "بلفدير", "الكاليفورنيا",
    "عين دياب", "سيدي معروف", "بوسيجور",
    "الولفة", "مبروكة", "الحبوس",
    "درب غلف", "درب الكبير", "درب كوبا", "سيدي أبي عثمان",
    "الكاريان", "سيدي مؤمن", "حي الفرح", "عين الذياب",
    "بوسكورة", "تيط مليل", "عين الحرودة", "لسان المدينة",
    "ابن امسيك", "الإدريسية", "المدينة القديمة",
    "عين البرجة", "المارشان",
    "قصبة الأمين", "غبيلة",
    # French names (highly specific to Casa)
    "Ain Chock", "Ain Sebaa", "Hay Mohammadi", "Sidi Bernoussi",
    "Anfa", "Maarif", "Roches Noires", "Moulay Rachid",
    "Sidi Moumen", "Derb Sultan", "Hay Hassani", "Bourgogne",
    "Gauthier", "Racine", "Belvedere", "Californie", "Palmier",
    "Ain Diab", "Bouskoura", "Habous", "Ben Msick", "Sbata",
    "Oulfa", "Sidi Belyout", "Sidi Maarouf",
]

# Generic neighborhood words that exist across ALL Moroccan cities.
# Only count if a STRONG Casablanca keyword is also present.
_GENERIC_NEIGHBORHOODS = [
    "السلام", "الرحمة", "الهناء", "التضامن", "الفتح",
    "الحسين", "الفاروق", "النصر", "الإنارة",
    "الحي الجديد", "النخيل",
]

_DISQUALIFY_BEFORE_BAIDAA = [
    "الأسلحة", "السلاح", "أسلحة", "بالأسلحة", "بالسلاح",
    "الخيول", "الورقة", "الكرة",
]
_OTHER_CITIES = [
    "فاس", "مراكش", "الرباط", "طنجة", "أكادير", "مكناس", "وجدة",
    "تطوان", "القنيطرة", "سلا", "بني ملال", "الجديدة", "الحسيمة",
    "تازة", "خريبكة", "سطات", "الناظور", "بنسودة",
    "Fes", "Marrakech", "Rabat", "Tanger", "Agadir", "Meknes", "Oujda",
]

def _has_strong_casablanca(text: str) -> bool:
    """True if text contains an unambiguous Casablanca reference."""
    # Guard عين الشق — must be exact word, not part of عين الشقف (Fes street)
    for kw in CASABLANCA_ARRONDISSEMENTS:
        if kw == "عين الشق":
            if re.search(r'عين الشق(?!ف|ة)', text):
                return True
        elif kw in text:
            return True
    for kw in CASABLANCA_NEIGHBORHOODS:
        if kw in text:
            return True
    for kw in ["الدار البيضاء", "الدارالبيضاء", "Casablanca", "Casa"]:
        if kw in text:
            return True
    return False

def _has_other_city(text: str) -> bool:
    return any(city in text for city in _OTHER_CITIES)

def is_casablanca_related(text):
    """Return (True, keyword) if text genuinely refers to Casablanca."""
    if not text:
        return False, None
    # Strong: unambiguous city string
    for kw in ["الدار البيضاء", "الدارالبيضاء", "Casablanca", "Casa"]:
        if kw in text:
            return True, kw
    # Strong: arrondissements (with boundary guard for عين الشق)
    for kw in CASABLANCA_ARRONDISSEMENTS:
        if kw == "عين الشق":
            if re.search(r'عين الشق(?!ف|ة)', text):
                return True, kw
        elif kw in text:
            # مولاي رشيد can be a person name — require city co-occurrence if isolated
            if kw == "مولاي رشيد" and not any(c in text for c in ["الدار البيضاء", "الدارالبيضاء", "Casa", "البيضاء"]):
                # Check if the article mentions another city → reject
                if _has_other_city(text):
                    continue
            return True, kw
    # Strong: specific neighborhoods
    for kw in CASABLANCA_NEIGHBORHOODS:
        if kw in text:
            return True, kw
    # Weak: generic neighborhood names — only if no OTHER city is named
    for kw in _GENERIC_NEIGHBORHOODS:
        if kw in text:
            if _has_other_city(text):
                return False, None  # That city's own neighborhood, not Casa
            return True, kw  # Probably Casa — accept
    # Weakest: البيضاء with context guard
    for m in re.finditer(r'البيضاء', text):
        prefix = text[max(0, m.start() - 35): m.start()]
        if not any(dq in prefix for dq in _DISQUALIFY_BEFORE_BAIDAA):
            if _has_other_city(text):
                return False, None
            return True, "البيضاء"
    return False, None

def extract_best_neighborhood(text: str, fallback: str) -> str:
    """Find most specific Casablanca neighborhood in text (arrondissements first)."""
    for nb in CASABLANCA_ARRONDISSEMENTS:
        if nb in text:
            return nb
    for nb in CASABLANCA_NEIGHBORHOODS:
        if nb in text:
            return nb
    return fallback or "الدار البيضاء"


def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text.strip())
    return text

def extract_full_content(soup):
    selectors = [
        "div.post-content",
        "div.entry-content",
        "div.article-body",
        "div.td-post-content",
        "article p",
    ]
    for sel in selectors:
        elements = soup.select(sel)
        if elements:
            paragraphs = [clean_text(p.get_text()) for p in elements if isinstance(p, NavigableString) or p.name == 'p' or len(clean_text(p.get_text())) > 20]
            content = "\n\n".join(paragraphs)
            if len(content) > 100:
                return content
    # Fallback: all paragraphs on page
    paragraphs = [clean_text(p.get_text()) for p in soup.find_all("p") if len(clean_text(p.get_text())) > 30]
    content = "\n\n".join(paragraphs)
    return content if len(content) > 100 else ""

def extract_articles_from_page(soup, base_url):
    """Tries common Moroccan news layouts. Very robust."""
    articles = []
    cards = (
        soup.find_all("article") or
        soup.select("div[class*='post']") or
        soup.select("div[class*='article']") or
        soup.select("div.td-block-span6") or
        soup.select("div.news-item") or
        soup.select("div.item")
    )
    for card in cards[:30]:  # safety
        try:
            title_tag = card.find("h2") or card.find("h3") or card.find("h1")
            a_tag = title_tag.find("a") if title_tag else card.find("a")
            if not a_tag or not a_tag.get("href"):
                continue
            title = clean_text(a_tag.get_text())
            url = a_tag["href"]
            if not url.startswith("http"):
                url = base_url.rstrip("/") + "/" + url.lstrip("/")
            # Date
            date_tag = card.find("time") or card.find(attrs={"class": re.compile(r"date|time|meta", re.I)})
            date_str = clean_text(date_tag.get_text()) if date_tag else ""
            # Snippet
            snippet_tag = card.find("p") or card.find(attrs={"class": re.compile(r"excerpt|summary|desc", re.I)})
            snippet = clean_text(snippet_tag.get_text()) if snippet_tag else ""
            if title and len(title) > 5:
                articles.append({
                    "title": title,
                    "url": url,
                    "date_raw": date_str,
                    "snippet": snippet
                })
        except:
            continue
    return articles

# ====================== MAIN ======================
def load_scraped_urls() -> set:
    """Load URLs already saved to JSONL to avoid re-scraping."""
    seen = set()
    if os.path.exists(JSONL_FILE):
        with open(JSONL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rec = json.loads(line)
                        url = rec.get("url")
                        if url:
                            seen.add(url)
                    except json.JSONDecodeError:
                        pass
    logging.info(f"📋 Loaded {len(seen)} already-scraped URLs — will skip duplicates")
    return seen

def main():
    logging.info("=== Casablanca Theft Scraper Started ===")
    keyword = "سرقة"
    all_articles = []
    processed_count = 0
    scraped_urls = load_scraped_urls()  # ← deduplication set

    for site in SITES_CONFIG:
        site_name = site["name"]
        logging.info(f"🔍 Starting {site_name.upper()} ...")
        search_url = site["search_template"].format(keyword=keyword)
        new_on_page = 0  # initialize here so it's always defined after the loop

        for page in range(1, 51):
            # ── Build correct page URL per pagination style ──────────────────
            style = site.get("pagination_style", "wp")
            if page == 1:
                page_url = search_url
            elif style == "tag":
                # e.g. https://al3omk.com/tag/سرقة/page/2/
                page_url = search_url.rstrip("/") + f"/page/{page}/"
            elif style == "wp":
                # WordPress ?s= with &paged=N
                sep = "&" if "?" in search_url else "?"
                page_url = f"{search_url}{sep}paged={page}"
            else:
                page_url = search_url.rstrip("/") + f"/page/{page}/"

            logging.info(f"   Page {page} → {page_url}")
            resp = safe_request(page_url)
            if not resp:
                break
            soup = BeautifulSoup(resp.text, "lxml")

            page_articles = extract_articles_from_page(soup, site["base"])
            if not page_articles:
                logging.info(f"   No more articles on page {page} → stopping site")
                break

            new_on_page = 0
            all_seen_page = True  # innocent until proven otherwise
            for art in page_articles:
                # Skip if already scraped in a previous run
                if art["url"] in scraped_urls:
                    continue
                all_seen_page = False  # at least one fresh article exists
                new_on_page += 1

                # Step 1-2: Quick filter
                related, detected = is_casablanca_related(art["title"] + " " + art["snippet"])
                full_content = ""
                fetch_status = "success"

                if not related:
                    # Fetch full body to confirm
                    time.sleep(random.uniform(1.5, 3))
                    detail_resp = safe_request(art["url"])
                    if detail_resp:
                        detail_soup = BeautifulSoup(detail_resp.text, "lxml")
                        full_content = extract_full_content(detail_soup)
                        related, detected = is_casablanca_related(full_content)
                    else:
                        fetch_status = "failed_body_fetch"

                if not related:
                    continue  # Not Casablanca

                # Step 3: If we didn't fetch body yet, do it now
                if not full_content:
                    time.sleep(random.uniform(1.5, 3))
                    detail_resp = safe_request(art["url"])
                    if detail_resp:
                        detail_soup = BeautifulSoup(detail_resp.text, "lxml")
                        full_content = extract_full_content(detail_soup)
                    else:
                        fetch_status = "failed_body_fetch"
                        full_content = ""

                if len(full_content) < 100:
                    fetch_status = "failed_min_length"

                # Try to refine to a specific neighborhood using the full content
                neighborhood = extract_best_neighborhood(
                    art["title"] + " " + full_content, detected
                )

                record = {
                    "source": site_name,
                    "title": art["title"],
                    "url": art["url"],
                    "date": art["date_raw"] or datetime.now().strftime("%Y-%m-%d"),
                    "snippet": art["snippet"],
                    "full_content": full_content,
                    "neighborhood_detected": neighborhood,
                    "fetch_status": fetch_status,
                    "content_length": len(full_content),
                    "scraped_at": datetime.now().isoformat()
                }

                # Save incrementally and track URL
                with open(JSONL_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                scraped_urls.add(art["url"])  # prevent same-run duplicates

                all_articles.append(record)
                processed_count += 1

                if processed_count % 50 == 0:
                    logging.info(f"💾 Checkpoint: {processed_count} articles saved")

                time.sleep(random.uniform(1.5, 3))

            # If EVERY article on the page was already in the JSONL,
            # we've caught up with previously scraped content → stop this site
            if all_seen_page:
                logging.info(f"   All articles on page {page} already seen — stopping {site_name}")
                break

        logging.info(f"✅ Finished {site_name} — {new_on_page} new articles this run")

    # Final CSV
    if all_articles:
        with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_articles[0].keys())
            writer.writeheader()
            writer.writerows(all_articles)
        logging.info(f"🎉 DONE! {processed_count} Casablanca theft articles saved to {JSONL_FILE} + {CSV_FILE}")

if __name__ == "__main__":
    main()
