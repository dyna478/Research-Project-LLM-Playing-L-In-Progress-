"""
avito_rent_scraper.py
─────────────────────────────────────────────────────────
Scrapes apartment rental listings from Avito.ma for
key Casablanca neighborhoods to build a socio-economic
wealth layer for the CrimeMind ABM environment.

For each neighborhood it extracts:
  - Price (DH/month)
  - Surface area (m²)
  - Computed price/m²

Then computes per-neighborhood statistics:
  - median_price, mean_price, min_price, max_price
  - median_price_per_m2 (the cleanest "Wealth Index")
  - std_dev, listing_count
  - wealth_category (1 = Very Poor → 5 = Very Rich)
  - wealth_label
  - lat/lng centroid for map rendering

Output:
  output/rent_data.json

Usage:
  python avito_rent_scraper.py               # all neighborhoods
  python avito_rent_scraper.py --test        # 1 neighborhood, 1 page
  python avito_rent_scraper.py --pages 1     # faster, 1 page per neighborhood
─────────────────────────────────────────────────────────
"""

import re
import json
import time
import random
import argparse
import statistics
from pathlib import Path
from datetime import datetime

try:
    import cloudscraper
    scraper = cloudscraper.create_scraper()
except ImportError:
    import requests as scraper_module
    import requests
    class FallbackScraper:
        def get(self, url, headers=None, timeout=30):
            return requests.get(url, headers=headers, timeout=timeout)
    scraper = FallbackScraper()

OUTPUT_DIR = Path(__file__).parent / "output"

# ─── Neighborhoods to scrape ──────────────────────────────────────────────────
# Format: (display_name, avito_slug, centroid_lat, centroid_lng, location_keywords)
# location_keywords: list of strings that MUST appear in the listing's location
# label to be accepted. Multiple variants handle Avito's inconsistent naming.
NEIGHBORHOODS = [
    # Very poor / dense informal
    ("Hay Mohammadi",   "hay-mohammadi",    33.590, -7.587,
     ["hay mohammadi", "hay mohamadi", "hay-mohammadi"]),
    ("Sidi Moumen",     "sidi-moumen",      33.598, -7.554,
     ["sidi moumen", "sidi-moumen"]),
    ("Moulay Rachid",   "moulay-rachid",    33.543, -7.591,
     ["moulay rachid", "moulay-rachid"]),

    # Working class / mid-low
    ("Ain Sebaa",       "ain-sebaa",        33.600, -7.580,
     ["ain sebaa", "ain-sebaa", "aïn sebaâ", "ain sebaâ"]),
    ("Sidi Bernoussi",  "sidi-bernoussi",   33.603, -7.620,
     ["sidi bernoussi", "bernoussi"]),
    ("Ben Msick",       "ben-msick",        33.553, -7.600,
     ["ben msick", "ben-msick", "benmsick", "benmssick"]),

    # Middle class
    ("Maarif",          "maarif",           33.574, -7.654,
     ["maarif", "maârif"]),
    ("Hay Hassani",     "hay-hassani",      33.549, -7.634,
     ["hay hassani", "hay-hassani", "hay el hassani"]),

    # Upper-middle
    ("Bourgogne",       "bourgogne",        33.582, -7.637,
     ["bourgogne"]),
    ("Ain Chock",       "ain-chock",        33.557, -7.538,
     ["ain chock", "ain-chock", "aïn chock"]),

    # Rich / Chic
    ("Anfa",            "anfa-place",       33.585, -7.650,
     ["anfa", "val fleuri", "californie"]),
    ("Racine",          "racine",           33.580, -7.648,
     ["racine"]),
    ("Gauthier",        "gauthier",         33.577, -7.636,
     ["gauthier"]),
    ("Ain Diab",        "ain-diab",         33.593, -7.697,
     ["ain diab", "ain-diab", "aïn diab"]),
]

BASE_URL = "https://www.avito.ma/sp/immobilier/location-appartement-{slug}-casablanca"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ─── Regex patterns ────────────────────────────────────────────────────────────
PRICE_RE = re.compile(r"(\d[\d\s,\.]{0,8})\s*DH", re.IGNORECASE)
SURFACE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m[²2]", re.IGNORECASE)

# Avito listing URL anchor — used to split HTML into per-listing blocks
# Each listing has a URL like: /fr/hay_mohammadi/appartements/...
LISTING_URL_RE = re.compile(
    r'https://www\.avito\.ma/fr/[^/]+/appartements/[^\s"\'<>]+\.htm'
)

# Avito location label — matches "Casablanca, Hay Mohammadi" style text
LOCATION_RE = re.compile(
    r'Casablanca,\s*([A-Za-zÀ-ÿ\s\-\']+)',
    re.IGNORECASE
)


def clean_price(raw: str) -> float | None:
    raw = raw.replace(" ", "").replace(",", "").replace(".", "")
    try:
        v = float(raw)
        if 800 <= v <= 150_000:
            return v
    except ValueError:
        pass
    return None


def clean_surface(raw: str) -> float | None:
    raw = raw.replace(",", ".")
    try:
        v = float(raw)
        if 10 <= v <= 1000:
            return v
    except ValueError:
        pass
    return None


def fetch_page(slug: str, page: int) -> str | None:
    """Fetch a single Avito listing page, returns raw HTML/text."""
    url = BASE_URL.format(slug=slug)
    if page > 1:
        url += f"?pagenumber={page}"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-MA,fr;q=0.9,ar;q=0.8,en;q=0.7",
        "Referer": "https://www.avito.ma",
    }

    for attempt in range(3):
        try:
            resp = scraper.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"    [429] Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code == 403:
                print(f"    [403] Blocked on page {page}, skipping...")
                return None
            else:
                print(f"    [HTTP {resp.status_code}] {url}")
                return None
        except Exception as e:
            print(f"    [Error] {e}")
            time.sleep(5)
    return None


def is_correct_neighborhood(block: str, keywords: list[str]) -> bool:
    """
    Return True only if the listing block's location text contains
    at least one of the target neighborhood keywords.
    Case-insensitive, accent-tolerant check.
    """
    block_lower = block.lower()
    # Remove accents for simpler matching
    import unicodedata
    block_norm = ''.join(
        c for c in unicodedata.normalize('NFD', block_lower)
        if unicodedata.category(c) != 'Mn'
    )
    for kw in keywords:
        kw_norm = ''.join(
            c for c in unicodedata.normalize('NFD', kw.lower())
            if unicodedata.category(c) != 'Mn'
        )
        if kw_norm in block_norm:
            return True
    return False


def parse_listings(html: str, location_keywords: list[str]) -> list[dict]:
    """
    Split the Avito page HTML into per-listing blocks using the listing
    URL anchors. For each block:
      1. Check that the location label matches our target neighborhood.
      2. Extract price + surface from that block only.

    This prevents cross-neighborhood contamination from "Résultats similaires".
    """
    # Split on listing URL boundaries
    parts = LISTING_URL_RE.split(html)
    # parts[0] = page header, parts[1:] = content after each listing URL
    # We interleave: listing_url, content_after_url, listing_url, ...
    urls = LISTING_URL_RE.findall(html)

    listings = []
    rejected = 0

    # Zip each URL with the content block that follows it
    for url, block in zip(urls, parts[1:]):
        # Take only the first ~1500 chars of the block — enough for one listing card
        snippet = block[:1500]

        # ── Location filter ────────────────────────────────────────
        if not is_correct_neighborhood(snippet, location_keywords):
            rejected += 1
            continue

        # ── Extract price ──────────────────────────────────────────
        price = None
        for m in PRICE_RE.finditer(snippet):
            p = clean_price(m.group(1))
            if p:
                price = p
                break  # first valid price in block wins

        if not price:
            continue

        # ── Extract surface ────────────────────────────────────────
        surface = None
        for m in SURFACE_RE.finditer(snippet):
            s = clean_surface(m.group(1))
            if s:
                surface = s
                break

        entry = {"price": price, "surface": surface}
        if surface:
            entry["price_per_m2"] = round(price / surface, 1)
        listings.append(entry)

    if rejected:
        print(f" [{rejected} cross-neighborhood listings filtered]", end="")

    return listings



def deduplicate(listings: list[dict]) -> list[dict]:
    """Remove near-duplicate prices (same price ± 5 DH, same surface)."""
    seen = set()
    out = []
    for L in listings:
        key = (round(L["price"] / 100) * 100, L.get("surface"))
        if key not in seen:
            seen.add(key)
            out.append(L)
    return out


def compute_stats(listings: list[dict]) -> dict:
    """Compute statistics from a list of listing dicts."""
    prices = [L["price"] for L in listings]
    ppms  = [L["price_per_m2"] for L in listings if "price_per_m2" in L]

    if not prices:
        return {}

    stats = {
        "listing_count":      len(prices),
        "median_price":       round(statistics.median(prices)),
        "mean_price":         round(statistics.mean(prices)),
        "min_price":          round(min(prices)),
        "max_price":          round(max(prices)),
        "std_dev_price":      round(statistics.stdev(prices) if len(prices) > 1 else 0),
    }

    if ppms:
        stats["median_price_per_m2"] = round(statistics.median(ppms), 1)
        stats["mean_price_per_m2"]   = round(statistics.mean(ppms), 1)

    return stats


def assign_wealth_category(median_ppm2: float | None, median_price: float) -> tuple[int, str]:
    """
    Assign a 1–5 wealth category based on median price/m² or raw price.
    Thresholds calibrated for Casablanca 2024–2025 market.
    """
    # Use price_per_m2 if available, else fall back to raw price
    if median_ppm2 is not None:
        if median_ppm2 < 40:
            return 1, "Très défavorisé"      # Very Poor
        elif median_ppm2 < 70:
            return 2, "Défavorisé"           # Poor
        elif median_ppm2 < 110:
            return 3, "Classe moyenne"       # Middle
        elif median_ppm2 < 180:
            return 4, "Classe aisée"         # Upper-Middle
        else:
            return 5, "Aisé / Chic"          # Rich
    else:
        if median_price < 2_500:
            return 1, "Très défavorisé"
        elif median_price < 5_000:
            return 2, "Défavorisé"
        elif median_price < 9_000:
            return 3, "Classe moyenne"
        elif median_price < 18_000:
            return 4, "Classe aisée"
        else:
            return 5, "Aisé / Chic"


WEALTH_COLORS = {
    1: "#ef4444",   # red    — Very Poor
    2: "#f97316",   # orange — Poor
    3: "#eab308",   # yellow — Middle
    4: "#22c55e",   # green  — Upper-Middle
    5: "#3b82f6",   # blue   — Rich/Chic
}


# ─── Main Scraping Loop ───────────────────────────────────────────────────────

def scrape_neighborhood(name: str, slug: str, lat: float, lng: float,
                        location_keywords: list[str],
                        pages: int = 2) -> dict:
    """Scrape N pages of Avito for one neighborhood and return full result."""
    print(f"\n  [{name}] ({slug})")
    all_listings = []

    for page in range(1, pages + 1):
        print(f"    Page {page}/{pages}...", end=" ", flush=True)
        html = fetch_page(slug, page)
        if not html:
            print("SKIP")
            break

        listings = parse_listings(html, location_keywords)
        print(f" → {len(listings)} valid listings")
        all_listings.extend(listings)

        # Polite delay between pages
        time.sleep(random.uniform(2.5, 5.0))

    # Deduplicate
    all_listings = deduplicate(all_listings)
    print(f"    → {len(all_listings)} unique listings after dedup")

    stats = compute_stats(all_listings)

    median_ppm2 = stats.get("median_price_per_m2")
    median_price = stats.get("median_price", 0)
    wealth_cat, wealth_label = assign_wealth_category(median_ppm2, median_price)

    return {
        "name":           name,
        "slug":           slug,
        "lat":            lat,
        "lng":            lng,
        "stats":          stats,
        "wealth_category": wealth_cat,
        "wealth_label":   wealth_label,
        "wealth_color":   WEALTH_COLORS[wealth_cat],
        "listings":       all_listings,
    }


def main():
    parser = argparse.ArgumentParser(description="Avito rent scraper for Casablanca neighborhoods")
    parser.add_argument("--test",   action="store_true", help="Run on 1 neighborhood, 1 page only")
    parser.add_argument("--pages",  type=int, default=2,  help="Pages per neighborhood (default: 2)")
    args = parser.parse_args()

    neighborhoods = NEIGHBORHOODS[:1] if args.test else NEIGHBORHOODS
    pages = 1 if args.test else args.pages

    print("=" * 62)
    print("  Avito Rent Scraper — Casablanca Neighborhoods")
    print(f"  Neighborhoods: {len(neighborhoods)}  |  Pages/nbh: {pages}")
    print("=" * 62)

    results = []
    for name, slug, lat, lng, keywords in neighborhoods:
        result = scrape_neighborhood(name, slug, lat, lng, keywords, pages=pages)
        results.append(result)
        # Polite delay between neighborhoods
        if len(neighborhoods) > 1:
            wait = random.uniform(4, 8)
            print(f"    Waiting {wait:.1f}s before next neighborhood...")
            time.sleep(wait)

    # ── Summary table ────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  RESULTS SUMMARY")
    print("=" * 62)
    print(f"  {'Neighborhood':<20} {'Listings':>8} {'Median DH':>10} {'DH/m²':>7}  {'Category'}")
    print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*7}  {'-'*20}")
    for r in sorted(results, key=lambda x: x["stats"].get("median_price", 0)):
        s = r["stats"]
        ppm2 = f"{s.get('median_price_per_m2', '?')}"
        print(f"  {r['name']:<20} {s.get('listing_count', 0):>8} "
              f"{s.get('median_price', 0):>10,}  {ppm2:>6}  "
              f"[{r['wealth_category']}] {r['wealth_label']}")

    # ── Save output ──────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "neighborhoods": results,
        "wealth_colors": WEALTH_COLORS,
        "thresholds": {
            "1": "< 40 DH/m² — Très défavorisé",
            "2": "40–70 DH/m² — Défavorisé",
            "3": "70–110 DH/m² — Classe moyenne",
            "4": "110–180 DH/m² — Classe aisée",
            "5": "> 180 DH/m² — Aisé / Chic",
        }
    }
    json_path = OUTPUT_DIR / "rent_data.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Saved {len(results)} neighborhoods → {json_path}")
    print("✅ Done!")


if __name__ == "__main__":
    main()
