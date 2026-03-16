#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper for Moroccan news websites to collect articles related to theft
in Casablanca and its neighbourhoods.

This script iterates through a list of target websites, searches each for
the Arabic keyword "سرقة" (theft/robbery), paginates through results,
filters for articles mentioning Casablanca or its neighbourhoods, fetches
the full article content, and saves the results to JSON and CSV files.

Key features:

* Rotates user‐agents on every HTTP request to mimic different browsers.
* Adds a random delay (1.5–3 seconds) between requests to reduce the risk
  of triggering anti‐scraping mechanisms.
* Retries failed requests up to three times with exponential backoff.
* Handles HTTP 429 (Too Many Requests) by waiting 30 seconds before
  retrying.
* Stops paginating when a 404 (Not Found) is encountered for a given site.
* Extracts article content using a sequence of CSS selectors, with a
  fallback to all paragraph tags.
* Filters articles by checking for Casablanca neighbourhood names in the
  title, snippet, or full body.
* Saves progress every 50 articles so that work is not lost if the process
  terminates prematurely.
* Logs all actions and errors to both the console and a log file.

To run the scraper, execute:

    python main.py

The script will produce JSON and CSV files in the working directory,
named with the current date and time. Logs will also be saved to
``scraper.log``.
"""

import csv
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional, Tuple, Dict

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    # Python ≥3.9 has zoneinfo in the standard library
    from zoneinfo import ZoneInfo  # type: ignore
except ImportError:
    # For older Python versions, use backports.zoneinfo if available
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except ImportError:
        ZoneInfo = None  # type: ignore


###############################################################################
# Configuration
###############################################################################

# Keyword to search for theft/robbery in Arabic
SEARCH_KEYWORD = "سرقة"

# List of target websites (domain names or full base URLs).  Each entry
# should include the scheme (https://) so that URLs are formed correctly.
TARGET_SITES = [
    "https://hespress.com",
    "https://al3omk.com",
    "https://alaoual.com",
    "https://aldar.ma",
    "https://kech24.com",
    "https://mamlakapress.com",
    "https://goud.ma",
    "https://alyaoum24.com",
    "https://hibapress.com",
    "https://anwarpress.com",
    "https://chouf.com",
    "https://barlamane.com",
    "https://ahdath.info",
    "https://le360.ma",
    "https://h24info.ma",
    "https://yabiladi.com",
    "https://medias24.com",
    "https://telquel.ma",
    "https://assabah.ma",
    "https://almassae.com",
]

# List of Casablanca neighbourhoods and variations to look for.  If any of
# these appear in the title, snippet, or body of an article, the article
# will be kept.  Note: spaces and casing are ignored when matching.
CASA_NEIGHBOURHOODS = [
    "الدار البيضاء",
    "البيضاء",
    "عين الشق",
    "سيدي البرنوصي",
    "عين السبع",
    "مولاي رشيد",
    "الحي المحمدي",
    "الحي الحسني",
    "أنفا",
    "سيدي عثمان",
    "الصخور السوداء",
    "سيدي مومن",
    "بوسكورة",
    "درب السلطان",
    "المعاريف",
    "التضامن",
    "casablanca",
]

# Minimum article body length in characters.  If the scraped body is
# shorter than this, the article is marked as a failed fetch.
MIN_BODY_LENGTH = 100

# Maximum number of result pages to paginate for each site
MAX_PAGES_PER_SITE = 50

# Number of articles after which progress is saved
SAVE_EVERY_N_ARTICLES = 50

# Output file templates.  The current date/time will be inserted when
# saving final results.
OUTPUT_JSON_TEMPLATE = "scraped_articles_{timestamp}.json"
OUTPUT_CSV_TEMPLATE = "scraped_articles_{timestamp}.csv"

# Logging configuration
LOG_FILE = "scraper.log"
LOG_LEVEL = logging.INFO


###############################################################################
# Data structures
###############################################################################

@dataclass
class ArticleRecord:
    """Dataclass representing a scraped article."""
    source: str
    title: str
    url: str
    date: str
    snippet: str
    full_content: str
    neighbourhood_detected: str
    fetch_status: str
    content_length: int
    scraped_at: str

    def to_json_compatible(self) -> Dict[str, str]:
        """Return the dataclass as a dictionary for JSON serialization."""
        return asdict(self)


###############################################################################
# Utility functions
###############################################################################

def setup_logging(log_file: str, level: int) -> None:
    """Configure logging to output to console and a log file."""
    logger = logging.getLogger()
    logger.setLevel(level)

    # Formatter with time, level, and message
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Stream handler for console
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def get_random_user_agent() -> str:
    """Return a random User-Agent string.

    A small set of user agents is defined here.  For more comprehensive
    rotation, this list can be expanded or loaded from an external source.
    """
    user_agents = [
        # Desktop browsers
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.70 Safari/537.36",
        # Mobile browsers
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15A5341f Safari/604.1",
    ]
    return random.choice(user_agents)


def create_http_session() -> requests.Session:
    """Create a configured requests Session with retry behavior."""
    session = requests.Session()

    # Configure retries for idempotent HTTP methods (GET, HEAD)
    retries = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def normalize_text(text: str) -> str:
    """Normalize Arabic and English text by stripping extra whitespace and lowercasing."""
    return re.sub(r"\s+", " ", text.strip().lower())


def contains_neighbourhood(text: str) -> Tuple[bool, str]:
    """Check if the given text contains any of the Casablanca neighbourhood names.

    Returns a tuple (found: bool, neighbourhood: str).  If a neighbourhood is
    detected, ``neighbourhood`` will be the first matching entry in
    ``CASA_NEIGHBOURHOODS``; otherwise it will be an empty string.
    """
    normalized = normalize_text(text)
    for n in CASA_NEIGHBOURHOODS:
        # Normalize the neighbourhood string similarly for comparison
        n_norm = normalize_text(n)
        if n_norm in normalized:
            return True, n
    return False, ""


###############################################################################
# Scraping functions
###############################################################################

def get_with_retries(session: requests.Session, url: str) -> Optional[str]:
    """Fetch a URL with retries, exponential backoff, and User-Agent rotation.

    Returns the response text if successful, or ``None`` if the request
    ultimately fails.
    """
    for attempt in range(1, 4):
        headers = {"User-Agent": get_random_user_agent()}
        try:
            response = session.get(url, headers=headers, timeout=15)
        except requests.RequestException as e:
            logging.warning(f"Request exception on attempt {attempt} for {url}: {e}")
            sleep_seconds = 1.5 * (2 ** (attempt - 1))
            time.sleep(sleep_seconds)
            continue

        # Check for HTTP status codes
        if response.status_code == 404:
            logging.info(f"Received 404 for {url}. Not retrying further.")
            return None
        if response.status_code == 429:
            # Too many requests: wait 30 seconds
            logging.warning(f"Received 429 for {url}. Waiting 30 seconds and retrying.")
            time.sleep(30)
            continue
        if 200 <= response.status_code < 300:
            response.encoding = response.apparent_encoding or 'utf-8'
            return response.text
        else:
            logging.warning(f"Unexpected status {response.status_code} on attempt {attempt} for {url}.")
            sleep_seconds = 1.5 * (2 ** (attempt - 1))
            time.sleep(sleep_seconds)
            continue

    logging.error(f"Failed to fetch {url} after multiple attempts.")
    return None


def parse_search_results(html: str, site: str) -> List[Dict[str, str]]:
    """Parse a search results page and extract article metadata.

    Returns a list of dictionaries with keys: 'title', 'url', 'snippet', 'date'.
    The parser aims to handle generic blog/news structures by looking for
    <article> tags, then falling back to <li> or <div> structures when
    necessary.  Because each site may structure its search results differently,
    this function uses heuristic approaches.
    """
    soup = BeautifulSoup(html, "html.parser")
    articles: List[Dict[str, str]] = []

    # Try to find <article> tags first
    article_tags = soup.find_all("article")
    if article_tags:
        for article in article_tags:
            link_tag = article.find(["a", "h1", "h2", "h3", "h4"])
            if not link_tag:
                continue
            href = link_tag.get("href")
            if not href:
                continue
            title = link_tag.get_text(strip=True)
            # Snippet: gather first paragraph or summary
            snippet_tag = article.find("p") or article.find(class_=re.compile(r"(summary|excerpt|entry-summary)"))
            snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""
            # Date: look for time tag or meta itemprop
            date_tag = article.find("time")
            date = date_tag.get("datetime") if date_tag and date_tag.get("datetime") else (date_tag.get_text(strip=True) if date_tag else "")
            articles.append({"title": title, "url": href, "snippet": snippet, "date": date})
    else:
        # Fallback: look for list items or divs with posts
        result_containers = soup.find_all(class_=re.compile(r"(result|post|entry)", re.I))
        for container in result_containers:
            link_tag = container.find("a")
            if not link_tag:
                continue
            href = link_tag.get("href")
            title = link_tag.get_text(strip=True)
            snippet_tag = container.find("p")
            snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""
            date_tag = container.find("time")
            date = date_tag.get("datetime") if date_tag and date_tag.get("datetime") else (date_tag.get_text(strip=True) if date_tag else "")
            articles.append({"title": title, "url": href, "snippet": snippet, "date": date})

    # Some sites include duplicated search result entries; filter duplicates
    seen_urls = set()
    deduped_articles = []
    for art in articles:
        if art["url"] not in seen_urls:
            deduped_articles.append(art)
            seen_urls.add(art["url"])
    return deduped_articles


def build_search_url(site: str, keyword: str, page: int) -> str:
    """Construct a search URL for the given site, keyword, and page.

    This function assumes that the target sites use a WordPress-style
    query parameter ``?s=<keyword>`` for search, with pagination via
    ``&paged=<page>``.  Some sites may not support pagination or may use
    different patterns; the generic approach here may not work for every
    site, but it provides a sensible default.
    """
    # Ensure the keyword is URL-encoded.  ``requests`` will handle encoding
    # when passed as params, but here we construct the URL manually.
    from urllib.parse import quote
    encoded_keyword = quote(keyword)
    if page <= 1:
        return f"{site}/?s={encoded_keyword}"
    return f"{site}/?s={encoded_keyword}&paged={page}"


def extract_article_body(html: str) -> str:
    """Extract the main article body from the HTML using various CSS selectors.

    Tries a series of selectors in order of decreasing specificity.  If
    none of these selectors yields content, falls back to concatenating
    all <p> tags on the page.  Returns the cleaned text.
    """
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div.post-content",
        "div.entry-content",
        "div.article-body",
        "div.td-post-content",
        "article",
    ]

    for selector in selectors:
        elem = soup.select_one(selector)
        if elem:
            paragraphs = elem.find_all("p") if selector != "article" else elem.find_all("p")
            if paragraphs:
                texts = [p.get_text(separator=" ", strip=True) for p in paragraphs]
                body = "\n".join([t for t in texts if t])
                if body:
                    return body

    # Fallback: collect all <p> tags on the page
    all_paragraphs = soup.find_all("p")
    texts = [p.get_text(separator=" ", strip=True) for p in all_paragraphs]
    return "\n".join([t for t in texts if t])


def search_site_for_keyword(session: requests.Session, site: str, keyword: str) -> List[Dict[str, str]]:
    """Search a single site for a keyword and return aggregated search results.

    The function paginates through up to ``MAX_PAGES_PER_SITE`` pages.  It
    stops early if a page returns 404 or if no results are found on a page.
    """
    all_results: List[Dict[str, str]] = []
    for page in range(1, MAX_PAGES_PER_SITE + 1):
        search_url = build_search_url(site, keyword, page)
        logging.info(f"Searching {site}: page {page} ({search_url})")
        html = get_with_retries(session, search_url)
        if html is None:
            # Received 404 or other fatal error; stop paginating
            break
        results = parse_search_results(html, site)
        if not results:
            # If no results on this page, assume there are no further pages
            logging.info(f"No results found on page {page} for {site}. Stopping pagination.")
            break
        all_results.extend(results)
        # Random delay between 1.5 and 3.0 seconds
        delay = random.uniform(1.5, 3.0)
        time.sleep(delay)
    return all_results


def scrape_sites(sites: List[str], keyword: str) -> List[ArticleRecord]:
    """Scrape all target sites for the given keyword and return article records."""
    session = create_http_session()
    scraped_articles: List[ArticleRecord] = []
    articles_since_save = 0
    now_ts = datetime.now(ZoneInfo("Africa/Casablanca")) if ZoneInfo else datetime.now()
    timestamp_str = now_ts.strftime("%Y%m%d_%H%M%S")

    # Determine output filenames upfront
    json_output_path = OUTPUT_JSON_TEMPLATE.format(timestamp=timestamp_str)
    csv_output_path = OUTPUT_CSV_TEMPLATE.format(timestamp=timestamp_str)

    logging.info(f"Scraping started at {timestamp_str}. Results will be saved to {json_output_path} and {csv_output_path}")

    for site in sites:
        logging.info(f"Starting scrape for {site}")
        try:
            search_results = search_site_for_keyword(session, site, keyword)
        except Exception as e:
            logging.error(f"Error while searching {site}: {e}")
            continue

        for result in search_results:
            title = result.get("title", "").strip()
            url = result.get("url", "").strip()
            snippet = result.get("snippet", "").strip()
            date_str = result.get("date", "").strip()

            if not url:
                continue

            # Determine if we have a neighbourhood in title or snippet
            found, neighbourhood = contains_neighbourhood(title + " " + snippet)

            # Fetch the article body regardless; we need to verify if the body
            # mentions a neighbourhood even if the title/snippet does not
            html = get_with_retries(session, url)
            if html is None:
                # Could not fetch article; skip
                fetch_status = "failed"
                full_content = ""
                content_length = 0
            else:
                full_content = extract_article_body(html)
                content_length = len(full_content)
                fetch_status = "success" if content_length >= MIN_BODY_LENGTH else "failed"

            # Only retain articles that mention a neighbourhood somewhere
            if not found and full_content:
                # Check if the full body contains a neighbourhood
                found_body, neighbourhood_body = contains_neighbourhood(full_content)
                if found_body:
                    found = True
                    neighbourhood = neighbourhood_body
            if not found:
                continue  # Skip this article; it does not relate to Casablanca

            # At this point we have a qualifying article.  If the body is
            # considered "failed" due to being too short, we still include it but
            # mark fetch_status accordingly.
            scraped_at_str = datetime.now(ZoneInfo("Africa/Casablanca")).isoformat() if ZoneInfo else datetime.now().isoformat()
            record = ArticleRecord(
                source=re.sub(r"^https?://", "", site).strip("/"),
                title=title,
                url=url,
                date=date_str,
                snippet=snippet,
                full_content=full_content,
                neighbourhood_detected=neighbourhood,
                fetch_status=fetch_status,
                content_length=content_length,
                scraped_at=scraped_at_str,
            )
            scraped_articles.append(record)
            articles_since_save += 1

            # Save progress every N articles
            if articles_since_save >= SAVE_EVERY_N_ARTICLES:
                save_progress(scraped_articles, json_output_path, csv_output_path, append=True)
                articles_since_save = 0

    # Save remaining articles
    if scraped_articles:
        save_progress(scraped_articles, json_output_path, csv_output_path, append=True)

    return scraped_articles


def save_progress(records: List[ArticleRecord], json_path: str, csv_path: str, append: bool = True) -> None:
    """Save a list of ArticleRecord objects to JSON and CSV files.

    When ``append`` is True, records will be appended to existing files if
    they exist.  Otherwise, existing files will be overwritten.
    """
    # Prepare data for JSON and CSV
    json_data = [record.to_json_compatible() for record in records]
    file_mode = "a" if append and os.path.exists(json_path) else "w"
    with open(json_path, file_mode, encoding="utf-8") as jf:
        for entry in json_data:
            json.dump(entry, jf, ensure_ascii=False)
            jf.write("\n")

    # For CSV, write header only if not appending or file does not exist
    csv_exists = os.path.exists(csv_path)
    with open(csv_path, "a" if append else "w", newline='', encoding="utf-8") as cf:
        writer = None
        if not append or not csv_exists:
            # Write header row
            fieldnames = [
                "source",
                "title",
                "url",
                "date",
                "snippet",
                "full_content",
                "neighbourhood_detected",
                "fetch_status",
                "content_length",
                "scraped_at",
            ]
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
        else:
            # When appending, we still need a writer; header will not be written
            fieldnames = [
                "source",
                "title",
                "url",
                "date",
                "snippet",
                "full_content",
                "neighbourhood_detected",
                "fetch_status",
                "content_length",
                "scraped_at",
            ]
            writer = csv.DictWriter(cf, fieldnames=fieldnames)

        for record in records:
            writer.writerow(record.to_json_compatible())
    logging.info(f"Saved {len(records)} records to {json_path} and {csv_path}")


###############################################################################
# Main entry point
###############################################################################

def main() -> None:
    setup_logging(LOG_FILE, LOG_LEVEL)
    logging.info("Starting Moroccan news scraper")
    try:
        scraped_records = scrape_sites(TARGET_SITES, SEARCH_KEYWORD)
        total = len(scraped_records)
        logging.info(f"Scraping completed. Total qualifying articles collected: {total}")
    except Exception as exc:
        logging.exception(f"An error occurred during scraping: {exc}")


if __name__ == "__main__":
    main()