"""
utils/helpers.py — Text cleaning, filtering, neighborhood extraction
"""

import re


def clean_text(text: str) -> str:
    """Remove extra whitespace and normalize Arabic/French text."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[\x00-\x1f\x7f]', '', text)
    return text.strip()


def contains_casablanca(text: str, keywords: list[str]) -> bool:
    """Return True if the text mentions Casablanca or any of its neighborhoods."""
    text_lower = text.lower()
    for kw in keywords:
        if kw.lower() in text_lower:
            return True
    return False


def extract_neighborhood(text: str, keywords: list[str]) -> str:
    """
    Extract the most specific neighborhood mentioned in text.
    Returns the keyword found, or 'الدار البيضاء' as fallback.
    """
    # Prioritize specific neighborhoods over generic city names
    generic = {"الدار البيضاء", "البيضاء", "الدارالبيضاء", "Casablanca", "Casa"}
    text_lower = text.lower()

    # First pass: look for specific neighborhoods
    for kw in keywords:
        if kw not in generic and kw.lower() in text_lower:
            return kw

    # Second pass: fallback to generic
    for kw in generic:
        if kw.lower() in text_lower:
            return kw

    return ""


def deduplicate_articles(articles: list[dict]) -> list[dict]:
    """Remove duplicate articles based on URL."""
    seen_urls = set()
    unique = []
    for article in articles:
        url = article.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(article)
    return unique


def normalize_date(date_str: str) -> str:
    """Try to normalize date strings to YYYY-MM-DD."""
    if not date_str:
        return ""
    # Already ISO format
    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]
    # DD/MM/YYYY
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return date_str
