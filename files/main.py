"""
main.py — Run the full Casablanca Crime Scraping Pipeline
─────────────────────────────────────────────────────────
Usage:
  python main.py --mode scrape           # scrape all sources
  python main.py --mode enrich           # run NLP on scraped data
  python main.py --mode full             # scrape + enrich end to end
  python main.py --mode scrape --tier 1  # scrape tier 1 only (fastest)
  python main.py --source hespress       # scrape one source only
─────────────────────────────────────────────────────────
"""

import argparse
import os
import json
import glob
from datetime import datetime

from news_scraper import (
    SOURCES, scrape_source, enrich_articles, save_results
)
from helpers import deduplicate_articles
from logger import get_logger

logger = get_logger("main")


def run_scrape(tier: int = None, source_name: str = None):
    """Scrape selected sources and save raw results."""
    sources_to_run = SOURCES

    if source_name:
        sources_to_run = [s for s in SOURCES if s["name"] == source_name]
        if not sources_to_run:
            logger.error(f"Source '{source_name}' not found. Available: {[s['name'] for s in SOURCES]}")
            return []

    if tier:
        sources_to_run = [s for s in sources_to_run if s["tier"] <= tier]

    logger.info(f"{'='*60}")
    logger.info(f"CASABLANCA CRIME SCRAPER")
    logger.info(f"Sources to scrape: {len(sources_to_run)}")
    logger.info(f"{'='*60}")

    all_articles = []

    for source in sources_to_run:
        # Try search mode first, then category mode
        articles = scrape_source(source, mode="search")
        if len(articles) < 5:
            logger.info(f"[{source['name']}] Search returned few results, trying category mode...")
            cat_articles = scrape_source(source, mode="category")
            articles = articles + cat_articles

        all_articles.extend(articles)
        logger.info(f"Running total: {len(all_articles)} articles\n")

    # Deduplicate
    unique = deduplicate_articles(all_articles)
    logger.info(f"After deduplication: {len(unique)} unique articles (removed {len(all_articles) - len(unique)})")

    # Enrich with full content
    sources_map = {s["name"]: s for s in SOURCES}
    unique = enrich_articles(unique, sources_map)

    # Save
    json_path, csv_path = save_results(unique)

    # Summary
    print_summary(unique)

    return unique, json_path


def run_enrich(input_json: str = None):
    """Run Claude NLP enrichment on existing scraped data."""
    from claude_extractor import enrich_all_articles

    if not input_json:
        # Find most recent scraped file
        files = glob.glob("output/casablanca_crimes_*.json")
        if not files:
            logger.error("No scraped data found. Run --mode scrape first.")
            return
        input_json = max(files)

    output_json = input_json.replace(".json", "_enriched.json")
    logger.info(f"Enriching: {input_json}")
    logger.info(f"Output:    {output_json}")

    enriched = enrich_all_articles(input_json, output_json)
    return enriched, output_json


def print_summary(articles: list):
    """Print a breakdown of scraped articles."""
    from collections import Counter

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE — {len(articles)} articles")
    print(f"{'='*60}")

    by_source = Counter(a["source"] for a in articles)
    print("\n📰 By source:")
    for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"   {source:<20} {count:>5} articles")

    by_neighborhood = Counter(
        a["neighborhood_detected"] for a in articles
        if a.get("neighborhood_detected")
    )
    print("\n🗺️  By neighborhood (top 15):")
    for nb, count in by_neighborhood.most_common(15):
        print(f"   {nb:<30} {count:>5} articles")

    by_lang = Counter(a["language"] for a in articles)
    print("\n🌐 By language:")
    for lang, count in by_lang.items():
        print(f"   {lang}: {count}")

    print(f"\n✅ Output saved to ./output/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Casablanca Crime Scraper")
    parser.add_argument("--mode", choices=["scrape", "enrich", "full"],
                        default="full", help="Pipeline mode")
    parser.add_argument("--tier", type=int, choices=[1, 2, 3],
                        help="Only scrape sources up to this tier")
    parser.add_argument("--source", type=str,
                        help="Scrape only this source (e.g. hespress)")
    parser.add_argument("--input", type=str,
                        help="Input JSON file for enrich mode")
    args = parser.parse_args()

    os.makedirs("output", exist_ok=True)

    if args.mode == "scrape":
        run_scrape(tier=args.tier, source_name=args.source)

    elif args.mode == "enrich":
        run_enrich(input_json=args.input)

    elif args.mode == "full":
        logger.info("Running full pipeline: scrape + enrich")
        result = run_scrape(tier=args.tier, source_name=args.source)
        if result:
            articles, json_path = result
            run_enrich(input_json=json_path)
