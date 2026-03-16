"""
neighborhood_extractor.py
─────────────────────────────────────────────────────────
LLM-powered Casablanca neighborhood extractor.

Reads:
  - output/casablanca_theft_articles.csv  → uses 'full_content' column
  - output/youtube_videos.jsonl           → uses 'title' + 'description'

Calls HuggingFace Inference API (Mixtral-8x7B) to extract the specific
Casablanca neighborhood where each crime occurred.

Falls back to the existing rule-based 'neighborhood_detected' column
when the LLM returns null or on error.

Saves: output/neighborhoods_extracted.json

Usage:
  python neighborhood_extractor.py              # full run
  python neighborhood_extractor.py --dry-run    # test first 10 items
  python neighborhood_extractor.py --limit 50   # process first 50 articles
  python neighborhood_extractor.py --no-llm     # no API calls, use existing data
─────────────────────────────────────────────────────────
"""

import argparse
import csv
import json
import os
import re
import time
import requests
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
CSV_PATH   = OUTPUT_DIR / "casablanca_theft_articles.csv"
JSONL_PATH = OUTPUT_DIR / "youtube_videos.jsonl"
OUT_PATH   = OUTPUT_DIR / "neighborhoods_extracted.json"

# HuggingFace API — same token as claude_extractor.py
HF_TOKEN = os.environ.get("HUGGING_FACE_HUB_TOKEN", "")
API_URL  = "https://api-inference.huggingface.co/models/mistralai/Mixtral-8x7B-Instruct-v0.1"

# Rate limiting
SLEEP_BETWEEN_CALLS = 1.5   # seconds
MAX_RETRIES         = 4
BACKOFF_BASE        = 2     # exponential: 2, 4, 8, 16 seconds

# Text truncation (to stay within token limits)
MAX_CONTENT_CHARS = 800

# ─── Known Casablanca neighborhoods (used in fallback / validation) ────────────

CASABLANCA_NEIGHBORHOODS = {
    # Official arrondissements
    "أنفا", "المعاريف", "عين الشق", "الفداء مرس السلطان", "الفداء",
    "الحي الحسني", "سيدي البرنوصي", "عين السبع الحي المحمدي",
    "عين السبع", "الحي المحمدي", "بن مسيك", "سيدي عثمان",
    "مولاي رشيد", "سيدي مومن", "الصخور السوداء", "بوسكورة",
    # Popular neighborhoods
    "غوتييه", "سيدي معروف", "درب غلف", "المدينة القديمة", "درب عمر",
    "الهراويين", "الإدريسسة", "ليساسفة", "المحمدية", "الزياتن",
    "درب السلطان", "الحي السلامي", "بوركون", "الفرح", "بنجدية",
    "مرس السلطان", "الحي المحمدي", "السالمية", "أناسي", "الرحمة",
    "النسيم", "القريعة", "الكاراج علال", "سيدي البرنوصي", "البرنوصي",
    "طماريس", "الوازيس", "العالية", "تيط مليل", "مديونة", "بن سليمان",
    "برنوصي", "درب الكبير", "ساحة الأمم المتحدة", "رياض الألفة",
    "الحي الحسني", "حي الفلاح", "القاعة"
}

# ─── LLM Extraction ───────────────────────────────────────────────────────────

PROMPT_TEMPLATE = """\
[INST] You are an expert in Moroccan Arabic (Darija) and Moroccan geography.

Your task: Extract the specific Casablanca neighborhood (حي or منطقة) where the crime occurred from the text below.

Rules:
- Only return a neighborhood that is inside Casablanca (الدار البيضاء / كازا).
- Use exact Arabic names. Examples: مولاي رشيد، عين السبع، الحي المحمدي، سيدي البرنوصي، أنفا، المعاريف، الحي الحسني، الفداء، بن مسيك، سيدي مومن، درب السلطان، الصخور السوداء، درب عمر، الهراويين، القريعة، المدينة القديمة.
- If the text says "وسط المدينة" (city center), return "وسط المدينة".
- If no specific Casablanca neighborhood is mentioned, return null.
- If the crime happened in another city (مراكش، فاس، الرباط، etc.), return null.

Return ONLY valid JSON, no explanation:
{{"neighborhood": "Arabic name or null", "confidence": "high or medium or low"}}

Text:
{text}
[/INST]"""


def call_llm(text: str) -> dict:
    """Call the HuggingFace API with exponential backoff. Returns dict with neighborhood & confidence."""
    if not HF_TOKEN:
        return {"neighborhood": None, "confidence": "none", "error": "no_token"}

    text_truncated = text[:MAX_CONTENT_CHARS]
    prompt = PROMPT_TEMPLATE.format(text=text_truncated)
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    payload = {"inputs": prompt, "parameters": {"max_new_tokens": 80, "temperature": 0.1}}

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=45)

            if resp.status_code == 429:
                wait = BACKOFF_BASE ** (attempt + 1)
                print(f"    [Rate limit] Waiting {wait}s before retry {attempt+1}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                return {"neighborhood": None, "confidence": "none",
                        "error": f"http_{resp.status_code}"}

            raw = resp.json()[0]["generated_text"]

            # Extract the JSON portion after [/INST]
            inst_end = raw.rfind("[/INST]")
            if inst_end != -1:
                raw = raw[inst_end + 7:]

            match = re.search(r'\{[^{}]+\}', raw, re.DOTALL)
            if not match:
                return {"neighborhood": None, "confidence": "low", "error": "no_json"}

            result = json.loads(match.group(0))
            nbh = result.get("neighborhood")
            if isinstance(nbh, str) and nbh.lower() in ("null", "none", ""):
                nbh = None
            result["neighborhood"] = nbh
            return result

        except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
            if attempt == MAX_RETRIES - 1:
                return {"neighborhood": None, "confidence": "none", "error": str(e)}
            time.sleep(BACKOFF_BASE ** attempt)

    return {"neighborhood": None, "confidence": "none", "error": "max_retries"}


def extract_neighborhood(text: str, fallback: str = None, use_llm: bool = True) -> dict:
    """Extract neighborhood from text, falling back to rule-based if LLM fails."""
    if use_llm and HF_TOKEN:
        result = call_llm(text)
        nbh = result.get("neighborhood")
        if nbh and len(nbh.strip()) > 1:
            return {"neighborhood": nbh.strip(),
                    "confidence": result.get("confidence", "medium"),
                    "source": "llm"}

    # Fallback: use the existing rule-based detection
    if fallback and fallback.strip() and fallback.strip() not in ("", "البيضاء", "الدار البيضاء"):
        return {"neighborhood": fallback.strip(), "confidence": "low", "source": "rules"}

    # Last resort: try regex search in text for known neighborhoods
    for nbh in CASABLANCA_NEIGHBORHOODS:
        if nbh in text:
            return {"neighborhood": nbh, "confidence": "low", "source": "regex"}

    return {"neighborhood": None, "confidence": "none", "source": "none"}


# ─── Data loaders ─────────────────────────────────────────────────────────────

def load_articles(limit: int = None) -> list:
    """Load articles from the CSV file."""
    articles = []
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            articles.append({
                "id": i,
                "source": row.get("source", ""),
                "title": row.get("title", ""),
                "url": row.get("url", ""),
                "date": row.get("date", ""),
                "full_content": row.get("full_content", ""),
                "neighborhood_detected": row.get("neighborhood_detected", ""),
            })
    return articles


def load_youtube(limit: int = None) -> list:
    """Load YouTube videos from the JSONL file."""
    videos = []
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                video = json.loads(line)
                videos.append({
                    "video_id": video.get("video_id", ""),
                    "title": video.get("title", ""),
                    "url": video.get("url", ""),
                    "description": video.get("description", "") or "",
                    "view_count": video.get("view_count", 0),
                    "duration_seconds": video.get("duration_seconds", 0),
                    "upload_date": video.get("upload_date", ""),
                })
            except json.JSONDecodeError:
                continue
    return videos


# ─── Main processing ──────────────────────────────────────────────────────────

def process_articles(articles: list, use_llm: bool = True) -> list:
    """Run extraction on all articles."""
    results = []
    total = len(articles)

    for i, art in enumerate(articles):
        print(f"  [Articles {i+1}/{total}] {art['source']} — {art['title'][:55]}...")

        # Combine title + full_content for best accuracy
        text = f"{art['title']}\n\n{art['full_content']}"

        result = extract_neighborhood(
            text=text,
            fallback=art.get("neighborhood_detected", ""),
            use_llm=use_llm
        )

        results.append({
            "id": art["id"],
            "source": art["source"],
            "title": art["title"],
            "url": art["url"],
            "date": art["date"],
            "neighborhood": result["neighborhood"],
            "confidence": result["confidence"],
            "detection_source": result["source"],
            "neighborhood_rule_based": art.get("neighborhood_detected", ""),
        })

        if use_llm and HF_TOKEN:
            time.sleep(SLEEP_BETWEEN_CALLS)

    return results


def process_youtube(videos: list, use_llm: bool = True) -> list:
    """Run extraction on all YouTube videos."""
    results = []
    total = len(videos)

    for i, vid in enumerate(videos):
        print(f"  [YouTube {i+1}/{total}] {vid['title'][:60]}...")

        # Title is often very informative for YouTube
        text = f"{vid['title']}\n\n{vid['description']}"

        result = extract_neighborhood(
            text=text,
            fallback=None,
            use_llm=use_llm
        )

        results.append({
            "video_id": vid["video_id"],
            "title": vid["title"],
            "url": vid["url"],
            "description": vid["description"][:300],
            "view_count": vid["view_count"],
            "neighborhood": result["neighborhood"],
            "confidence": result["confidence"],
            "detection_source": result["source"],
        })

        if use_llm and HF_TOKEN:
            time.sleep(SLEEP_BETWEEN_CALLS)

    return results


def compute_stats(articles: list, youtube: list) -> dict:
    """Aggregate neighborhood counts."""
    from collections import Counter
    art_counter  = Counter()
    yt_counter   = Counter()

    for a in articles:
        nbh = a.get("neighborhood")
        if nbh:
            art_counter[nbh] += 1

    for v in youtube:
        nbh = v.get("neighborhood")
        if nbh:
            yt_counter[nbh] += 1

    all_nbhs = set(art_counter.keys()) | set(yt_counter.keys())
    stats = {}
    for nbh in all_nbhs:
        total = art_counter[nbh] + yt_counter[nbh]
        stats[nbh] = {
            "total": total,
            "articles": art_counter[nbh],
            "youtube": yt_counter[nbh],
            "zone": "red" if total >= 15 else ("orange" if total >= 5 else ("yellow" if total >= 2 else "green"))
        }

    return dict(sorted(stats.items(), key=lambda x: -x[1]["total"]))


def save_results(articles: list, youtube: list):
    """Save combined results + stats to JSON."""
    stats = compute_stats(articles, youtube)
    output = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "stats": {
            "articles_processed": len(articles),
            "youtube_processed": len(youtube),
            "articles_with_neighborhood": sum(1 for a in articles if a.get("neighborhood")),
            "youtube_with_neighborhood": sum(1 for v in youtube if v.get("neighborhood")),
        },
        "neighborhood_counts": stats,
        "articles": articles,
        "youtube": youtube,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Saved to {OUT_PATH}")
    print(f"\nTop 10 neighborhoods:")
    for i, (nbh, data) in enumerate(list(stats.items())[:10]):
        bar = "█" * min(data["total"], 30)
        zone = data["zone"].upper()
        print(f"  {i+1:2d}. [{zone:6s}] {nbh:25s} {bar} {data['total']} (📰{data['articles']} 📺{data['youtube']})")


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract Casablanca neighborhoods using LLM")
    parser.add_argument("--dry-run", action="store_true", help="Process only first 10 items")
    parser.add_argument("--limit",   type=int,            help="Limit number of articles to process")
    parser.add_argument("--no-llm",  action="store_true", help="Skip LLM, use only rule-based detection")
    args = parser.parse_args()

    use_llm = not args.no_llm
    limit   = 10 if args.dry_run else args.limit

    if use_llm and not HF_TOKEN:
        print("⚠️  HUGGING_FACE_HUB_TOKEN not set. Falling back to rule-based extraction only.")
        use_llm = False

    print("=" * 60)
    print("  Casablanca Neighborhood Extractor")
    print("=" * 60)
    print(f"  Mode : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    print(f"  LLM  : {'✓ Mixtral-8x7B via HuggingFace' if use_llm else '✗ Disabled (rule-based fallback)'}")
    print(f"  Limit: {limit or 'none'}")
    print()

    # Process articles
    print("── Articles ────────────────────────────────────────────")
    articles = load_articles(limit=limit)
    print(f"  Loaded {len(articles)} articles")
    article_results = process_articles(articles, use_llm=use_llm)

    # Process YouTube
    print("\n── YouTube videos ──────────────────────────────────────")
    youtube = load_youtube(limit=limit)
    print(f"  Loaded {len(youtube)} videos")
    youtube_results = process_youtube(youtube, use_llm=use_llm)

    # Save
    print("\n── Saving results ──────────────────────────────────────")
    save_results(article_results, youtube_results)


if __name__ == "__main__":
    main()
