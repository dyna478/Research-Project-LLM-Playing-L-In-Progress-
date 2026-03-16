"""
nlp/claude_extractor.py
─────────────────────────────────────────────────────────
Uses a Hugging Face model to extract structured crime data from each article.
Run AFTER scraping to enrich the raw JSON with:
  - crime_type, severity, neighborhood, coordinates, summary
─────────────────────────────────────────────────────────
"""

import json
import re
import time
import os
import requests

# Hugging Face Inference API configuration
API_URL = "https://api-inference.huggingface.co/models/mistralai/Mixtral-8x7B-Instruct-v0.1"
HF_TOKEN = os.environ.get("HUGGING_FACE_HUB_TOKEN")

EXTRACTION_PROMPT = """
[INST] You are a crime data analyst specializing in Moroccan news.
Analyze this news article and extract structured crime information.

Article Title: {title}
Article Content: {content}
Date: {date}
Source: {source}

Return ONLY a valid JSON object with these exact fields (no explanation, no markdown):
{{
  "is_crime": true or false,
  "city": "city name or null",
  "neighborhood": "specific Casablanca neighborhood in Arabic or null",
  "crime_type": one of ["theft", "robbery", "assault", "murder", "fraud", "drugs", "kidnapping", "sexual_assault", "vandalism", "carjacking", "burglary", "pickpocketing", "gang_activity", "other"] or null,
  "crime_subtype": "more specific description in Arabic or null",
  "severity": one of ["low", "medium", "high", "critical"] or null,
  "victims_count": number or null,
  "suspects_arrested": true or false or null,
  "weapon_used": true or false or null,
  "location_hint": "any street, landmark, or address mentioned or null",
  "crime_time": "time of day if mentioned: morning/afternoon/evening/night or null",
  "crime_date": "date in YYYY-MM-DD if mentioned or null",
  "summary_en": "one sentence summary in English",
  "summary_ar": "one sentence summary in Arabic"
}}

Rules:
- neighborhood: use exact Arabic names like عين الشق، سيدي البرنوصي، عين السبع، مولاي رشيد، الحي المحمدي، أنفا، سيدي عثمان، الصخور السوداء، بوسكورة، سيدي مومن، الحي الحسني، درب السلطان، المعاريف، etc.
- If city is not Casablanca (الدار البيضاء), set is_crime to false
- severity: low=minor theft, medium=robbery/assault, high=armed robbery/serious assault, critical=murder/kidnapping
- Return ONLY the JSON object, nothing else [/INST]
"""


def extract_crime_data(article: dict) -> dict:
    """Call Hugging Face API to extract structured data from one article."""
    if not HF_TOKEN:
        return {"error": "HUGGING_FACE_HUB_TOKEN environment variable not set.", "is_crime": False}

    content = article.get("full_content") or article.get("snippet", "")
    if len(content) > 3000:
        content = content[:3000]

    prompt = EXTRACTION_PROMPT.format(
        title=article.get("title", ""),
        content=content,
        date=article.get("date", ""),
        source=article.get("source", "")
    )

    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    payload = {"inputs": prompt, "parameters": {"max_new_tokens": 600}}

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
        if response.status_code != 200:
            return {"error": f"API request failed with status {response.status_code}: {response.text}", "is_crime": False}

        result = response.json()
        raw = result[0]['generated_text']

        # Extract the JSON part from the model's response
        json_part_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not json_part_match:
            return {"error": "No JSON object found in the model's response.", "is_crime": False, "raw_response": raw}

        raw_json = json_part_match.group(0)
        extracted = json.loads(raw_json)
        return extracted

    except requests.RequestException as e:
        return {"error": f"API request error: {e}", "is_crime": False}
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse error: {e}", "is_crime": False, "raw_response": raw}
    except (KeyError, IndexError) as e:
        return {"error": f"Unexpected API response format: {e}", "is_crime": False, "raw_response": result}
    except Exception as e:
        return {"error": str(e), "is_crime": False}


def enrich_all_articles(input_json: str, output_json: str, batch_size: int = 50):
    """
    Load scraped articles, enrich each with Hugging Face NLP,
    save to output_json progressively.
    """
    with open(input_json, "r", encoding="utf-8") as f:
        articles = json.load(f)

    print(f"Loaded {len(articles)} articles. Starting NLP enrichment...")

    enriched = []
    errors = 0

    for i, article in enumerate(articles):
        print(f"[{i+1}/{len(articles)}] {article.get('source')} — {article.get('title', '')[:60]}")

        extracted = extract_crime_data(article)

        # Merge extracted data into article
        article["nlp"] = extracted
        article["crime_type"] = extracted.get("crime_type")
        article["severity"] = extracted.get("severity")
        article["neighborhood_nlp"] = extracted.get("neighborhood")
        article["location_hint"] = extracted.get("location_hint")
        article["summary_en"] = extracted.get("summary_en")
        article["summary_ar"] = extracted.get("summary_ar")
        article["is_crime_confirmed"] = extracted.get("is_crime", False)
        article["suspects_arrested"] = extracted.get("suspects_arrested")

        # Use NLP neighborhood if more specific than scraper detection
        if extracted.get("neighborhood"):
            article["neighborhood_final"] = extracted["neighborhood"]
        else:
            article["neighborhood_final"] = article.get("neighborhood_detected", "")

        if "error" in extracted:
            errors += 1
            print(f"  ERROR: {extracted['error']}")

        enriched.append(article)

        # Save every batch_size articles (progressive save)
        if (i + 1) % batch_size == 0:
            _save_progress(enriched, output_json)
            print(f"  ✓ Progress saved ({i+1} articles)")

        # Rate limiting
        time.sleep(1.5) # A bit more delay for the free tier

    # Final save
    _save_progress(enriched, output_json)

    # Stats
    confirmed = sum(1 for a in enriched if a.get("is_crime_confirmed"))
    print(f"\n{'='*50}")
    print(f"Enrichment complete!")
    print(f"  Total articles:    {len(enriched)}")
    print(f"  Crime confirmed:   {confirmed}")
    print(f"  Errors:            {errors}")
    print(f"  Output:            {output_json}")

    return enriched


def _save_progress(articles: list, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import sys
    import re
    if len(sys.argv) < 3:
        print("Usage: python claude_extractor.py input.json output.json")
        sys.exit(1)
    enrich_all_articles(sys.argv[1], sys.argv[2])
