"""
youtube_scraper.py
──────────────────
Scrapes all videos from a YouTube channel search query,
fetches their Arabic/French transcripts, and saves to JSONL.

Usage:
    python youtube_scraper.py

Output:
    output/youtube_videos.jsonl
"""

import json
import logging
import os
import time

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import CouldNotRetrieveTranscript

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CHANNEL_URL  = "https://www.youtube.com/@chouftvlive-maroc"
SEARCH_QUERY = "سرقة الدار البيضاء"          # the search inside the channel
OUTPUT_DIR   = "output"
JSONL_FILE   = os.path.join(OUTPUT_DIR, "youtube_videos.jsonl")

# Preferred transcript languages in priority order
TRANSCRIPT_LANGS = ["ar", "fr", "en"]

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# LOAD ALREADY SCRAPED VIDEO IDS
# ─────────────────────────────────────────────
def load_scraped_ids() -> set:
    seen = set()
    if os.path.exists(JSONL_FILE):
        with open(JSONL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("video_id"):
                        seen.add(rec["video_id"])
                except Exception:
                    pass
    log.info(f"📋  Loaded {len(seen)} already-scraped video IDs")
    return seen


# ─────────────────────────────────────────────
# STEP 1 — COLLECT VIDEO IDS VIA yt-dlp
# ─────────────────────────────────────────────
def get_video_ids() -> list[dict]:
    """
    Uses yt-dlp to list all videos returned by searching the channel.
    yt-dlp understands the ytsearch: syntax and channel search URLs.
    """
    log.info(f"🔍  Fetching video list for query: «{SEARCH_QUERY}»")

    search_url = (
        f"{CHANNEL_URL}/search?query={__import__('urllib.parse', fromlist=['quote']).quote(SEARCH_QUERY)}"
    )

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,          # don't download, just list
        "skip_download": True,
        "playlistend": 500,            # safety cap
    }

    videos = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(search_url, download=False)
        except Exception as e:
            log.error(f"yt-dlp error: {e}")
            return videos

        entries = info.get("entries") or []
        log.info(f"   Found {len(entries)} videos in search results")
        for entry in entries:
            vid_id = entry.get("id") or entry.get("url", "")
            # sometimes url is the full watch URL
            if "watch?v=" in vid_id:
                vid_id = vid_id.split("watch?v=")[-1].split("&")[0]
            if not vid_id or len(vid_id) < 5:
                continue
            videos.append(
                {
                    "video_id": vid_id,
                    "title": entry.get("title", ""),
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "duration": entry.get("duration"),
                    "upload_date": entry.get("upload_date", ""),
                    "view_count": entry.get("view_count"),
                    "description": entry.get("description", ""),
                }
            )

    return videos


# ─────────────────────────────────────────────
# STEP 2a — GET TRANSCRIPT VIA youtube_transcript_api
# ─────────────────────────────────────────────
# One shared API instance (new v1.x style)
_YT_API = YouTubeTranscriptApi()

def get_transcript_api(video_id: str) -> tuple[str, str]:
    """
    Returns (transcript_text, source) where source is 'api:<lang>'.
    Raises CouldNotRetrieveTranscript on failure.
    """
    # List available transcripts to choose best language
    transcript_list = _YT_API.list(video_id)
    available = list(transcript_list)

    chosen = None
    for lang in TRANSCRIPT_LANGS:
        for t in available:
            if t.language_code.startswith(lang):
                chosen = t
                break
        if chosen:
            break

    if chosen is None and available:
        chosen = available[0]

    if chosen is None:
        raise CouldNotRetrieveTranscript(video_id)

    # Fetch using the specific language code
    data = _YT_API.fetch(video_id, languages=[chosen.language_code])
    # Segments are objects with .text attribute in newer versions
    full_text = " ".join(
        seg.text if hasattr(seg, "text") else seg.get("text", "")
        for seg in data
    )
    return full_text.strip(), f"api:{chosen.language_code}"


# ─────────────────────────────────────────────
# STEP 2b — FALLBACK: Playwright browser (click "Show transcript")
# ─────────────────────────────────────────────
# Opens YouTube in a real browser, clicks the transcript button, scrapes text.
# Works without auth and bypasses bot-detection for caption fetching.

def get_transcript_browser(video_id: str) -> tuple[str, str]:
    """
    Uses Playwright (headless Chromium) to open the YouTube video page,
    click 'Show transcript', and scrape the transcript text.
    Returns (transcript_text, 'browser').
    Raises on failure.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    url = f"https://www.youtube.com/watch?v={video_id}"
    log.info(f"   [{video_id}] 🌐 Launching browser to click 'Show transcript'...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            locale="ar-MA",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Accept any consent dialog
            try:
                page.click("button[aria-label*='Accept']", timeout=4000)
            except PWTimeout:
                pass
            try:
                page.click("button:has-text('Accept all')", timeout=3000)
            except PWTimeout:
                pass

            # Wait for the video player area to load
            page.wait_for_selector("#below", timeout=15000)

            # ── Click the "..." (more actions) button below the video ──
            # The button is inside #above-the-fold, labelled "More actions"
            more_btn = page.locator("button[aria-label='More actions']").first
            more_btn.wait_for(timeout=10000)
            more_btn.click()

            # ── Click "Show transcript" from the popup menu ──
            transcript_item = page.locator(
                "yt-formatted-string:has-text('Show transcript'), "
                "tp-yt-paper-item:has-text('Show transcript'), "
                "ytd-menu-service-item-renderer:has-text('transcript'), "
                "yt-formatted-string:has-text('الحصول على النص المكتوب')"
            ).first
            transcript_item.wait_for(timeout=8000)
            transcript_item.click()

            # ── Wait for the transcript panel to appear ──
            panel = page.locator("ytd-transcript-segment-list-renderer, ytd-transcript-renderer")
            panel.wait_for(timeout=12000)

            # Give segments a moment to fully render
            page.wait_for_timeout(2000)

            # ── Scrape all transcript segments ──
            # Each segment: timestamp div + text div inside cue-group renderer
            segments = page.locator("ytd-transcript-segment-renderer .segment-text")
            count = segments.count()
            if count == 0:
                # Try alternate selector
                segments = page.locator("[class*='segment-text'], .ytd-transcript-segment-renderer")
                count = segments.count()

            parts = []
            for i in range(count):
                seg_text = segments.nth(i).inner_text().strip()
                if seg_text:
                    parts.append(seg_text)

            if not parts:
                raise ValueError("Transcript panel loaded but no segments found")

            text = " ".join(parts)
            log.info(f"   [{video_id}] Browser got {len(parts)} segments, {len(text)} chars")
            return text, "browser"

        finally:
            browser.close()


# ─────────────────────────────────────────────
# STEP 2c — LAST RESORT: yt-dlp VTT subtitles
# ─────────────────────────────────────────────
def get_transcript_ytdlp(video_id: str) -> tuple[str, str]:
    """
    Downloads auto-subtitles via yt-dlp and parses VTT text.
    May fail if YouTube demands sign-in, but worth trying.
    """
    import tempfile, glob, re as _re

    url = f"https://www.youtube.com/watch?v={video_id}"
    with tempfile.TemporaryDirectory() as tmpdir:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "writeautomaticsub": True,
            "writesubtitles": True,
            "subtitleslangs": TRANSCRIPT_LANGS + ["ar-SA", "ar-MA"],
            "subtitlesformat": "vtt",
            "outtmpl": f"{tmpdir}/%(id)s.%(ext)s",
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        vtt_files = glob.glob(f"{tmpdir}/*.vtt")
        if not vtt_files:
            raise ValueError(f"No VTT files for {video_id}")

        chosen = vtt_files[0]
        for f in vtt_files:
            if ".ar" in f:
                chosen = f
                break

        lang_code = chosen.rsplit(".", 2)[-2] if chosen.count(".") >= 2 else "ar"
        with open(chosen, "r", encoding="utf-8") as f:
            vtt = f.read()

        lines = vtt.splitlines()
        text_lines = []
        for line in lines:
            if (line.startswith("WEBVTT") or line.startswith("NOTE")
                    or "-->" in line or not line.strip()
                    or _re.match(r"^\d+$", line.strip())):
                continue
            clean = _re.sub(r"<[^>]+>", "", line).strip()
            if clean:
                text_lines.append(clean)

        deduped = []
        for line in text_lines:
            if not deduped or line != deduped[-1]:
                deduped.append(line)

        text = " ".join(deduped).strip()
        if not text:
            raise ValueError(f"Empty VTT for {video_id}")
        return text, f"ytdlp:{lang_code}"


# ─────────────────────────────────────────────
# STEP 2 — ORCHESTRATE TRANSCRIPT FETCH
# ─────────────────────────────────────────────
def get_transcript(video_id: str) -> tuple[str, str]:
    """
    3-tier transcript extraction:
      1. youtube_transcript_api (fastest, no browser)
      2. Playwright browser — clicks 'Show transcript' button on YouTube
      3. yt-dlp VTT download (last resort, may fail if YouTube demands sign-in)
    """
    # ── Tier 1: youtube_transcript_api ──
    try:
        return get_transcript_api(video_id)
    except CouldNotRetrieveTranscript:
        log.warning(f"   [{video_id}] API: no transcript → browser")
    except Exception as e:
        log.warning(f"   [{video_id}] API error: {e} → browser")

    # ── Tier 2: Playwright browser ──
    try:
        time.sleep(1)
        return get_transcript_browser(video_id)
    except Exception as e:
        log.warning(f"   [{video_id}] Browser fallback failed: {e} → yt-dlp")

    # ── Tier 3: yt-dlp VTT ──
    try:
        return get_transcript_ytdlp(video_id)
    except Exception as e:
        log.warning(f"   [{video_id}] yt-dlp also failed: {e}")
        return "", "none"


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=== YouTube Channel Scraper Started ===")
    scraped_ids = load_scraped_ids()

    videos = get_video_ids()
    if not videos:
        log.error("No videos found — check the channel URL or search query.")
        return

    new_count = 0
    for i, vid in enumerate(videos, 1):
        vid_id = vid["video_id"]

        if vid_id in scraped_ids:
            log.info(f"   [{i}/{len(videos)}] Already scraped: {vid_id} — skip")
            continue

        log.info(f"   [{i}/{len(videos)}] Processing: {vid['title'][:60]} ({vid_id})")

        transcript, transcript_source = get_transcript(vid_id)

        record = {
            "video_id":          vid_id,
            "title":             vid["title"],
            "url":               vid["url"],
            "upload_date":       vid["upload_date"],
            "duration_seconds":  vid["duration"],
            "view_count":        vid["view_count"],
            "description":       vid["description"],
            "transcript":        transcript,
            "transcript_source": transcript_source,
            "has_transcript":    bool(transcript),
            "scraped_at":        __import__("datetime").datetime.now().isoformat(),
        }

        with open(JSONL_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        scraped_ids.add(vid_id)
        new_count += 1

        log.info(
            f"   ✅ Saved | transcript={'yes' if transcript else 'NO'} "
            f"({transcript_source}) | {len(transcript)} chars"
        )

        # Polite delay between transcript requests
        time.sleep(2)

    log.info(f"🎉 DONE — {new_count} new videos saved to {JSONL_FILE}")


if __name__ == "__main__":
    main()
