"""
YouTube Video View Counter
Reads URLs from a text file, fetches view counts, and updates a CSV where
each video is a unique row and each execution date adds a new column.

CSV layout:
    Video ID | Title | URL | 2026-03-01 | 2026-03-02 | ...

Usage:
    python youtube_views.py                        # uses urls.txt and views_log.csv
    python youtube_views.py urls.txt               # custom URL file
    python youtube_views.py urls.txt my_log.csv    # custom URL file and CSV output
"""

import csv
import json
import os
import re
import sys
import urllib.request
from datetime import datetime

FIXED_HEADERS = ["Video ID", "Title", "URL"]


# ── YouTube helpers ───────────────────────────────────────────────────────────

def extract_video_id(url: str) -> str | None:
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"(?:embed\/)([0-9A-Za-z_-]{11})",
        r"(?:youtu\.be\/)([0-9A-Za-z_-]{11})",
        r"(?:shorts\/)([0-9A-Za-z_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def get_video_info(url: str) -> dict:
    video_id = extract_video_id(url)
    if not video_id:
        raise ValueError(f"Could not extract a valid video ID from: {url}")

    page_url = f"https://www.youtube.com/watch?v={video_id}"
    req = urllib.request.Request(
        page_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        html = response.read().decode("utf-8")

    views = None
    for pattern in [r'"viewCount":"(\d+)"', r'interactionCount"[^>]*content="(\d+)"']:
        match = re.search(pattern, html)
        if match:
            views = int(match.group(1).replace(",", ""))
            break
    if views is None:
        raise ValueError("Could not parse view count.")

    title = "Unknown"
    title_match = re.search(r'"title":"((?:[^"\\]|\\.)*)"', html)
    if title_match:
        title = json.loads(f'"{title_match.group(1)}"')

    return {"video_id": video_id, "url": page_url, "title": title, "views": views}


# ── URL file reader ───────────────────────────────────────────────────────────

def read_urls(filepath: str) -> list[str]:
    if not os.path.exists(filepath):
        print(f"Error: URL file '{filepath}' not found.")
        sys.exit(1)
    urls = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    if not urls:
        print(f"No URLs found in '{filepath}'.")
        sys.exit(1)
    return urls


# ── CSV read / write ──────────────────────────────────────────────────────────

def load_csv(filepath: str) -> tuple[list[str], dict[str, dict]]:
    """
    Returns (headers, rows_by_video_id).
    rows_by_video_id maps video_id -> full row dict.
    """
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return list(FIXED_HEADERS), {}

    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or list(FIXED_HEADERS)
        rows = {row["Video ID"]: dict(row) for row in reader}

    return list(headers), rows


def save_csv(filepath: str, headers: list[str], rows_by_id: dict[str, dict]) -> None:
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_by_id.values())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    url_file = sys.argv[1] if len(sys.argv) > 1 else "urls.txt"
    csv_file = sys.argv[2] if len(sys.argv) > 2 else "views_log.csv"
    today = datetime.now().strftime("%Y-%m-%d")

    urls = read_urls(url_file)

    print(f"{'─'*60}")
    print(f"  YouTube View Tracker  |  {today}")
    print(f"{'─'*60}")
    print(f"  URLs file : {url_file}  ({len(urls)} URL{'s' if len(urls) != 1 else ''})")
    print(f"  Output    : {csv_file}")
    print(f"{'─'*60}\n")

    # Load existing CSV state
    headers, rows_by_id = load_csv(csv_file)

    # Add today's date column if not already present
    if today not in headers:
        headers.append(today)

    updated = 0
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] {url}")
        try:
            info = get_video_info(url)
            vid = info["video_id"]

            if vid not in rows_by_id:
                # New video: initialise row with empty strings for past date columns
                row = {"Video ID": vid, "Title": info["title"], "URL": info["url"]}
                for col in headers:
                    if col not in FIXED_HEADERS:
                        row.setdefault(col, "")
                rows_by_id[vid] = row
            else:
                # Update title/URL in case they changed
                rows_by_id[vid]["Title"] = info["title"]
                rows_by_id[vid]["URL"] = info["url"]

            rows_by_id[vid][today] = info["views"]
            updated += 1
            print(f"        Title : {info['title']}")
            print(f"        Views : {info['views']:,}\n")
        except Exception as e:
            print(f"        Error : {e}\n")

    if updated:
        save_csv(csv_file, headers, rows_by_id)
        print(f"{'─'*60}")
        print(f"  + {updated} video{'s' if updated != 1 else ''} updated in '{csv_file}'")
        print(f"{'─'*60}")
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()
