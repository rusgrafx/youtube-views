"""
YouTube Video View Counter
Gets the view count from a YouTube video URL without requiring an API key.
"""

import json
import re
import sys
import urllib.request


def extract_video_id(url: str) -> str | None:
    """Extract the video ID from various YouTube URL formats."""
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


def get_view_count(url: str) -> dict:
    """
    Fetch view count and basic info from a YouTube video URL.

    Returns a dict with keys: video_id, title, views, views_formatted
    Raises ValueError on invalid URL or fetch failure.
    """
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

    # Extract view count
    view_patterns = [
        r'"viewCount":"(\d+)"',
        r'"views":\{"simpleText":"([\d,]+) views"\}',
        r'interactionCount"[^>]*content="(\d+)"',
    ]
    views = None
    for pattern in view_patterns:
        match = re.search(pattern, html)
        if match:
            views = int(match.group(1).replace(",", ""))
            break

    if views is None:
        raise ValueError("Could not parse view count — YouTube may have changed its page structure.")

    # Extract title
    title = "Unknown"
    title_match = re.search(r'"title":"((?:[^"\\]|\\.)*)"', html)
    if title_match:
        title = json.loads(f'"{title_match.group(1)}"')

    return {
        "video_id": video_id,
        "url": page_url,
        "title": title,
        "views": views,
        "views_formatted": f"{views:,}",
    }


def main():
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = input("Enter YouTube video URL: ").strip()

    if not url:
        print("Error: No URL provided.")
        sys.exit(1)

    print(f"\nFetching data for: {url}")
    print("-" * 50)

    try:
        info = get_view_count(url)
        print(f"Title    : {info['title']}")
        print(f"Video ID : {info['video_id']}")
        print(f"Views    : {info['views_formatted']}")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
