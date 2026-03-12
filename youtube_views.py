"""
YouTube Video View Counter

Reads URLs from a text file (local or S3), fetches view counts, and updates
a JSON file where each video is an entry and each run date adds a new data point.

JSON layout:

    {
        "dates": ["2026-03-01", "2026-03-02", ...],
        "songs": [
            {
                "id": "VIDEO_ID",
                "title": "Song Title",
                "url": "https://www.youtube.com/watch?v=VIDEO_ID",
                "views": {"2026-03-01": 123, "2026-03-02": 456, ...}
            },
            ...
        ]
    }

Local usage:

    python youtube_views.py                          # urls.txt -> YYYY_views_data.json
    python youtube_views.py urls.txt                 # custom URL file
    python youtube_views.py urls.txt my_data.json    # custom URL file and JSON

AWS Lambda usage:
    Deploy as a Lambda function (Python 3.12, handler: youtube_views.lambda_handler).
    Set these environment variables in the Lambda configuration:

    - S3_BUCKET      -- my-youtube-views  (default)
    - S3_URLS_KEY    -- urls.txt          (default)
    - S3_JSON_PREFIX -- youtube/          (default: "" - root of bucket)

    The JSON is read from and written back to:
    s3://<S3_BUCKET>/<S3_JSON_PREFIX><YYYY>_views_data.json

    IAM permissions required for the Lambda execution role:
    - s3:GetObject, s3:PutObject  on  arn:aws:s3:::my-youtube-views/*
    - s3:ListBucket               on  arn:aws:s3:::my-youtube-views

    Schedule with EventBridge: cron(0 9 * * ? *) runs every day at 09:00 UTC.
"""

import json
import os
import re
import sys
import urllib.request
from datetime import datetime


# -- YouTube helpers -----------------------------------------------------------

def extract_video_id(url):
    """Return the 11-character YouTube video ID from any recognised URL format."""
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


def get_video_info(url):
    """Fetch title and view count for a YouTube URL.

    Returns a dict with keys: video_id, url, title, views.
    Raises ValueError if the video ID cannot be extracted or view count parsed.
    """
    video_id = extract_video_id(url)
    if not video_id:
        raise ValueError(f"Could not extract a valid video ID from: {url}")

    page_url = "https://www.youtube.com/watch?v=" + video_id
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

    views = _parse_view_count(html)
    title = _parse_title(html)
    return {"video_id": video_id, "url": page_url, "title": title, "views": views}


def _parse_view_count(html):
    """Extract the view count integer from a YouTube page HTML string.

    Tries multiple regex patterns to handle both desktop and Lambda responses.
    Raises ValueError if no pattern matches.
    """
    # Lambda gets a different page variant than a desktop browser,
    # so multiple fallbacks are needed.
    view_patterns = [
        r'"viewCount":"(\d+)"',                          # desktop response
        r'"viewCount":\{"videoViewCountRenderer":'
        r'\{"viewCount":\{"simpleText":"([\d,]+)',        # Lambda response
        r'"originalViewCount":"(\d+)"',                  # Lambda fallback
        r'interactionCount"[^>]*content="(\d+)"',        # meta tag fallback
    ]
    for pattern in view_patterns:
        match = re.search(pattern, html)
        if match:
            return int(match.group(1).replace(",", ""))
    raise ValueError("Could not parse view count.")


def _parse_title(html):
    """Extract the video title from a YouTube page HTML string.

    Tries multiple regex patterns to handle both desktop and Lambda responses.
    Returns 'Unknown' if no pattern matches.
    """
    # Patterns tried in order:
    #   runs array (Lambda), overlay simpleText (Lambda), plain string (desktop).
    t1 = re.search(r'"title":\{"runs":\[\{"text":"((?:[^"\\]|\\.)*)"', html)
    t2 = re.search(
        r'"playerOverlayVideoDetailsRenderer":'
        r'\{"title":\{"simpleText":"((?:[^"\\]|\\.)*)"',
        html,
    )
    t3 = re.search(r'"title":"((?:[^"\\]|\\.)*)"', html)
    for t_match in [t1, t2, t3]:
        if t_match:
            try:
                return json.loads('"' + t_match.group(1) + '"')
            except (json.JSONDecodeError, ValueError):
                continue
    return "Unknown"


# -- URL parsing ---------------------------------------------------------------

def parse_urls(text):
    """Return a list of URLs from a newline-delimited string, ignoring comments."""
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


# -- Local file I/O ------------------------------------------------------------

def read_urls_local(filepath):
    """Read and return URLs from a local file. Exits on error."""
    if not os.path.exists(filepath):
        print(f"Error: URL file '{filepath}' not found.")
        sys.exit(1)
    with open(filepath, encoding="utf-8") as f:
        urls = parse_urls(f.read())
    if not urls:
        print(f"No URLs found in '{filepath}'.")
        sys.exit(1)
    return urls


def load_json_local(filepath):
    """Load the views JSON from a local file, or return an empty structure."""
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return {"dates": [], "songs": []}
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def save_json_local(filepath, data):
    """Write the views data dict to a local JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# -- S3 I/O --------------------------------------------------------------------

def _s3():
    """Return a boto3 S3 client (imported lazily to keep Lambda cold-start fast)."""
    import boto3  # pylint: disable=import-outside-toplevel
    return boto3.client("s3")


def read_urls_s3(bucket, key):
    """Read and return URLs from an S3 object."""
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
    except Exception as exc:
        raise FileNotFoundError(
            f"s3://{bucket}/{key} not found: {exc}"
        ) from exc
    urls = parse_urls(text)
    if not urls:
        raise ValueError(f"No URLs found in s3://{bucket}/{key}")
    return urls


def load_json_s3(bucket, key):
    """Load the views JSON from S3, or return an empty structure if not found."""
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if "NoSuchKey" in type(exc).__name__ or "NoSuchKey" in str(exc):
            return {"dates": [], "songs": []}
        raise


def save_json_s3(bucket, key, data):
    """Write the views data dict to an S3 object."""
    s3 = _s3()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


# -- Core logic (shared by both modes) -----------------------------------------

def run(urls, data, today, log):
    """Fetch view counts for all URLs and update data in-place.

    Returns the updated data dict and the number of successfully updated videos.
    """
    if today not in data["dates"]:
        data["dates"].append(today)

    songs_by_id = {s["id"]: s for s in data["songs"]}

    updated = 0
    for i, url in enumerate(urls, 1):
        log(f"[{i}/{len(urls)}] {url}")
        try:
            info = get_video_info(url)
            vid = info["video_id"]
            if vid not in songs_by_id:
                song = {
                    "id": vid,
                    "title": info["title"],
                    "url": info["url"],
                    "views": {},
                }
                data["songs"].append(song)
                songs_by_id[vid] = song
            else:
                songs_by_id[vid]["title"] = info["title"]
                songs_by_id[vid]["url"] = info["url"]
            songs_by_id[vid]["views"][today] = info["views"]
            updated += 1
            log(f"        Title : {info['title']}")
            log(f"        Views : {info['views']:,}\n")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log(f"        Error : {exc}\n")

    return data, updated


# -- Lambda entry point --------------------------------------------------------

def lambda_handler(event, context):  # pylint: disable=unused-argument
    """AWS Lambda entry point. Reads from S3, updates view counts, writes back."""
    bucket = os.environ.get("S3_BUCKET", "my-youtube-views")
    urls_key = os.environ.get("S3_URLS_KEY", "urls.txt")
    json_prefix = os.environ.get("S3_JSON_PREFIX", "")

    today = datetime.utcnow().strftime("%Y-%m-%d")
    year = today[:4]
    json_key = json_prefix + year + "_views_data.json"

    messages = []
    log = messages.append

    log("=" * 60)
    log(f"  YouTube View Tracker (Lambda)  |  {today}")
    log("=" * 60)
    log(f"  URLs : s3://{bucket}/{urls_key}")
    log(f"  JSON : s3://{bucket}/{json_key}\n")

    urls = read_urls_s3(bucket, urls_key)
    data = load_json_s3(bucket, json_key)
    data, n = run(urls, data, today, log)

    if n:
        save_json_s3(bucket, json_key, data)
        suffix = "s" if n != 1 else ""
        log(f"  + {n} video{suffix} updated in s3://{bucket}/{json_key}")

    output = "\n".join(messages)
    print(output)
    return {"statusCode": 200, "body": output}


# -- Local CLI entry point -----------------------------------------------------

def main():
    """Local CLI entry point. Reads URLs and JSON from the filesystem."""
    url_file = sys.argv[1] if len(sys.argv) > 1 else "urls.txt"
    today = datetime.now().strftime("%Y-%m-%d")
    year = today[:4]
    json_file = sys.argv[2] if len(sys.argv) > 2 else year + "_views_data.json"

    urls = read_urls_local(url_file)
    data = load_json_local(json_file)

    url_suffix = "s" if len(urls) != 1 else ""
    print("=" * 60)
    print(f"  YouTube View Tracker  |  {today}")
    print("=" * 60)
    print(f"  URLs file : {url_file}  ({len(urls)} URL{url_suffix})")
    print(f"  Output    : {json_file}")
    print("=" * 60 + "\n")

    data, n = run(urls, data, today, print)

    if n:
        suffix = "s" if n != 1 else ""
        save_json_local(json_file, data)
        print("=" * 60)
        print(f"  + {n} video{suffix} updated in '{json_file}'")
        print("=" * 60)
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()
