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

-- Local usage --
    python youtube_views.py                            # urls.txt -> YYYY_views_data.json
    python youtube_views.py urls.txt                   # custom URL file
    python youtube_views.py urls.txt my_data.json      # custom URL file and JSON

-- AWS Lambda usage --
Deploy this file as a Lambda function (Python 3.12, handler: youtube_views.lambda_handler).
Set these environment variables in the Lambda configuration:

    S3_BUCKET       my-youtube-views        (default)
    S3_URLS_KEY     youtube/urls.txt        (default: urls.txt)
    S3_JSON_PREFIX  youtube/                (default: "" - root of bucket)

The JSON is read from and written back to:
    s3://<S3_BUCKET>/<S3_JSON_PREFIX><YYYY>_views_data.json

IAM permissions required for the Lambda execution role:
    s3:GetObject, s3:PutObject  on  arn:aws:s3:::my-youtube-views/*
    s3:ListBucket               on  arn:aws:s3:::my-youtube-views

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
    video_id = extract_video_id(url)
    if not video_id:
        raise ValueError("Could not extract a valid video ID from: " + url)

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

    # --- View count ---
    # Try patterns in order of preference; Lambda gets a different page variant
    # than a desktop browser, so multiple fallbacks are needed.
    views = None
    view_patterns = [
        r'"viewCount":"(\d+)"',                                                             # desktop response
        r'"viewCount":\{"videoViewCountRenderer":\{"viewCount":\{"simpleText":"([\d,]+)',  # Lambda response
        r'"originalViewCount":"(\d+)"',                                                     # Lambda fallback
        r'interactionCount"[^>]*content="(\d+)"',                                          # meta tag fallback
    ]
    for vp in view_patterns:
        m = re.search(vp, html)
        if m:
            views = int(m.group(1).replace(",", ""))
            break
    if views is None:
        raise ValueError("Could not parse view count.")

    # --- Title ---
    # Lambda returns title inside JSON structures rather than a plain string.
    # Patterns tried in order: runs array (Lambda), overlay simpleText (Lambda), plain string (desktop).
    title = "Unknown"
    t1 = re.search(r'"title":\{"runs":\[\{"text":"((?:[^"\\]|\\.)*)"', html)
    t2 = re.search(r'"playerOverlayVideoDetailsRenderer":\{"title":\{"simpleText":"((?:[^"\\]|\\.)*)"', html)
    t3 = re.search(r'"title":"((?:[^"\\]|\\.)*)"', html)
    for t_match in [t1, t2, t3]:
        if t_match:
            try:
                title = json.loads('"' + t_match.group(1) + '"')
                break
            except Exception:
                continue

    return {"video_id": video_id, "url": page_url, "title": title, "views": views}


# -- URL parsing ---------------------------------------------------------------

def parse_urls(text):
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


# -- Local file I/O ------------------------------------------------------------

def read_urls_local(filepath):
    if not os.path.exists(filepath):
        print("Error: URL file '" + filepath + "' not found.")
        sys.exit(1)
    with open(filepath, encoding="utf-8") as f:
        urls = parse_urls(f.read())
    if not urls:
        print("No URLs found in '" + filepath + "'.")
        sys.exit(1)
    return urls


def load_json_local(filepath):
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return {"dates": [], "songs": []}
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def save_json_local(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# -- S3 I/O --------------------------------------------------------------------

def _s3():
    import boto3
    return boto3.client("s3")


def read_urls_s3(bucket, key):
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
    except Exception as e:
        raise FileNotFoundError("s3://" + bucket + "/" + key + " not found: " + str(e))
    urls = parse_urls(text)
    if not urls:
        raise ValueError("No URLs found in s3://" + bucket + "/" + key)
    return urls


def load_json_s3(bucket, key):
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        if "NoSuchKey" in type(e).__name__ or "NoSuchKey" in str(e):
            return {"dates": [], "songs": []}
        raise


def save_json_s3(bucket, key, data):
    s3 = _s3()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


# -- Core logic (shared by both modes) -----------------------------------------

def run(urls, data, today, log):
    if today not in data["dates"]:
        data["dates"].append(today)

    # Index existing songs by video ID for fast lookup
    songs_by_id = {s["id"]: s for s in data["songs"]}

    updated = 0
    for i, url in enumerate(urls, 1):
        log("[" + str(i) + "/" + str(len(urls)) + "] " + url)
        try:
            info = get_video_info(url)
            vid = info["video_id"]
            if vid not in songs_by_id:
                song = {"id": vid, "title": info["title"], "url": info["url"], "views": {}}
                data["songs"].append(song)
                songs_by_id[vid] = song
            else:
                songs_by_id[vid]["title"] = info["title"]
                songs_by_id[vid]["url"]   = info["url"]
            songs_by_id[vid]["views"][today] = info["views"]
            updated += 1
            log("        Title : " + info["title"])
            log("        Views : " + "{:,}".format(info["views"]) + "\n")
        except Exception as e:
            log("        Error : " + str(e) + "\n")

    return data, updated


# -- Lambda entry point --------------------------------------------------------

def lambda_handler(event, context):
    bucket      = os.environ.get("S3_BUCKET", "my-youtube-views")
    urls_key    = os.environ.get("S3_URLS_KEY", "urls.txt")
    json_prefix = os.environ.get("S3_JSON_PREFIX", "")

    today    = datetime.utcnow().strftime("%Y-%m-%d")
    year     = today[:4]
    json_key = json_prefix + year + "_views_data.json"

    messages = []
    log = messages.append

    log("=" * 60)
    log("  YouTube View Tracker (Lambda)  |  " + today)
    log("=" * 60)
    log("  URLs : s3://" + bucket + "/" + urls_key)
    log("  JSON : s3://" + bucket + "/" + json_key + "\n")

    urls    = read_urls_s3(bucket, urls_key)
    data    = load_json_s3(bucket, json_key)
    data, n = run(urls, data, today, log)

    if n:
        save_json_s3(bucket, json_key, data)
        log("  + " + str(n) + " video" + ("s" if n != 1 else "") + " updated in s3://" + bucket + "/" + json_key)

    output = "\n".join(messages)
    print(output)
    return {"statusCode": 200, "body": output}


# -- Local CLI entry point -----------------------------------------------------

def main():
    url_file  = sys.argv[1] if len(sys.argv) > 1 else "urls.txt"
    today     = datetime.now().strftime("%Y-%m-%d")
    year      = today[:4]
    json_file = sys.argv[2] if len(sys.argv) > 2 else year + "_views_data.json"

    urls = read_urls_local(url_file)
    data = load_json_local(json_file)

    print("=" * 60)
    print("  YouTube View Tracker  |  " + today)
    print("=" * 60)
    print("  URLs file : " + url_file + "  (" + str(len(urls)) + " URL" + ("s" if len(urls) != 1 else "") + ")")
    print("  Output    : " + json_file)
    print("=" * 60 + "\n")

    data, n = run(urls, data, today, print)

    if n:
        save_json_local(json_file, data)
        print("=" * 60)
        print("  + " + str(n) + " video" + ("s" if n != 1 else "") + " updated in '" + json_file + "'")
        print("=" * 60)
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()
