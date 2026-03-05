"""
YouTube Video View Counter
Reads URLs from a text file (local or S3), fetches view counts, and updates
a CSV where each video is a unique row and each run date adds a new column.

CSV layout:
    Video ID | Title | URL | 2026-03-01 | 2026-03-02 | ...

── Local usage ──────────────────────────────────────────────────────────────
    python youtube_views.py                        # urls.txt → YYYY_views_log.csv
    python youtube_views.py urls.txt               # custom URL file
    python youtube_views.py urls.txt my_log.csv    # custom URL file and CSV

── AWS Lambda usage ─────────────────────────────────────────────────────────
Deploy this file as a Lambda function (Python 3.12, handler: youtube_views.lambda_handler).
Set these environment variables in the Lambda configuration:

    S3_BUCKET      my-bucket-name          (required)
    S3_URLS_KEY    youtube/urls.txt        (default: urls.txt)
    S3_CSV_PREFIX  youtube/               (default: "" — root of bucket)

The CSV is read from and written back to:
    s3://<S3_BUCKET>/<S3_CSV_PREFIX><YYYY>_views_log.csv

IAM permissions required for the Lambda execution role:
    s3:GetObject, s3:PutObject  on  arn:aws:s3:::<S3_BUCKET>/*

Schedule it with EventBridge (CloudWatch Events) — e.g. cron(0 9 * * ? *)
to run every day at 09:00 UTC.
"""

import csv
import io
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


# ── URL parsing ───────────────────────────────────────────────────────────────

def parse_urls(text: str) -> list[str]:
    """Extract URLs from raw text, skipping blank lines and comments."""
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


# ── Local file I/O ────────────────────────────────────────────────────────────

def read_urls_local(filepath: str) -> list[str]:
    if not os.path.exists(filepath):
        print(f"Error: URL file '{filepath}' not found.")
        sys.exit(1)
    with open(filepath, encoding="utf-8") as f:
        urls = parse_urls(f.read())
    if not urls:
        print(f"No URLs found in '{filepath}'.")
        sys.exit(1)
    return urls


def load_csv_local(filepath: str) -> tuple[list[str], dict[str, dict]]:
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return list(FIXED_HEADERS), {}
    with open(filepath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or FIXED_HEADERS)
        rows = {row["Video ID"]: dict(row) for row in reader}
    return headers, rows


def save_csv_local(filepath: str, headers: list[str], rows_by_id: dict[str, dict]) -> None:
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_by_id.values())


# ── S3 I/O ────────────────────────────────────────────────────────────────────

def _s3_client():
    import boto3
    return boto3.client("s3")


def read_urls_s3(bucket: str, key: str) -> list[str]:
    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found.")
    urls = parse_urls(text)
    if not urls:
        raise ValueError(f"No URLs found in s3://{bucket}/{key}.")
    return urls


def load_csv_s3(bucket: str, key: str) -> tuple[list[str], dict[str, dict]]:
    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        # Strip UTF-8 BOM if present
        raw = obj["Body"].read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(raw))
        headers = list(reader.fieldnames or FIXED_HEADERS)
        rows = {row["Video ID"]: dict(row) for row in reader}
        return headers, rows
    except Exception as e:
        if "NoSuchKey" in type(e).__name__ or "404" in str(e):
            return list(FIXED_HEADERS), {}
        raise


def save_csv_s3(bucket: str, key: str, headers: list[str], rows_by_id: dict[str, dict]) -> None:
    s3 = _s3_client()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows_by_id.values())
    # Encode with BOM so Excel opens Cyrillic/CJK correctly
    body = "\ufeff" + buf.getvalue()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )


# ── Core logic (shared by both modes) ────────────────────────────────────────

def run(urls: list[str], headers: list[str], rows_by_id: dict[str, dict], today: str, log) -> tuple[list[str], dict[str, dict], int]:
    if today not in headers:
        headers.append(today)

    updated = 0
    for i, url in enumerate(urls, 1):
        log(f"[{i}/{len(urls)}] {url}")
        try:
            info = get_video_info(url)
            vid = info["video_id"]
            if vid not in rows_by_id:
                row = {"Video ID": vid, "Title": info["title"], "URL": info["url"]}
                for col in headers:
                    if col not in FIXED_HEADERS:
                        row.setdefault(col, "")
                rows_by_id[vid] = row
            else:
                rows_by_id[vid]["Title"] = info["title"]
                rows_by_id[vid]["URL"] = info["url"]
            rows_by_id[vid][today] = info["views"]
            updated += 1
            log(f"        Title : {info['title']}")
            log(f"        Views : {info['views']:,}\n")
        except Exception as e:
            log(f"        Error : {e}\n")

    return headers, rows_by_id, updated


# ── Lambda entry point ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    bucket     = os.environ.get("S3_BUCKET", "ru-youtube-views")
    urls_key   = os.environ.get("S3_URLS_KEY", "urls.txt")
    csv_prefix = os.environ.get("S3_CSV_PREFIX", "")

    today    = datetime.utcnow().strftime("%Y-%m-%d")
    year     = today[:4]
    csv_key  = f"{csv_prefix}{year}_views_log.csv"

    messages = []
    log = messages.append

    log(f"{'─'*60}")
    log(f"  YouTube View Tracker (Lambda)  |  {today}")
    log(f"{'─'*60}")
    log(f"  URLs : s3://{bucket}/{urls_key}")
    log(f"  CSV  : s3://{bucket}/{csv_key}\n")

    urls                    = read_urls_s3(bucket, urls_key)
    headers, rows_by_id     = load_csv_s3(bucket, csv_key)
    headers, rows_by_id, n  = run(urls, headers, rows_by_id, today, log)

    if n:
        save_csv_s3(bucket, csv_key, headers, rows_by_id)
        log(f"  + {n} video{'s' if n != 1 else ''} updated in s3://{bucket}/{csv_key}")

    output = "\n".join(messages)
    print(output)
    return {"statusCode": 200, "body": output}


# ── Local CLI entry point ─────────────────────────────────────────────────────

def main():
    url_file = sys.argv[1] if len(sys.argv) > 1 else "urls.txt"
    today    = datetime.now().strftime("%Y-%m-%d")
    year     = today[:4]
    csv_file = sys.argv[2] if len(sys.argv) > 2 else f"{year}_views_log.csv"

    urls                    = read_urls_local(url_file)
    headers, rows_by_id     = load_csv_local(csv_file)

    print(f"{'─'*60}")
    print(f"  YouTube View Tracker  |  {today}")
    print(f"{'─'*60}")
    print(f"  URLs file : {url_file}  ({len(urls)} URL{'s' if len(urls) != 1 else ''})")
    print(f"  Output    : {csv_file}")
    print(f"{'─'*60}\n")

    headers, rows_by_id, n = run(urls, headers, rows_by_id, today, print)

    if n:
        save_csv_local(csv_file, headers, rows_by_id)
        print(f"{'─'*60}")
        print(f"  + {n} video{'s' if n != 1 else ''} updated in '{csv_file}'")
        print(f"{'─'*60}")
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()
