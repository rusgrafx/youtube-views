# YouTube Video View Counter

Reads URLs from a text file (local or S3), fetches view counts, and updates
a CSV where each video is a unique row and each run date adds a new column.

## CSV layout:

`Video ID | Title | URL | 2026-03-01 | 2026-03-02 | ...`

## Local usage

```
    python youtube_views.py                        # urls.txt -> YYYY_views_log.csv
    python youtube_views.py urls.txt               # custom URL file
    python youtube_views.py urls.txt my_log.csv    # custom URL file and CSV
```

## AWS Lambda usage
Deploy this file as a Lambda function (Python 3.12, handler: youtube_views.lambda_handler).
Set these environment variables in the Lambda configuration:

```
    S3_BUCKET      my-youtube-views        (default)
    S3_URLS_KEY    youtube/urls.txt        (default: urls.txt)
    S3_CSV_PREFIX  youtube/                (default: "" - root of bucket)
```

The CSV is read from and written back to:

`s3://<S3_BUCKET>/<S3_CSV_PREFIX><YYYY>_views_log.csv`

IAM permissions required for the Lambda execution role:

```
s3:GetObject, s3:PutObject  on  arn:aws:s3:::my-youtube-views/*
s3:ListBucket  on  arn:aws:s3:::my-youtube-views
```

Schedule with EventBridge: `cron(0 9 * * ? *)` runs every day at 09:00 UTC.
