# ingest_channel.py

import argparse
import concurrent.futures
from typing import List

from ingestion.youtube_client import (
    get_authenticated_service,
    get_channel_uploads_playlist_id,
    get_latest_video_ids,
)
from ingest_video import ingest_single_video


def ingest_channel(channel_id: str, max_videos: int = 20):
    """
    Ingest the latest N videos from a given YouTube channel using multiple threads.
    """
    youtube = get_authenticated_service()
    uploads_playlist_id = get_channel_uploads_playlist_id(youtube, channel_id)
    if not uploads_playlist_id:
        print(f"[{channel_id}] No uploads playlist found.")
        return

    video_ids = get_latest_video_ids(youtube, uploads_playlist_id, limit=max_videos)
    print(f"[{channel_id}] Found {len(video_ids)} recent videos. Starting ingestion...")

    # Run ingestion in parallel (4 workers is safe for SQLite)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(ingest_single_video, vid): vid for vid in video_ids}

        for future in concurrent.futures.as_completed(futures):
            vid = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[{channel_id}] Error ingesting video {vid}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest channels into analytics DB")
    parser.add_argument(
        "--channel",
        dest="channels",
        action="append",
        help="YouTube channel ID (can be used multiple times)",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=20,
        help="Max number of recent videos to ingest per channel.",
    )

    args = parser.parse_args()

    if not args.channels:
        print("Please provide at least one --channel <CHANNEL_ID>")
        raise SystemExit(1)

    for ch_id in args.channels:
        ingest_channel(ch_id, max_videos=args.max_videos)


if __name__ == "__main__":
    main()
