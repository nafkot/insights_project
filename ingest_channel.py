import argparse
import sys
import sqlite3
import re
from ingestion.youtube_client import get_authenticated_service, get_channel_details, get_channel_videos
from ingest_video import ingest_single_video
from config import DB_PATH

try:
    from utils.social_extractor import extract_socials
except ImportError:
    def extract_socials(text): return {}

NON_ENGLISH_PATTERN = re.compile(r'[\u0400-\u04FF\u4e00-\u9fff\u3040-\u309F\u30A0-\u30FF\uAC00-\uD7AF\u0600-\u06FF]')

def is_english_channel(channel_data):
    text = (channel_data.get("title", "") + " " + channel_data.get("description", "")).strip()
    if NON_ENGLISH_PATTERN.search(text):
        return False
    return True

def ingest_channel(channel_id_or_handle, max_videos=10):
    youtube = get_authenticated_service()

    print(f"Fetching channel details for: {channel_id_or_handle}...")
    channel = get_channel_details(youtube, channel_id_or_handle)
    if not channel:
        print("Channel not found.")
        return

    if not is_english_channel(channel):
        print(f"⚠️  Skipping Channel: '{channel['title']}' detected as non-English.")
        return

    desc = channel.get("description", "")
    socials = extract_socials(desc)
    print(f"Found Socials: {socials}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        INSERT INTO channels (
            channel_id, title, description, subscriber_count, view_count, video_count, thumbnail_url, platform,
            email, website, instagram, tiktok, twitter, spotify, soundcloud
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'YouTube', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            title=excluded.title,
            description=excluded.description,
            subscriber_count=excluded.subscriber_count,
            view_count=excluded.view_count,
            video_count=excluded.video_count,
            thumbnail_url=excluded.thumbnail_url,
            email=excluded.email,
            website=excluded.website,
            instagram=excluded.instagram,
            tiktok=excluded.tiktok,
            twitter=excluded.twitter,
            spotify=excluded.spotify,
            soundcloud=excluded.soundcloud
    """, (
        channel["id"], channel["title"], channel["description"],
        channel["stats"]["subscriberCount"], channel["stats"]["viewCount"],
        channel["stats"]["videoCount"], channel["thumbnail"],
        socials.get("email"), socials.get("website"), socials.get("instagram"),
        socials.get("tiktok"), socials.get("twitter"), socials.get("spotify"),
        socials.get("soundcloud")
    ))
    conn.commit()
    conn.close()

    if max_videos > 0:
        print(f"Fetching last {max_videos} videos...")
        videos = get_channel_videos(youtube, channel["id"], limit=max_videos)

        total = len(videos)
        for i, v in enumerate(videos, 1):
            print(f"[{i}/{total}] Processing {v['title']}...")
            ingest_single_video(v["id"])
    else:
        print("Skipping video ingestion (max_videos=0). Channel details updated.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel", required=True, help="Channel ID or Handle")
    parser.add_argument("--max-videos", type=int, default=10)
    args = parser.parse_args()

    ingest_channel(args.channel, args.max_videos)
