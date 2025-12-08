import sqlite3
import argparse
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import DB_PATH

def reset_channel(channel_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print(f"--- Resetting Channel: {channel_id} ---")

    # 1. Get Videos
    videos = c.execute("SELECT video_id FROM videos WHERE channel_id = ?", (channel_id,)).fetchall()
    video_ids = [v[0] for v in videos]
    print(f"Found {len(video_ids)} videos to reset.")

    if not video_ids:
        print("No videos found. Nothing to do.")
        return

    # 2. Delete Mentions
    c.execute(f"DELETE FROM brand_mentions WHERE channel_id = ?", (channel_id,))
    c.execute(f"DELETE FROM product_mentions WHERE channel_id = ?", (channel_id,))
    print("Deleted mentions.")

    # 3. Delete AI Cache (CRITICAL: This forces re-extraction of topics)
    placeholders = ','.join(['?'] * len(video_ids))
    c.execute(f"DELETE FROM video_extraction_cache WHERE video_id IN ({placeholders})", video_ids)
    print("Deleted AI extraction cache.")

    # 4. Delete Videos (So ingest_video.py sees them as 'new')
    c.execute(f"DELETE FROM videos WHERE channel_id = ?", (channel_id,))
    print("Deleted video records.")

    # 5. Delete Segments
    c.execute(f"DELETE FROM video_segments WHERE video_id IN ({placeholders})", video_ids)
    print("Deleted transcript segments.")

    conn.commit()
    conn.close()
    print("✅ Channel reset complete. Run ingest_channel.py now.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel", required=True)
    args = parser.parse_args()
    reset_channel(args.channel)
