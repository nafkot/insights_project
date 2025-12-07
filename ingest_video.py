import sqlite3
import json
from datetime import datetime
from ingestion.youtube_client import get_authenticated_service, get_video_metadata
from ingestion.transcript import get_transcript_segments
from ingestion.extraction import extract_entities_for_video
from config import DB_PATH

def video_already_exists(video_id):
    """Checks if video is already fully ingested in the DB."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # FIX: Select 'video_id' (or 1) instead of 'id', which doesn't exist
    row = cursor.execute("SELECT video_id FROM videos WHERE video_id = ?", (video_id,)).fetchone()
    conn.close()
    return row is not None

def upsert_product(conn, product_name, brand_name=None):
    """
    Ensures product exists. If brand_name is provided, tries to link/create brand.
    Returns product_id.
    """
    c = conn.cursor()

    # 1. Handle Brand
    brand_id = None
    if brand_name:
        brand_norm = brand_name.strip().lower()
        # Find existing brand
        row = c.execute("SELECT id FROM brands WHERE normalized_name = ?", (brand_norm,)).fetchone()
        if row:
            brand_id = row[0]
        else:
            # Create new brand
            c.execute("INSERT INTO brands (name, normalized_name) VALUES (?, ?)", (brand_name, brand_norm))
            brand_id = c.lastrowid

    # 2. Handle Product
    product_norm = product_name.strip().lower()
    row = c.execute("SELECT id FROM products WHERE normalized_name = ? AND (brand_id = ? OR brand_id IS NULL)",
                    (product_norm, brand_id)).fetchone()

    if row:
        return row[0]
    else:
        c.execute("""
            INSERT INTO products (name, normalized_name, brand_id, brand_name)
            VALUES (?, ?, ?, ?)
        """, (product_name, product_norm, brand_id, brand_name))
        return c.lastrowid

def save_video_to_db(video_meta, segments):
    """
    Saves video metadata, transcript, and extracted entities to the database.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        # 1. Insert/Update Video
        c.execute("""
            INSERT INTO videos (
                video_id, channel_id, channel_name, title, description,
                upload_date, thumbnail_url, view_count, like_count, comment_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                comment_count=excluded.comment_count,
                title=excluded.title
        """, (
            video_meta["id"],
            video_meta["channel_id"],
            video_meta["channel_name"],
            video_meta["title"],
            video_meta["description"],
            video_meta["upload_date"],
            video_meta["thumbnail"],
            video_meta["stats"].get("viewCount", 0),
            video_meta["stats"].get("likeCount", 0),
            video_meta["stats"].get("commentCount", 0)
        ))

        # 2. Update Channel Stats (Safe Update)
        try:
            c.execute("UPDATE channels SET video_count = video_count + 1 WHERE channel_id = ?", (video_meta["channel_id"],))
        except Exception as e:
            # It's possible the channel doesn't exist if we skipped channel ingestion
            print(f"[WARN] Could not update channel stats: {e}")

        # 3. Save Transcript Segments
        c.execute("DELETE FROM video_segments WHERE video_id = ?", (video_meta["id"],))

        for seg in segments:
            c.execute("""
                INSERT INTO video_segments (video_id, start_time, end_time, text)
                VALUES (?, ?, ?, ?)
            """, (video_meta["id"], seg["start"], seg["duration"], seg["text"]))

        # 4. Extract Brands & Products
        brands, products, sponsors = extract_entities_for_video(video_meta["id"], segments)

        # 5. Link Extracted Data

        # A) Link Brands
        for b_name in brands:
            b_norm = b_name.strip().lower()
            c.execute("INSERT OR IGNORE INTO brands (name, normalized_name) VALUES (?, ?)", (b_name, b_norm))

            b_row = c.execute("SELECT id FROM brands WHERE normalized_name = ?", (b_norm,)).fetchone()
            if b_row:
                b_id = b_row[0]
                c.execute("""
                    INSERT INTO brand_mentions (brand_id, video_id, channel_id, mention_count, sentiment_score, first_seen_date)
                    VALUES (?, ?, ?, 1, 0, ?)
                """, (b_id, video_meta["id"], video_meta["channel_id"], video_meta["upload_date"]))

        # B) Link Products
        for p in products:
            if not p.get('product'): continue

            p_name = p['product']
            b_name = p.get('brand')

            p_id = upsert_product(conn, p_name, b_name)

            c.execute("""
                INSERT INTO product_mentions (product_id, video_id, channel_id, mention_count, sentiment_score, first_seen_date)
                VALUES (?, ?, ?, 1, 0, ?)
            """, (p_id, video_meta["id"], video_meta["channel_id"], video_meta["upload_date"]))

        conn.commit()

    except Exception as e:
        print(f"[ERROR] Failed to save video {video_meta.get('id', 'unknown')}: {e}")
        conn.rollback()
    finally:
        conn.close()

def ingest_single_video(video_id: str) -> None:
    # 1. Check DB
    if video_already_exists(video_id):
        print(f"[SKIP] Video {video_id} already fully ingested.")
        return

    # 2. Fetch Metadata
    youtube = get_authenticated_service()
    video_meta = get_video_metadata(youtube, video_id)
    if not video_meta:
        print(f"[{video_id}] Metadata not found (private/deleted?).")
        return

    # 3. Fetch Transcript
    segments = get_transcript_segments(video_id)
    if not segments:
        print(f"[{video_id}] No transcript available – skipping.")
        return

    # 4. Save & Process
    save_video_to_db(video_meta, segments)
