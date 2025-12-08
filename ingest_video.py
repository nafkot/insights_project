import sqlite3
import json
from datetime import datetime
from ingestion.youtube_client import get_authenticated_service, get_video_metadata
from ingestion.transcript import get_transcript_segments
from ingestion.extraction import extract_entities_for_video
from config import DB_PATH

def log_attempt(video_id, channel_id, status, step, error_msg=None):
    """Writes to the ingestion_logs table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO ingestion_logs (video_id, channel_id, status, step, error_message)
            VALUES (?, ?, ?, ?, ?)
        """, (video_id, channel_id, status, step, str(error_msg) if error_msg else None))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[LOG ERROR] Could not write log: {e}")

def video_already_exists(video_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    row = cursor.execute("SELECT video_id FROM videos WHERE video_id = ?", (video_id,)).fetchone()
    conn.close()
    return row is not None

def upsert_product(conn, product_name, brand_name=None):
    """Safely ensures product exists, handling unique constraints."""
    c = conn.cursor()
    
    # 1. Handle Brand
    brand_id = None
    if brand_name:
        brand_norm = brand_name.strip().lower()
        row = c.execute("SELECT id FROM brands WHERE normalized_name = ?", (brand_norm,)).fetchone()
        if row:
            brand_id = row[0]
        else:
            try:
                c.execute("INSERT INTO brands (name, normalized_name) VALUES (?, ?)", (brand_name, brand_norm))
                brand_id = c.lastrowid
            except sqlite3.IntegrityError:
                row = c.execute("SELECT id FROM brands WHERE name = ?", (brand_name,)).fetchone()
                if row: brand_id = row[0]

    # 2. Handle Product
    product_norm = product_name.strip().lower()
    row = c.execute("SELECT id FROM products WHERE normalized_name = ? AND (brand_id = ? OR brand_id IS NULL)", 
                    (product_norm, brand_id)).fetchone()
    if row: return row[0]
    
    try:
        c.execute("INSERT INTO products (name, normalized_name, brand_id, brand_name) VALUES (?, ?, ?, ?)", 
                  (product_name, product_norm, brand_id, brand_name))
        return c.lastrowid
    except sqlite3.IntegrityError:
        row = c.execute("SELECT id FROM products WHERE name = ?", (product_name,)).fetchone()
        return row[0] if row else None

def save_video_to_db(video_meta, segments):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        # 1. AI Extraction (Unpack 5 values safely)
        extraction_result = extract_entities_for_video(video_meta["id"], segments)
        
        # Handle mismatch if cache has old 4-item tuples
        if len(extraction_result) == 5:
            brands, products, sponsors, topics, summary = extraction_result
        else:
            brands, products, sponsors, topics = extraction_result[:4]
            summary = ""

        # 2. Insert Video
        youtube_tags = video_meta.get("tags", [])
        # Merge AI topics with YouTube tags
        final_topics = list(set(youtube_tags + topics))
        topics_str = ",".join(final_topics)

        c.execute("""
            INSERT INTO videos (
                video_id, channel_id, channel_name, title, description, 
                upload_date, thumbnail_url, view_count, like_count, comment_count, 
                topics, overall_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                comment_count=excluded.comment_count,
                title=excluded.title,
                topics=excluded.topics,
                overall_summary=excluded.overall_summary
        """, (
            video_meta["id"], video_meta["channel_id"], video_meta["channel_name"],
            video_meta["title"], video_meta["description"], video_meta["upload_date"],
            video_meta["thumbnail"], video_meta["stats"].get("viewCount", 0),
            video_meta["stats"].get("likeCount", 0), video_meta["stats"].get("commentCount", 0),
            topics_str, 
            summary
        ))

        try:
            c.execute("UPDATE channels SET video_count = video_count + 1 WHERE channel_id = ?", (video_meta["channel_id"],))
        except: pass

        # 3. Save Transcript
        c.execute("DELETE FROM video_segments WHERE video_id = ?", (video_meta["id"],))
        for seg in segments:
            start = seg.get("start", 0)
            text = seg.get("text", "")
            
            if "end" in seg: end = seg["end"]
            elif "duration" in seg: end = start + seg["duration"]
            else: end = start + 5.0
                
            c.execute("INSERT INTO video_segments (video_id, start_time, end_time, text) VALUES (?, ?, ?, ?)", 
                      (video_meta["id"], start, end, text))

        # 4. Link Brands & Products
        for b_name in brands:
            b_norm = b_name.strip().lower()
            c.execute("INSERT OR IGNORE INTO brands (name, normalized_name) VALUES (?, ?)", (b_name, b_norm))
            try:
                b_row = c.execute("SELECT id FROM brands WHERE normalized_name = ?", (b_norm,)).fetchone()
                if not b_row: b_row = c.execute("SELECT id FROM brands WHERE name = ?", (b_name,)).fetchone()
                
                if b_row:
                    b_id = b_row[0]
                    c.execute("""
                        INSERT INTO brand_mentions (brand_id, video_id, channel_id, mention_count, sentiment_score, first_seen_date)
                        VALUES (?, ?, ?, 1, 0, ?)
                    """, (b_id, video_meta["id"], video_meta["channel_id"], video_meta["upload_date"]))
            except: pass

        for p in products:
            if not p.get('product'): continue
            p_id = upsert_product(conn, p['product'], p.get('brand'))
            if p_id:
                c.execute("""
                    INSERT INTO product_mentions (product_id, video_id, channel_id, mention_count, sentiment_score, first_seen_date)
                    VALUES (?, ?, ?, 1, 0, ?)
                """, (p_id, video_meta["id"], video_meta["channel_id"], video_meta["upload_date"]))

        conn.commit()
        log_attempt(video_meta["id"], video_meta["channel_id"], "SUCCESS", "DB_SAVE", "Ingested successfully")

    except Exception as e:
        print(f"[ERROR] DB Save failed: {e}")
        conn.rollback()
        log_attempt(video_meta.get("id"), video_meta.get("channel_id"), "FAILED", "DB_SAVE", str(e))
    finally:
        conn.close()

def ingest_single_video(video_id: str) -> None:
    if video_already_exists(video_id):
        # SKIP LOGIC COMMENTED OUT FOR TESTING UPDATES
        # print(f"[SKIP] Video {video_id} already ingested.")
        # return
        pass

    youtube = get_authenticated_service()
    video_meta = get_video_metadata(youtube, video_id)
    
    if not video_meta:
        print(f"[{video_id}] Metadata not found.")
        log_attempt(video_id, "unknown", "FAILED", "METADATA", "Video not found/Private")
        return

    segments = get_transcript_segments(video_id)
    if not segments:
        print(f"[{video_id}] No transcript available.")
        log_attempt(video_id, video_meta.get("channel_id"), "FAILED", "TRANSCRIPT", "No subtitles found")
        return

    save_video_to_db(video_meta, segments)
