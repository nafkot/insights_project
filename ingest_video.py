# ingest_video.py

import sqlite3
from datetime import datetime
import json
from typing import Dict, Any

from ingestion.extraction import extract_entities_for_video
from config import DB_PATH
from llm_ingest import analyze_transcript
from ingestion.transcript_pipeline import get_transcript_segments
from ingestion.youtube_client import get_authenticated_service, get_video_metadata, get_channel_details


def get_db_connection():
    # Set a high timeout (30s) to handle concurrent writes from multiple threads
    return sqlite3.connect(DB_PATH, timeout=30.0)


def video_already_exists(video_id: str) -> bool:
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT 1 FROM videos WHERE video_id = ? LIMIT 1", (video_id,))
        row = c.fetchone()
        return row is not None
    finally:
        conn.close()

def sentiment_to_score(sentiment: str) -> int:
    s = (sentiment or "").lower()
    if "positive" in s:
        return 85
    if "negative" in s:
        return 15
    return 50


def normalize_name(name: str) -> str:
    return (name or "").strip().lower()


def upsert_brand(conn, name: str, category: str | None = None, meta: dict | None = None) -> int | None:
    if not name:
        return None
    norm = normalize_name(name)
    meta_json = json.dumps(meta or {})
    c = conn.cursor()
    c.execute("SELECT id FROM brands WHERE normalized_name = ?", (norm,))
    row = c.fetchone()
    if row:
        return row[0]
    c.execute(
        "INSERT INTO brands (name, normalized_name, category, meta) VALUES (?, ?, ?, ?)",
        (name, norm, category or "", meta_json),
    )
    conn.commit()
    return c.lastrowid


def upsert_sponsor(conn, name: str, category: str | None = None, meta: dict | None = None) -> int | None:
    if not name:
        return None
    norm = normalize_name(name)
    meta_json = json.dumps(meta or {})
    c = conn.cursor()
    c.execute("SELECT id FROM sponsors WHERE normalized_name = ?", (norm,))
    row = c.fetchone()
    if row:
        return row[0]
    c.execute(
        "INSERT INTO sponsors (name, normalized_name, category, meta) VALUES (?, ?, ?, ?)",
        (name, norm, category or "sponsor", meta_json),
    )
    conn.commit()
    return c.lastrowid


def upsert_product(conn, name: str, brand_name: str | None):
    if not name:
        return None

    norm = normalize_name(name)
    c = conn.cursor()

    # Get brand_id if exists
    brand_id = None
    if brand_name:
        row = c.execute(
            "SELECT id FROM brands WHERE lower(name)=?",
            (brand_name.strip().lower(),)
        ).fetchone()
        if row:
            brand_id = row[0]

    # Check if product exists
    row = c.execute(
        "SELECT id FROM products WHERE lower(name)=?", (norm,)
    ).fetchone()

    if row:
        product_id = row[0]
        # update brand_id only if missing
        if brand_id:
            c.execute(
                "UPDATE products SET brand_id=? WHERE id=? AND brand_id IS NULL",
                (brand_id, product_id)
            )
        conn.commit()
        return product_id

    # Insert new
    c.execute(
        "INSERT INTO products (name, brand_id) VALUES (?, ?)",
        (name, brand_id)
    )
    conn.commit()
    return c.lastrowid


def _segments_to_text(segments) -> str:
    if isinstance(segments, list):
        return "\n".join(s.get("text", "") for s in segments if "text" in s)
    return ""


def save_video_to_db(video_meta: Dict[str, Any], transcript_segments) -> None:
    conn = get_db_connection()
    c = conn.cursor()

    try:
        # --- NEW BLOCK: Upsert Channel Data ---
        try:
            yt = get_authenticated_service()
            ch_data = get_channel_details(yt, video_meta["channel_id"])

            if ch_data:
                c.execute("""
                    INSERT INTO channels (
                        channel_id, title, description, subscriber_count,
                        video_count, view_count, creation_date,
                        thumbnail_url, platform
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        subscriber_count = excluded.subscriber_count,
                        video_count = excluded.video_count,
                        view_count = excluded.view_count,
                        thumbnail_url = excluded.thumbnail_url,
                        platform = excluded.platform
                """, (
                    ch_data["channel_id"],
                    ch_data["title"],
                    ch_data["description"],
                    ch_data["subscriber_count"],
                    ch_data["video_count"],
                    ch_data["view_count"],
                    ch_data["creation_date"],
                    ch_data["thumbnail_url"],
                    "YouTube"
                ))
                conn.commit()
        except Exception as e:
            print(f"[WARN] Could not update channel details: {e}")
        # --------------------------------------

        # 1) LLM ingestion analysis
        text = _segments_to_text(transcript_segments or [])
        analysis = analyze_transcript(
            title=video_meta.get("title", ""),
            channel=video_meta.get("channel", ""),
            text=text,
        )

        brands, products, sponsors = extract_entities_for_video(
            video_meta["id"],
            transcript_segments or [],
        )

        topics = analysis.get("topics", []) or []
        topics_str = ",".join(topics) if isinstance(topics, list) else (topics or "")
        brands_json = json.dumps(brands)
        sponsors_json = json.dumps(sponsors)
        products_json = json.dumps(products)

        # 2) Upsert video row
        c.execute(
            """
            INSERT OR REPLACE INTO videos (
                video_id, channel_id, title, channel_name, upload_date, duration,
                overall_summary, overall_sentiment, topics, brands, sponsors, products,
                view_count, like_count, thumbnail_url, author, is_family_safe,
                owner_profile_url, category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                video_meta["id"],
                video_meta["channel_id"],
                video_meta.get("title", ""),
                video_meta.get("channel", ""),
                video_meta.get("date", ""),
                video_meta.get("duration", 0),
                analysis.get("summary", ""),
                analysis.get("sentiment", "Neutral"),
                topics_str,
                brands_json,
                sponsors_json,
                products_json,
                int(video_meta.get("viewCount", 0) or 0),
                int(video_meta.get("likeCount", 0) or 0),
                video_meta.get("thumbnail", ""),
                video_meta.get("author", ""),
                1 if video_meta.get("isFamilySafe", True) else 0,
                video_meta.get("ownerProfileUrl", ""),
                video_meta.get("category", ""),
            ),
        )

        # 3) Segments table
        c.execute("DELETE FROM video_segments WHERE video_id = ?", (video_meta["id"],))
        for seg in transcript_segments or []:
            c.execute(
                """
                INSERT INTO video_segments (video_id, start_time, end_time, text)
                VALUES (?, ?, ?, ?)
                """,
                (
                    video_meta["id"],
                    float(seg.get("start", 0.0)),
                    float(seg.get("end", 0.0)),
                    seg.get("text", ""),
                ),
            )

        # 4) Mentions tables
        score = sentiment_to_score(analysis.get("sentiment", "Neutral"))
        channel_id = video_meta["channel_id"]
        upload_date = video_meta.get("date", "") or datetime.utcnow().isoformat()

        c.execute("DELETE FROM brand_mentions WHERE video_id = ?", (video_meta["id"],))
        c.execute("DELETE FROM sponsor_mentions WHERE video_id = ?", (video_meta["id"],))
        c.execute("DELETE FROM product_mentions WHERE video_id = ?", (video_meta["id"],))

        main_brand_name = brands[0] if brands else None

        for b in brands:
            brand_id = upsert_brand(conn, b, category=video_meta.get("category"))
            if brand_id:
                c.execute(
                    """
                    INSERT INTO brand_mentions (
                        brand_id, video_id, channel_id, mention_count,
                        sentiment_score, first_seen_date
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (brand_id, video_meta["id"], channel_id, 1, score, upload_date),
                )

        for s in sponsors:
            sponsor_id = upsert_sponsor(conn, s, category="sponsor")
            if sponsor_id:
                c.execute(
                    """
                    INSERT INTO sponsor_mentions (
                        sponsor_id, video_id, channel_id, mention_count,
                        sentiment_score, first_seen_date
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (sponsor_id, video_meta["id"], channel_id, 1, score, upload_date),
                )

        # --- 5) PRODUCTS & CACHE INVALIDATION ---
        for p_dict in products:
            if not isinstance(p_dict, dict): continue

            product_name = p_dict.get("product")
            if not product_name: continue

            brand_for_product = p_dict.get("brand") or main_brand_name
            product_id = upsert_product(conn, product_name, brand_name=brand_for_product)

            if product_id:
                c.execute(
                    """
                    INSERT INTO product_mentions (
                        product_id, brand_id, video_id, channel_id, mention_count,
                        sentiment_score, first_seen_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (product_id, brand_id, video_meta["id"], channel_id, 1, score, upload_date),
                )

                # --- CACHE INVALIDATION ---
                cache_key = f"product:{product_id}:intel_v2"
                c.execute("DELETE FROM cached_dashboards WHERE key=?", (cache_key,))
                print(f"[CACHE] Invalidated brief for product {product_id} ({product_name})")

        conn.commit()
        print(f"[OK] Saved video {video_meta['id']} with {len(brands)} brands.")
    finally:
        conn.close()


def ingest_single_video(video_id: str) -> None:
    if video_already_exists(video_id):
        print(f"[SKIP] Video {video_id} already ingested – skipping.")
        return

    youtube = get_authenticated_service()
    video_meta = get_video_metadata(youtube, video_id)
    if not video_meta:
        print(f"[{video_id}] No metadata found.")
        return

    segments = get_transcript_segments(video_id)
    if not segments:
        print(f"[{video_id}] No transcript segments available – skipping.")
        return

    save_video_to_db(video_meta, segments)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python ingest_video.py <VIDEO_ID>")
        raise SystemExit(1)

    vid = sys.argv[1]
    ingest_single_video(vid)
