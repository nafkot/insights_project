# db_init.py
import sqlite3
from config import DB_PATH

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON")

    # Channels
    # Added: platform (defaults to YouTube), avatar_url
    c.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            subscriber_count INTEGER,
            video_count INTEGER,
            view_count INTEGER,
            creation_date TEXT,
            category TEXT,
            thumbnail_url TEXT,
            platform TEXT DEFAULT 'YouTube',
            avatar_url TEXT
        )
    """)

    # Videos
    c.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            title TEXT,
            channel_name TEXT,
            upload_date TEXT,
            duration INTEGER,
            overall_summary TEXT,
            overall_sentiment TEXT,
            topics TEXT,
            brands TEXT,
            sponsors TEXT,
            products TEXT,
            view_count INTEGER,
            like_count INTEGER,
            thumbnail_url TEXT,
            author TEXT,
            is_family_safe INTEGER,
            owner_profile_url TEXT,
            category TEXT,
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        )
    """)

    # Video segments
    c.execute("""
        CREATE TABLE IF NOT EXISTS video_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            start_time REAL,
            end_time REAL,
            text TEXT,
            FOREIGN KEY(video_id) REFERENCES videos(video_id)
        )
    """)

    # Brands
    c.execute("""
        CREATE TABLE IF NOT EXISTS brands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            normalized_name TEXT,
            category TEXT,
            meta TEXT
        )
    """)

    # Sponsors
    c.execute("""
        CREATE TABLE IF NOT EXISTS sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            normalized_name TEXT,
            category TEXT,
            meta TEXT
        )
    """)

    # Brand mentions
    c.execute("""
        CREATE TABLE IF NOT EXISTS brand_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_id INTEGER,
            video_id TEXT,
            channel_id TEXT,
            mention_count INTEGER,
            sentiment_score INTEGER,
            first_seen_date TEXT,
            FOREIGN KEY(brand_id) REFERENCES brands(id),
            FOREIGN KEY(video_id) REFERENCES videos(video_id),
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        )
    """)

    # Sponsor mentions
    c.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sponsor_id INTEGER,
            video_id TEXT,
            channel_id TEXT,
            mention_count INTEGER,
            sentiment_score INTEGER,
            first_seen_date TEXT,
            FOREIGN KEY(sponsor_id) REFERENCES sponsors(id),
            FOREIGN KEY(video_id) REFERENCES videos(video_id),
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        )
    """)

    # Products
    # Added: brand_id foreign key
    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            normalized_name TEXT,
            brand_name TEXT,
            meta TEXT,
            brand_id INTEGER,
            FOREIGN KEY(brand_id) REFERENCES brands(id)
        )
    """)

    # Product mentions
    c.execute("""
        CREATE TABLE IF NOT EXISTS product_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            video_id TEXT,
            channel_id TEXT,
            mention_count INTEGER,
            sentiment_score INTEGER,
            first_seen_date TEXT,
            brand_id INTEGER,
            FOREIGN KEY(brand_id) REFERENCES brands(id),
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(video_id) REFERENCES videos(video_id),
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
        )
    """)

    # Search queries (for autocomplete & trending searches)
    c.execute("""
        CREATE TABLE IF NOT EXISTS search_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT,
            type TEXT,   -- 'brand','sponsor','channel','product','topic','free'
            count INTEGER DEFAULT 1,
            last_used TEXT
        )
    """)

    # Cached dashboards (for future cron-based precompute)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cached_dashboards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE,    -- 'brand:Maybelline', 'product:Fit Me Foundation'
            type TEXT,          -- 'brand','sponsor','channel','product'
            payload TEXT,
            updated_at TEXT
        )
    """)

    # Extraction cache (avoids re-running LLM on same transcript)
    c.execute("""
        CREATE TABLE IF NOT EXISTS video_extraction_cache (
            video_id TEXT PRIMARY KEY,
            transcript_hash TEXT,
            brands_json TEXT,
            products_json TEXT,
            sponsors_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("Database initialised/updated.")


if __name__ == "__main__":
    init_db()
