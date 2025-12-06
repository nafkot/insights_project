import os
import sys
import json
import sqlite3
from collections import Counter
from flask import Flask, render_template, request, g, jsonify, url_for, redirect
from dotenv import load_dotenv
from openai import OpenAI
import markdown

# ------------------------------------------------------------------------------
# CRITICAL FIX: Add the parent directory to sys.path to allow importing 'utils'
# ------------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

# --- Local Imports (Must come AFTER the sys.path fix above) ---
from utils.search_engine import answer_user_query
from utils.autocomplete import hybrid_autocomplete, llm_semantic_suggestions

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")

# Ensure DB path is correct relative to where the script is run
DB_PATH = os.getenv("YOUTUBE_DB", "youtube_insights.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


# -------------------------------
# Database Connection
# -------------------------------
def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()

# -------------------------------
# Template Filters
# -------------------------------
@app.template_filter("from_json")
def from_json_filter(value):
    try:
        return json.loads(value)
    except:
        return []

# -------------------------------
# Context Processor (Navbar Categories)
# -------------------------------
@app.context_processor
def inject_categories():
    """
    Injects the full list of YouTube categories into the navbar.
    Combines DB data with a hardcoded fallback list.
    """
    try:
        # 1. Start with the official YouTube category list
        full_categories = [
            "Autos & Vehicles",
            "Comedy",
            "Education",
            "Entertainment",
            "Film & Animation",
            "Gaming",
            "Howto & Style",
            "Music",
            "News & Politics",
            "Nonprofits & Activism",
            "People & Blogs",
            "Pets & Animals",
            "Science & Technology",
            "Sports",
            "Travel & Events"
        ]

        # 2. Optionally, check DB for any custom/new ones
        conn = get_db()
        rows = conn.execute("SELECT DISTINCT category FROM channels WHERE category IS NOT NULL AND category != ''").fetchall()
        db_cats = [r['category'] for r in rows]

        # Merge and sort unique categories
        final_cats = sorted(list(set(full_categories + db_cats)))

        return dict(navbar_categories=final_cats)

    except Exception as e:
        print(f"Error loading categories: {e}")
        # Fallback if DB fails
        return dict(navbar_categories=[
            "Autos & Vehicles", "Comedy", "Education", "Entertainment", "Film & Animation",
            "Gaming", "Howto & Style", "Music", "News & Politics", "Nonprofits & Activism",
            "People & Blogs", "Pets & Animals", "Science & Technology", "Sports", "Travel & Events"
        ])


# -------------------------------
# Routes
# -------------------------------

@app.route("/", methods=["GET"])
def home():
    """
    Simple Google-style landing page (Search Only).
    """
    return render_template("home.html")


@app.route("/brands", methods=["GET"])
def brands_hub():
    """
    The main Dashboard for Brands (formerly the home page).
    Accessed via 'Categories -> Howto & Style -> Brands'.
    """
    conn = get_db()

    # 1. Trending (Highest Mention Count)
    trending_sql = """
        SELECT name, 'brand' as type, cnt, id
        FROM (
            SELECT b.name, b.id, count(bm.id) as cnt
            FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
            GROUP BY b.id
        )
        UNION ALL
        SELECT name, 'product' as type, cnt, id
        FROM (
            SELECT p.name, p.id, count(pm.id) as cnt
            FROM products p JOIN product_mentions pm ON p.id = pm.product_id
            GROUP BY p.id
        )
        ORDER BY cnt DESC
        LIMIT 10
    """
    trending = conn.execute(trending_sql).fetchall()

    # 2. Popular (Highest Positive Sentiment, min 3 mentions)
    popular_sql = """
        SELECT name, 'brand' as type, score, id
        FROM (
            SELECT b.name, b.id, AVG(bm.sentiment_score) as score, count(bm.id) as c
            FROM brands b JOIN brand_mentions bm ON b.id = bm.brand_id
            GROUP BY b.id HAVING c > 2
        )
        UNION ALL
        SELECT name, 'product' as type, score, id
        FROM (
            SELECT p.name, p.id, AVG(pm.sentiment_score) as score, count(pm.id) as c
            FROM products p JOIN product_mentions pm ON p.id = pm.product_id
            GROUP BY p.id HAVING c > 2
        )
        ORDER BY score DESC
        LIMIT 10
    """
    popular = conn.execute(popular_sql).fetchall()

    # 3. Top Channels (Subscriber Count)
    channels_sql = """
        SELECT channel_id, title, subscriber_count
        FROM channels
        ORDER BY subscriber_count DESC
        LIMIT 10
    """
    channels = conn.execute(channels_sql).fetchall()

    return render_template(
        "brands_landing.html",
        trending=trending,
        popular=popular,
        channels=channels
    )


@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()

    # If empty query, redirect to home
    if not query:
        return redirect(url_for("home"))

    conn = get_db()
    videos = []

    # Simple title match search
    # (Enhanced semantic search happens in the 'ai_answer' logic below)
    rows = conn.execute(
        "SELECT video_id, title, channel_name, thumbnail_url FROM videos WHERE title LIKE ? ORDER BY upload_date DESC LIMIT 30",
        (f"%{query}%",)
    ).fetchall()

    for row in rows:
        videos.append({
            "video_id": row["video_id"],
            "title": row["title"],
            "channel_name": row["channel_name"],
            "thumbnail_url": row["thumbnail_url"]
        })

    # Generate AI Answer using utils
    ai_answer = answer_user_query(query)

    return render_template(
        "search.html",
        query=query,
        videos=videos,
        ai_answer=ai_answer
    )


@app.route("/channel/<channel_id>")
def channel_profile(channel_id):
    conn = get_db()
    channel = conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
    if not channel:
        return "Channel not found", 404

    videos = conn.execute("SELECT * FROM videos WHERE channel_id = ? ORDER BY upload_date DESC", (channel_id,)).fetchall()

    # Basic aggregations for the profile page
    brand_counts = Counter()
    product_counts = Counter()
    sponsor_counts = Counter()
    topics_list = []
    sentiments = []

    for v in videos:
        if v["topics"]: topics_list.extend(v["topics"].split(","))

        # Load JSON fields safely
        try:
            if v["brands"]: brand_counts.update(json.loads(v["brands"]))
            if v["products"]: product_counts.update([p.get("product") for p in json.loads(v["products"])])
            if v["sponsors"]: sponsor_counts.update(json.loads(v["sponsors"]))
        except:
            pass

        # Sentiment calc
        s = (v["overall_sentiment"] or "").lower()
        if "positive" in s: sentiments.append(100)
        elif "negative" in s: sentiments.append(0)
        else: sentiments.append(50)

    stats = {
        "top_topics": [t[0] for t in Counter(topics_list).most_common(10)],
        "top_brands": [b[0] for b in brand_counts.most_common(10)],
        "top_products": [p[0] for p in product_counts.most_common(10)],
        "top_sponsors": [s[0] for s in sponsor_counts.most_common(10)],
        "brand_count": len(brand_counts),
        "product_count": len(product_counts),
        "sponsor_count": len(sponsor_counts),
        "video_count": len(videos),
        "sentiment_avg": sum(sentiments)/len(sentiments) if sentiments else 50
    }

    return render_template("channel_profile.html", channel=channel, videos=videos, stats=stats)


@app.route("/brand/<brand_id>")
def brand_profile(brand_id):
    conn = get_db()
    # Support lookup by ID (integer) or potentially name (string) if you expand logic
    brand = conn.execute("SELECT * FROM brands WHERE id = ?", (brand_id,)).fetchone()
    if not brand:
        return "Brand not found", 404

    mentions = conn.execute("""
        SELECT v.title, v.channel_name, v.video_id, bm.first_seen_date, bm.sentiment_score
        FROM brand_mentions bm
        JOIN videos v ON bm.video_id = v.video_id
        WHERE bm.brand_id = ?
        ORDER BY bm.first_seen_date DESC
    """, (brand_id,)).fetchall()

    return render_template("brand_profile.html", brand=brand, mentions=mentions)


@app.route("/product/<int:product_id>")
def product_profile(product_id):
    conn = get_db()
    product = conn.execute("""
        SELECT p.id, p.name, b.id AS brand_id, b.name AS brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.id = ?
    """, (product_id,)).fetchone()

    if not product:
        return "Product not found", 404

    metrics = conn.execute("""
        SELECT COUNT(*) as total_mentions, COUNT(DISTINCT channel_id) as unique_channels, AVG(sentiment_score) as avg_sentiment
        FROM product_mentions
        WHERE product_id = ?
    """, (product_id,)).fetchone()

    videos = conn.execute("""
        SELECT v.video_id, v.title, v.channel_name
        FROM product_mentions pm
        JOIN videos v ON v.video_id = pm.video_id
        WHERE pm.product_id = ?
        ORDER BY pm.mention_count DESC LIMIT 20
    """, (product_id,)).fetchall()

    return render_template("product_profile.html", product=product, metrics=metrics, videos=videos)


@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q", "").strip().lower()
    conn = get_db()

    results = {
        "channels": [],
        "brands": [],
        "products": [],
        "sponsors": [],
        "semantic": []
    }

    if not query:
        return jsonify(results)

    # Helper for LIKE queries
    def get_matches(table, col_id, col_name, limit=5):
        q_str = f"%{query}%"
        return conn.execute(f"SELECT {col_id}, {col_name} FROM {table} WHERE lower({col_name}) LIKE ? LIMIT ?", (q_str, limit)).fetchall()

    # Channels
    for r in get_matches("channels", "channel_id", "title"):
        results["channels"].append({"id": r[0], "name": r[1]})

    # Brands
    for r in get_matches("brands", "id", "name"):
        results["brands"].append({"id": r[0], "name": r[1]})

    # Products
    for r in get_matches("products", "id", "name"):
        results["products"].append({"id": r[0], "name": r[1]})

    # Sponsors
    for r in get_matches("sponsors", "id", "name"):
        results["sponsors"].append({"id": r[0], "name": r[1]})

    # Hybrid Fallback: If minimal DB results, use LLM
    total_hits = sum(len(v) for v in results.values())
    if total_hits < 3:
        semantic_suggestions = llm_semantic_suggestions(query)
        # Tag these so frontend knows they are AI guesses, not DB records
        results["semantic"] = [{"id": None, "name": s} for s in semantic_suggestions]

    return jsonify(results)


# -------------------------------
# Run Server
# -------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
